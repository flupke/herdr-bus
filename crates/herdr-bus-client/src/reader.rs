use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader};
use std::process::ChildStdout;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Condvar, Mutex, MutexGuard};

use herdr_bus::Seq;

use crate::protocol::Line;
use crate::{Error, Result};

pub(crate) type Slot = Sender<Result<Line>>;

#[derive(Default)]
pub(crate) struct Waiters {
    state: Mutex<State>,
    changed: Condvar,
}

#[derive(Default)]
struct State {
    answers: VecDeque<Waiter>,
    outcomes: HashMap<Seq, VecDeque<Slot>>,
    notified: bool,
    ended: Option<String>,
}

struct Waiter {
    answer: Slot,
    outcome: Option<Slot>,
}

impl Waiters {
    pub(crate) fn expect(&self, outcome: Option<Slot>) -> Result<Receiver<Result<Line>>> {
        let mut state = self.lock();
        if let Some(why) = &state.ended {
            return Err(Error::Session(why.clone()));
        }
        let (answer, answered) = channel();
        state.answers.push_back(Waiter { answer, outcome });
        Ok(answered)
    }

    pub(crate) fn wait_for_notify(&self) -> Result<()> {
        let mut state = self.lock();
        loop {
            if let Some(why) = &state.ended {
                return Err(Error::Session(why.clone()));
            }
            if state.notified {
                return Ok(());
            }
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    pub(crate) fn end(&self, why: String) {
        let mut state = self.lock();
        if state.ended.is_some() {
            return;
        }
        let State {
            answers, outcomes, ..
        } = &mut *state;
        let waiting = answers
            .drain(..)
            .map(|waiter| waiter.answer)
            .chain(outcomes.drain().flat_map(|(_, slots)| slots));
        for slot in waiting {
            let _ = slot.send(Err(Error::Session(why.clone())));
        }
        state.ended = Some(why);
        self.changed.notify_all();
    }

    fn dispatch(&self, line: Line) {
        let mut state = self.lock();
        match line {
            Line::Notify {} => {
                state.notified = true;
                self.changed.notify_all();
            }
            Line::Reply { to, .. } | Line::Timeout { to, .. } => {
                let slot = state.outcomes.get_mut(&to).and_then(VecDeque::pop_front);
                if state.outcomes.get(&to).is_some_and(VecDeque::is_empty) {
                    state.outcomes.remove(&to);
                }
                if let Some(slot) = slot {
                    let _ = slot.send(Ok(line));
                }
            }
            Line::Ready {} => {}
            answer => {
                let Some(waiter) = state.answers.pop_front() else {
                    return;
                };
                match &answer {
                    Line::Messages(_) => state.notified = false,
                    Line::Requested { seq, .. } => {
                        if let Some(outcome) = waiter.outcome {
                            state.outcomes.entry(*seq).or_default().push_back(outcome);
                        }
                    }
                    _ => {}
                }
                let _ = waiter.answer.send(Ok(answer));
            }
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

pub(crate) fn read(
    mut from_serve: BufReader<ChildStdout>,
    waiters: &Waiters,
    ended: impl FnOnce() -> String,
) {
    let mut line = String::new();
    loop {
        line.clear();
        match from_serve.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => match serde_json::from_str(&line) {
                Ok(parsed) => waiters.dispatch(parsed),
                Err(error) => {
                    waiters.end(format!("serve sent {:?}: {error}", line.trim_end()));
                    return;
                }
            },
            Err(error) => {
                waiters.end(format!("cannot read from serve: {error}"));
                return;
            }
        }
    }
    waiters.end(ended());
}

#[cfg(test)]
#[path = "reader.tests.rs"]
mod tests;
