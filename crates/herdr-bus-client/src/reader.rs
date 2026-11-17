//! The reader of a session: one thread reads every line `serve` sends, and hands it to the
//! caller that waits for it, so that several threads share one session. Answers go to the
//! callers in the order their lines were sent, the reply or timeout of a request to its asker,
//! and a notify to the pull that waits for one.

use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader};
use std::process::ChildStdout;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Condvar, Mutex, MutexGuard};

use herdr_bus::Seq;

use crate::protocol::Line;
use crate::{Error, Result};

/// Where a line, or the error of the session, goes: one value is sent on it.
pub(crate) type Slot = Sender<Result<Line>>;

/// What the reader and the callers share.
#[derive(Default)]
pub(crate) struct Waiters {
    state: Mutex<State>,
    /// Signalled on a notify, and when the session ends.
    changed: Condvar,
}

#[derive(Default)]
struct State {
    /// The callers waiting for an answer, in the order their lines were sent.
    answers: VecDeque<Waiter>,
    /// The askers waiting for the reply or timeout of a request, by its seq: one line goes to
    /// each, first come first served.
    outcomes: HashMap<Seq, VecDeque<Slot>>,
    /// Whether `serve` notified since the last answer to a `get_messages`, which covers every
    /// notify before it.
    notified: bool,
    /// Why the session ended, once it has.
    ended: Option<String>,
}

/// A caller waiting for an answer.
struct Waiter {
    answer: Slot,
    /// For a request: where its reply or timeout goes, once its answer says its seq.
    outcome: Option<Slot>,
}

impl Waiters {
    /// Expects the next answer, for a line about to be sent: call it in the order lines are
    /// sent. `outcome`, for a request, is where its reply or timeout goes. Fails once the
    /// session ended.
    pub(crate) fn expect(&self, outcome: Option<Slot>) -> Result<Receiver<Result<Line>>> {
        let mut state = self.lock();
        if let Some(why) = &state.ended {
            return Err(Error::Session(why.clone()));
        }
        let (answer, answered) = channel();
        state.answers.push_back(Waiter { answer, outcome });
        Ok(answered)
    }

    /// Waits until `serve` notifies, after the last answer to a `get_messages`, or the session
    /// ends.
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

    /// Ends the session, unless it ended already: every caller waiting, and every later one,
    /// gets `why`.
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
                // An asker that gave up, its link silent, gets nothing.
                let slot = state.outcomes.get_mut(&to).and_then(VecDeque::pop_front);
                if state.outcomes.get(&to).is_some_and(VecDeque::is_empty) {
                    state.outcomes.remove(&to);
                }
                if let Some(slot) = slot {
                    let _ = slot.send(Ok(line));
                }
            }
            // Only the first line: the client reads it before the reader starts.
            Line::Ready {} => {}
            answer => {
                // An answer nobody waits for came out of turn: nobody to give it to.
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
        // A caller that panicked holding the lock left the state whole: each change is one step.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Reads `serve`'s lines and hands each to its waiter until the session ends, then ends it with
/// what `ended` says of it.
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
                    // No caller can be told which answer it was: the session cannot go on.
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
