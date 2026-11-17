//! `herdr-bus request`: asks a request and waits, in this process on the bus's machine, for
//! its reply or its deadline.

use std::path::Path;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::{Duration, Instant};

use herdr_bus::{Bus, Message, Outcome, Seq};

use crate::watch::{BACKSTOP, SETTLE, instant_of, watch};

/// Asks `message` on `output` as a request with `timeout`, and returns its seq and outcome:
/// its reply, or its timeout. Never [`Outcome::Pending`].
pub(crate) fn ask(
    bus: &Bus,
    path: &Path,
    output: &str,
    message: &Message,
    timeout: Duration,
) -> Result<(Seq, Outcome), Box<dyn std::error::Error>> {
    let (written, wakes) = mpsc::channel();
    // Watching before the request goes, so that a reply written at once is not missed.
    // Without a watcher, it looks once a second all the same.
    let to_watcher = written.clone();
    let _watcher = watch(path, move || {
        let _ = to_watcher.send(());
    })
    .ok();
    let seq = bus.request(output, message, timeout)?;
    let mut look = Instant::now();
    loop {
        let now = Instant::now();
        if now >= look {
            match bus.outcome(seq)? {
                Outcome::Pending { deadline } => look = (now + BACKSTOP).min(instant_of(deadline)),
                outcome => return Ok((seq, outcome)),
            }
        }
        match wakes.recv_timeout(look.saturating_duration_since(Instant::now())) {
            Ok(()) => look = look.min(Instant::now() + SETTLE),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => unreachable!("`written` is held here"),
        }
    }
}
