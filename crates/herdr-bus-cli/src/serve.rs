//! `herdr-bus serve`: a session with one consumer, in JSON lines over stdin and stdout, as
//! `herdr_bus_client::protocol` describes them. It tells the consumer when it has messages
//! past those the session delivered, and sends messages only when asked. It publishes, asks,
//! takes and replies to requests for the consumer, and sends the reply or timeout of each
//! request it asked once there is one.
//!
//! It learns of writes by watching the bus file's folder ([`crate::watch`]), and wakes at the
//! nearest deadline of the requests it asked.

use std::collections::HashMap;
use std::error::Error;
use std::io::{self, BufRead, Write};
use std::path::Path;
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::time::{Duration, Instant};

use herdr_bus::{Bus, ConnectionId, Error as BusError, Outcome, Seq, Timestamp, Uuid};
use herdr_bus_client::protocol::{Call, Line};

use crate::watch::{BACKSTOP, SETTLE, instant_of, watch};

/// What wakes the session.
enum Event {
    /// A line from the consumer.
    Line(String),
    /// A line from the consumer that is not UTF-8.
    Unreadable,
    /// The `-wal` file was modified: something was written.
    Written,
    /// The consumer closed stdin: the session is over.
    Closed,
}

/// Holds the session until the consumer closes stdin, or stops reading stdout.
pub(crate) fn serve(bus: Bus, path: &Path, consumer: &str) -> Result<(), Box<dyn Error>> {
    // Refuses a name that is no consumer's before the session starts.
    bus.positions(consumer)?;
    let (events, received) = mpsc::channel();
    // Without a watcher, as where the system has none left to give, the session looks once a
    // second all the same.
    let written = events.clone();
    let _watcher = watch(path, move || {
        let _ = written.send(Event::Written);
    })
    .inspect_err(|error| {
        eprintln!("herdr-bus serve: cannot watch the bus file, looks once a second: {error}");
    })
    .ok();
    std::thread::spawn(move || read_lines(&events));
    let session = Session {
        bus,
        consumer: consumer.to_owned(),
        delivered: HashMap::new(),
        notified: false,
        asked: Vec::new(),
        nearest: None,
    };
    match session.run(&received, &mut io::stdout().lock()) {
        // The consumer stopped reading: it has ended the session.
        Err(error) if is_broken_pipe(&*error) => Ok(()),
        outcome => outcome,
    }
}

struct Session {
    bus: Bus,
    consumer: String,
    /// The highest seq the session delivered on each connection: an ack past it is refused.
    delivered: HashMap<ConnectionId, Seq>,
    /// Whether the consumer was notified since it last asked for its messages.
    notified: bool,
    /// Each request the session asked that has no outcome yet, its seq and id: one entry for
    /// each ask, which gets one reply or timeout.
    asked: Vec<(Seq, Uuid)>,
    /// The nearest deadline of the requests in `asked`.
    nearest: Option<Timestamp>,
}

impl Session {
    fn run(
        mut self,
        events: &mpsc::Receiver<Event>,
        out: &mut impl Write,
    ) -> Result<(), Box<dyn Error>> {
        send(out, &Line::Ready {})?;
        self.look(out)?;
        let mut due = Instant::now() + BACKSTOP;
        loop {
            let wake = self
                .nearest
                .map_or(due, |nearest| due.min(instant_of(nearest)));
            match events.recv_timeout(wake.saturating_duration_since(Instant::now())) {
                Ok(Event::Line(line)) => {
                    if !line.trim().is_empty() {
                        self.answer(&line, out)?;
                        self.look(out)?;
                    }
                }
                Ok(Event::Unreadable) => {
                    send(out, &Line::Error("a line is UTF-8".to_owned()))?;
                }
                Ok(Event::Written) => due = due.min(Instant::now() + SETTLE),
                Ok(Event::Closed) | Err(RecvTimeoutError::Disconnected) => return Ok(()),
                Err(RecvTimeoutError::Timeout) => {
                    self.look(out)?;
                    // A deadline may have woken it before a write had settled.
                    if Instant::now() >= due {
                        due = Instant::now() + BACKSTOP;
                    }
                }
            }
        }
    }

    /// Notifies the consumer, once, when it has messages past those the session delivered,
    /// and sends the outcome of each request it asked that has one.
    fn look(&mut self, out: &mut impl Write) -> Result<(), Box<dyn Error>> {
        if !self.notified && self.has_new()? {
            send(out, &Line::Notify {})?;
            self.notified = true;
        }
        self.settle(out)
    }

    fn has_new(&self) -> herdr_bus::Result<bool> {
        Ok(self.bus.positions(&self.consumer)?.iter().any(|position| {
            let seen = self
                .delivered
                .get(&position.connection)
                .map_or(position.cursor, |delivered| {
                    (*delivered).max(position.cursor)
                });
            position.newest.is_some_and(|newest| newest > seen)
        }))
    }

    /// Sends the reply or timeout of each request the session asked that has one, and keeps
    /// the nearest deadline of the others.
    fn settle(&mut self, out: &mut impl Write) -> Result<(), Box<dyn Error>> {
        self.nearest = None;
        for (seq, id) in std::mem::take(&mut self.asked) {
            match self.bus.outcome(seq)? {
                Outcome::Pending { deadline } => {
                    self.nearest = Some(self.nearest.map_or(deadline, |n| n.min(deadline)));
                    self.asked.push((seq, id));
                }
                Outcome::Replied(reply) => send(
                    out,
                    &Line::Reply {
                        to: seq,
                        id,
                        data: reply.data,
                    },
                )?,
                Outcome::TimedOut { taken } => {
                    send(out, &Line::Timeout { to: seq, id, taken })?;
                }
            }
        }
        Ok(())
    }

    fn answer(&mut self, line: &str, out: &mut impl Write) -> io::Result<()> {
        let answer = match serde_json::from_str::<Call>(line) {
            Ok(call) => self.carry_out(call).unwrap_or_else(Line::Error),
            Err(error) => Line::Error(format!("not a line of the session: {error}")),
        };
        send(out, &answer)
    }

    fn carry_out(&mut self, call: Call) -> Result<Line, String> {
        let bus = &self.bus;
        match call {
            Call::GetMessages { limit } => self.get_messages(limit),
            Call::Ack { connection, seq } => self.ack(connection, seq),
            Call::Publish { output, message } => {
                let seq = bus
                    .publish(&output, &message)
                    .map_err(|error| error.to_string())?;
                Ok(Line::Published {
                    seq,
                    id: message.id,
                })
            }
            Call::Request {
                output,
                message,
                timeout_ms,
            } => {
                let timeout = Duration::from_millis(timeout_ms);
                let seq = bus
                    .request(&output, &message, timeout)
                    .map_err(|error| error.to_string())?;
                self.asked.push((seq, message.id));
                Ok(Line::Requested {
                    seq,
                    id: message.id,
                })
            }
            Call::Take { seq } => Ok(Line::Taken {
                seq,
                state: bus.take(seq).map_err(|error| error.to_string())?,
            }),
            Call::Reply { to, data } => {
                bus.reply(to, data).map_err(|error| error.to_string())?;
                Ok(Line::Replied { to })
            }
        }
    }

    fn get_messages(&mut self, limit: usize) -> Result<Line, String> {
        // An empty answer would read as "nothing left", and bring the next notify at once.
        if limit == 0 {
            return Err("limit must be at least 1".to_owned());
        }
        // Asked for, whatever comes of it: the consumer may be notified again.
        self.notified = false;
        let deliveries = self
            .bus
            .pull(&self.consumer, limit)
            .map_err(|error| error.to_string())?;
        for delivery in &deliveries {
            let delivered = self
                .delivered
                .entry(delivery.connection)
                .or_insert(delivery.seq);
            *delivered = (*delivered).max(delivery.seq);
        }
        Ok(Line::Messages(deliveries))
    }

    fn ack(&mut self, connection: ConnectionId, seq: Seq) -> Result<Line, String> {
        if self
            .delivered
            .get(&connection)
            .is_none_or(|delivered| seq > *delivered)
        {
            return Err(format!(
                "message {seq} was not delivered on connection {connection} in this session"
            ));
        }
        match self.bus.ack(connection, seq) {
            // A connection removed since has nothing left to acknowledge.
            Ok(()) | Err(BusError::UnknownConnection(_)) => Ok(Line::Acked { connection, seq }),
            Err(error) => Err(error.to_string()),
        }
    }
}

fn send(out: &mut impl Write, line: &Line) -> io::Result<()> {
    let line = serde_json::to_string(line).expect("lines serialize");
    writeln!(out, "{line}")?;
    out.flush()
}

fn read_lines(events: &Sender<Event>) {
    let mut stdin = io::stdin().lock();
    let mut line = Vec::new();
    loop {
        line.clear();
        if !matches!(stdin.read_until(b'\n', &mut line), Ok(read) if read > 0) {
            break;
        }
        let event = match String::from_utf8(std::mem::take(&mut line)) {
            Ok(line) => Event::Line(line),
            Err(_) => Event::Unreadable,
        };
        if events.send(event).is_err() {
            return;
        }
    }
    let _ = events.send(Event::Closed);
}

fn is_broken_pipe(error: &(dyn Error + 'static)) -> bool {
    error
        .downcast_ref::<io::Error>()
        .is_some_and(|error| error.kind() == io::ErrorKind::BrokenPipe)
}
