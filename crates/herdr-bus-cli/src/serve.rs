//! `herdr-bus serve`: a session with one consumer, in JSON lines over stdin and stdout, as
//! `herdr_bus_client::protocol` describes them. It tells the consumer when it has messages
//! past those the session delivered, and sends messages only when asked.
//!
//! It learns of writes by watching the bus file's folder: every write modifies the `-wal` file,
//! and no read does. The event comes before the write can be read, so it looks a moment later,
//! and now and then without an event too (`.agents/wiki/shell-checks.md`, Watching the file).

use std::collections::HashMap;
use std::error::Error;
use std::ffi::OsString;
use std::io::{self, BufRead, Write};
use std::path::Path;
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::time::{Duration, Instant};

use herdr_bus::{Bus, ConnectionId, Error as BusError, Seq};
use herdr_bus_client::protocol::{Line, Request};
use notify::event::ModifyKind;
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};

/// How long after a write's event the session looks at the bus: 30 ms after it, a read saw
/// every write in the runs the wiki records.
const SETTLE: Duration = Duration::from_millis(30);

/// How long the session waits without an event before it looks anyway.
const BACKSTOP: Duration = Duration::from_secs(1);

/// What wakes the session.
enum Event {
    /// A line from the consumer.
    Request(String),
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
    // SQLite puts the `-wal` file beside the file a symbolic link names. Without a watcher, as
    // where the system has none left to give, the session looks once a second all the same.
    let _watcher = watch(&std::fs::canonicalize(path)?, events.clone())
        .inspect_err(|error| {
            eprintln!("herdr-bus serve: cannot watch the bus file, looks once a second: {error}");
        })
        .ok();
    std::thread::spawn(move || read_requests(&events));
    let session = Session {
        bus,
        consumer: consumer.to_owned(),
        delivered: HashMap::new(),
        notified: false,
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
            match events.recv_timeout(due.saturating_duration_since(Instant::now())) {
                Ok(Event::Request(line)) => {
                    if !line.trim().is_empty() {
                        self.answer(&line, out)?;
                        self.look(out)?;
                    }
                }
                Ok(Event::Unreadable) => {
                    send(out, &Line::Error("a request is a line of UTF-8".to_owned()))?;
                }
                Ok(Event::Written) => due = due.min(Instant::now() + SETTLE),
                Ok(Event::Closed) | Err(RecvTimeoutError::Disconnected) => return Ok(()),
                Err(RecvTimeoutError::Timeout) => {
                    self.look(out)?;
                    due = Instant::now() + BACKSTOP;
                }
            }
        }
    }

    /// Notifies the consumer, once, when it has messages past those the session delivered.
    fn look(&mut self, out: &mut impl Write) -> Result<(), Box<dyn Error>> {
        if !self.notified && self.has_new()? {
            send(out, &Line::Notify {})?;
            self.notified = true;
        }
        Ok(())
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

    fn answer(&mut self, line: &str, out: &mut impl Write) -> io::Result<()> {
        let answer = match serde_json::from_str::<Request>(line) {
            Ok(request) => self.carry_out(&request).unwrap_or_else(Line::Error),
            Err(error) => Line::Error(format!("not a request: {error}")),
        };
        send(out, &answer)
    }

    fn carry_out(&mut self, request: &Request) -> Result<Line, String> {
        match *request {
            Request::GetMessages { limit } => self.get_messages(limit),
            Request::Ack { connection, seq } => self.ack(connection, seq),
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

fn read_requests(events: &Sender<Event>) {
    let mut stdin = io::stdin().lock();
    let mut line = Vec::new();
    loop {
        line.clear();
        if !matches!(stdin.read_until(b'\n', &mut line), Ok(read) if read > 0) {
            break;
        }
        let event = match String::from_utf8(std::mem::take(&mut line)) {
            Ok(line) => Event::Request(line),
            Err(_) => Event::Unreadable,
        };
        if events.send(event).is_err() {
            return;
        }
    }
    let _ = events.send(Event::Closed);
}

/// Watches the folder of the bus file at `path` for modifications of its `-wal` file. The
/// folder, not the file, which goes whenever the last program closes the bus.
fn watch(path: &Path, events: Sender<Event>) -> notify::Result<RecommendedWatcher> {
    let folder = path
        .parent()
        .filter(|folder| !folder.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut wal = OsString::from(path.file_name().unwrap_or_default());
    wal.push("-wal");
    let mut watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
        let Ok(event) = event else { return };
        let modified = matches!(
            event.kind,
            EventKind::Modify(ModifyKind::Data(_) | ModifyKind::Any)
        );
        if modified
            && event
                .paths
                .iter()
                .any(|path| path.file_name() == Some(&wal))
        {
            let _ = events.send(Event::Written);
        }
    })?;
    watcher.watch(folder, RecursiveMode::NonRecursive)?;
    Ok(watcher)
}

fn is_broken_pipe(error: &(dyn Error + 'static)) -> bool {
    error
        .downcast_ref::<io::Error>()
        .is_some_and(|error| error.kind() == io::ErrorKind::BrokenPipe)
}
