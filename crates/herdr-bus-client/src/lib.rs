//! A consumer's session with a bus, on this machine or another: the client starts `herdr-bus
//! serve <consumer>` there, and pulls and acknowledges the consumer's messages through it,
//! publishes, and asks, takes and replies to requests. Where the bus has no message for it,
//! [`Client::pull`] waits for one; [`Client::request`] waits for the reply.
//!
//! ```no_run
//! use herdr_bus_client::{BusLocation, Client};
//!
//! let client = Client::connect(BusLocation::Local, "app")?;
//! loop {
//!     for delivery in client.pull(50)? {
//!         println!("{}", delivery.message.data);
//!         client.ack(delivery.connection, delivery.seq)?;
//!     }
//! }
//! # Ok::<(), herdr_bus_client::Error>(())
//! ```
//!
//! Every method takes `&self`, and a client is `Send` and `Sync`: one thread waits in
//! [`Client::pull`] while others publish, ask, take, reply and acknowledge, and several wait
//! in [`Client::request`] at once. [`Client::close`], from any thread, ends the session.
//!
//! Once the session ends, `serve` gone, the link to its machine dead or the client closed,
//! every waiting and later call returns an error, with what the command wrote on stderr: the
//! caller connects again. Cursors and requests stay in the bus file, so a new session loses
//! nothing: its first pull returns at once what waited, and a request asked again under its id
//! is the same request.
//!
//! The session is one JSON line each way over the command's stdin and stdout: the lines of
//! [`protocol`], which `serve` speaks too.

pub mod protocol;
mod reader;

use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::process::CommandExt as _;
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use herdr_bus::{Bus, ConnectionId, Delivery, Message, Seq, Take};
use rustix::process::{Pid, Signal, kill_process_group};
use serde::Deserialize;
use serde_json::Value;

use crate::protocol::{Call, Line};
use crate::reader::Waiters;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("cannot run {program}: {source}")]
    Spawn {
        program: String,
        source: std::io::Error,
    },
    /// The session did not start, `serve` refused a call, or the session ended.
    #[error("session: {0}")]
    Session(String),
    /// `serve` said nothing of a request [`GRACE`] past its deadline: the link went silent,
    /// and the request may have been taken.
    #[error(
        "session: herdr-bus serve {0} said nothing of the request past its deadline: the link \
         went silent, and the request may have been taken"
    )]
    Silent(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BusLocation {
    Local,
    /// Another machine's, by its name in `hc machine ls`, which the client reaches with `ssh`.
    Machine(String),
    /// Another machine's, through a command that reaches it and runs there the line given as
    /// its last argument, as the command of `hc machine add --via` does.
    Command(Vec<String>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Outcome {
    /// Its reply's data.
    Reply(Value),
    /// Its deadline passed with no reply. `taken: false` is final: nobody will ever handle
    /// it.
    Timeout { taken: bool },
}

/// A session with `herdr-bus serve`, which runs as a process of its own.
pub struct Client {
    to_serve: Mutex<Option<Sender<String>>>,
    waiters: Arc<Waiters>,
    child: Mutex<Option<Child>>,
    place: String,
    grace: Duration,
}

/// How long past a request's deadline [`Client::request`] waits for `serve` to say what became
/// of it, before it says the link went silent.
pub const GRACE: Duration = Duration::from_secs(2);

/// How long [`Client::connect`] waits for `serve`'s `ready`: longer than ssh's own 15 s to reach
/// a machine, plus a slow login shell there.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

const STDERR_TAIL: usize = 4096;

/// How long the client waits, once the command exited, for the rest of its stderr: a process it
/// left behind, such as the master of a shared ssh connection, may hold stderr open for good.
const STDERR_GRACE: Duration = Duration::from_secs(1);

impl Client {
    /// Starts `herdr-bus serve <consumer>` at `location`; fails unless it answers `ready` within
    /// [`CONNECT_TIMEOUT`], as [`Client::connect_within`] does.
    pub fn connect(location: BusLocation, consumer: &str) -> Result<Client> {
        Self::connect_within(location, consumer, CONNECT_TIMEOUT)
    }

    /// Starts `herdr-bus serve <consumer>` at `location`; fails unless it answers `ready` within
    /// `timeout` of the command's start. Past it, the command is killed with its process group,
    /// which holds every process it started that did not leave it, and the error says what it
    /// wrote on stderr; a process that left the group and holds stderr delays it by up to 1 s.
    ///
    /// The command runs in a process group of its own, so a signal to the app's group, such as
    /// Ctrl-C in a terminal, does not reach it: once the app is gone, `serve` ends when its stdin
    /// closes, and a link that went silent lasts until it times out. Nor can the command read
    /// the terminal, which would stop it: a `Command` must not prompt there.
    ///
    /// `Local` runs `herdr-bus` from `PATH`. `Machine` finds the machine's `target` in `hc
    /// machine ls --json`, and runs `herdr-bus` there through `ssh`, from a login shell's
    /// `PATH`; ssh never prompts, and ends a link that dies silently. A bare consumer name is
    /// completed with the id of this machine's bus first, so that the other machine's bus
    /// knows the consumer as this machine's. `Command` runs its program with its arguments,
    /// and the line ssh would run there as one last argument; the consumer is completed as for
    /// `Machine`. A command with no program is refused.
    ///
    /// A program that only publishes and asks gives a consumer name with no connection.
    pub fn connect_within(
        location: BusLocation,
        consumer: &str,
        timeout: Duration,
    ) -> Result<Client> {
        let (command, place) = match location {
            BusLocation::Local => {
                let mut command = Command::new("herdr-bus");
                command.args(["serve", "--", consumer]);
                (command, "here".to_owned())
            }
            BusLocation::Machine(machine) => {
                let target = target(&machine)?;
                let consumer = this_machines(consumer)?;
                (ssh(&target, &consumer), format!("on {machine}"))
            }
            BusLocation::Command(argv) => through(&argv, consumer)?,
        };
        Self::start(command, &place, timeout)
    }

    /// The consumer's next messages, at most `limit`, as `Bus::pull` returns them; where
    /// `Bus::pull` would return none, it waits until there are some. A `limit` of 0 counts as 1.
    pub fn pull(&self, limit: usize) -> Result<Vec<Delivery>> {
        let call = Call::GetMessages {
            limit: limit.max(1),
        };
        loop {
            match self.call(&call)? {
                Line::Messages(deliveries) if !deliveries.is_empty() => return Ok(deliveries),
                Line::Messages(_) => self.waiters.wait_for_notify()?,
                line => return Err(unexpected(&line)),
            }
        }
    }

    /// As `Bus::ack`: acknowledges the message at `seq` on `connection`, and every one before
    /// it there. A seq past those the session delivered there is refused.
    pub fn ack(&self, connection: ConnectionId, seq: Seq) -> Result<()> {
        match self.call(&Call::Ack { connection, seq })? {
            Line::Acked { .. } => Ok(()),
            line => Err(unexpected(&line)),
        }
    }

    pub fn publish(&self, output: &str, message: &Message) -> Result<Seq> {
        let call = Call::Publish {
            output: output.to_owned(),
            message: message.clone(),
        };
        match self.call(&call)? {
            Line::Published { seq, .. } => Ok(seq),
            line => Err(unexpected(&line)),
        }
    }

    /// As `Bus::request`: asks `message` on `output` as a request, with a deadline `timeout`
    /// from now by the bus's clock, and waits for its reply or its timeout. It returns by then,
    /// whatever the link does: when `serve` has said nothing [`GRACE`] past it, it returns
    /// [`Error::Silent`], and the request may have been taken. Asked again under the same id,
    /// in this session or another, it is the same request, with its first deadline: asked
    /// again with a timeout shorter than that deadline leaves, it may return [`Error::Silent`]
    /// before the bus says what became of it.
    pub fn request(&self, output: &str, message: &Message, timeout: Duration) -> Result<Outcome> {
        let limit = Instant::now().checked_add(timeout.saturating_add(self.grace));
        let call = Call::Request {
            output: output.to_owned(),
            message: message.clone(),
            timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
        };
        let (outcome, came) = mpsc::channel();
        let answered = self.send(&call, Some(outcome))?;
        match self.wait(&answered, limit)? {
            Line::Requested { .. } => {}
            line => return Err(unexpected(&line)),
        }
        match self.wait(&came, limit)? {
            Line::Reply { data, .. } => Ok(Outcome::Reply(data)),
            Line::Timeout { taken, .. } => Ok(Outcome::Timeout { taken }),
            line => Err(unexpected(&line)),
        }
    }

    pub fn take(&self, seq: Seq) -> Result<Take> {
        match self.call(&Call::Take { seq })? {
            Line::Taken { state, .. } => Ok(state),
            line => Err(unexpected(&line)),
        }
    }

    /// As `Bus::reply`: replies to the request at `seq`. A request that has a reply keeps the
    /// first one; one past its deadline that nobody took is refused.
    pub fn reply(&self, seq: Seq, data: Value) -> Result<()> {
        match self.call(&Call::Reply { to: seq, data })? {
            Line::Replied { .. } => Ok(()),
            line => Err(unexpected(&line)),
        }
    }

    /// Ends the session, from any thread: every waiting and later call returns its error.
    pub fn close(&self) {
        self.waiters.end(format!(
            "the session with herdr-bus serve {} was closed",
            self.place
        ));
        self.stop();
    }

    fn start(mut command: Command, place: &str, timeout: Duration) -> Result<Client> {
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // A group of its own, which `kill` ends whole: a process the command started, such
            // as the child of `sh -c` or ssh's proxy, holds the pipes too.
            .process_group(0)
            .spawn()
            .map_err(|source| Error::Spawn {
                program: command.get_program().to_string_lossy().into_owned(),
                source,
            })?;
        let (Some(to_serve), Some(from_serve), Some(stderr)) =
            (child.stdin.take(), child.stdout.take(), child.stderr.take())
        else {
            unreachable!("the three are piped");
        };
        let stderr = Stderr::read(stderr);
        let (started, start) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = started.send(ready(BufReader::new(from_serve)));
        });
        let from_serve = match start.recv_timeout(timeout) {
            Ok(Start::Ready(from_serve)) => from_serve,
            got => {
                drop(to_serve);
                kill(child);
                return Err(Error::Session(match got {
                    Ok(Start::First(line)) => {
                        format!("herdr-bus serve {place} sent {line:?} first")
                    }
                    Err(RecvTimeoutError::Timeout) => stderr.why(format!(
                        "herdr-bus serve {place} did not answer within {timeout:?}"
                    )),
                    _ => stderr.why(format!("herdr-bus serve did not start {place}")),
                }));
            }
        };
        let waiters = Arc::new(Waiters::default());
        std::thread::spawn({
            let waiters = Arc::clone(&waiters);
            let ended = format!("the session with herdr-bus serve {place} ended");
            move || reader::read(from_serve, &waiters, || stderr.why(ended))
        });
        let (lines, to_write) = mpsc::channel();
        std::thread::spawn(move || write_lines(to_serve, &to_write));
        Ok(Client {
            to_serve: Mutex::new(Some(lines)),
            waiters,
            child: Mutex::new(Some(child)),
            place: place.to_owned(),
            grace: GRACE,
        })
    }

    fn call(&self, call: &Call) -> Result<Line> {
        let answered = self.send(call, None)?;
        self.wait(&answered, None)
    }

    fn send(&self, call: &Call, outcome: Option<reader::Slot>) -> Result<Receiver<Result<Line>>> {
        let line = serde_json::to_string(call).expect("calls serialize");
        let to_serve = lock(&self.to_serve);
        let answered = self.waiters.expect(outcome)?;
        if let Some(lines) = to_serve.as_ref() {
            let _ = lines.send(line);
        }
        Ok(answered)
    }

    fn wait(&self, receiver: &Receiver<Result<Line>>, limit: Option<Instant>) -> Result<Line> {
        let got = match limit {
            Some(limit) => {
                match receiver.recv_timeout(limit.saturating_duration_since(Instant::now())) {
                    Err(RecvTimeoutError::Timeout) => {
                        return Err(Error::Silent(self.place.clone()));
                    }
                    got => got.map_err(|_| ()),
                }
            }
            None => receiver.recv().map_err(|_| ()),
        };
        match got {
            Ok(Ok(Line::Error(error))) => Err(Error::Session(error)),
            Ok(line) => line,
            Err(()) => unreachable!("a slot is answered before it goes"),
        }
    }

    fn stop(&self) {
        drop(lock(&self.to_serve).take());
        if let Some(child) = lock(&self.child).take() {
            kill(child);
        }
    }
}

enum Start {
    Ready(BufReader<ChildStdout>),
    First(Line),
    Ended,
}

fn ready(mut from_serve: BufReader<ChildStdout>) -> Start {
    let mut line = String::new();
    loop {
        line.clear();
        if !matches!(from_serve.read_line(&mut line), Ok(read) if read > 0) {
            return Start::Ended;
        }
        match serde_json::from_str(&line) {
            Ok(Line::Ready {}) => return Start::Ready(from_serve),
            Ok(line) => return Start::First(line),
            // What a login shell's profile prints there comes before `serve` runs.
            Err(_) => {}
        }
    }
}

/// Kills the process group `child` leads, which holds every process the command started that
/// did not leave it, and reaps `child`. `child` is not reaped before, so its pid still names the
/// group.
fn kill(mut child: Child) {
    // Fails when the group ended already.
    let _ = kill_process_group(Pid::from_child(&child), Signal::KILL);
    let _ = child.wait();
}

fn write_lines(mut to_serve: ChildStdin, lines: &Receiver<String>) {
    for line in lines {
        if writeln!(to_serve, "{line}")
            .and_then(|()| to_serve.flush())
            .is_err()
        {
            return;
        }
    }
}

impl Drop for Client {
    /// Ends the command: cursors and requests are in the bus file, so nothing is lost.
    fn drop(&mut self) {
        self.stop();
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// What the command writes on stderr, which a thread reads for the whole session, so that a
/// full pipe never blocks it: read once the session ended, to say why.
struct Stderr {
    tail: Arc<Mutex<Vec<u8>>>,
    read: Receiver<()>,
}

impl Stderr {
    fn read(stderr: ChildStderr) -> Self {
        let tail = Arc::new(Mutex::new(Vec::new()));
        let (done, read) = mpsc::channel();
        std::thread::spawn({
            let tail = Arc::clone(&tail);
            move || {
                keep_tail(stderr, &tail);
                drop(done);
            }
        });
        Self { tail, read }
    }

    fn why(&self, what: String) -> String {
        let _ = self.read.recv_timeout(STDERR_GRACE);
        let wrote = String::from_utf8_lossy(&lock(&self.tail)).into_owned();
        match wrote.trim() {
            "" => what,
            wrote => format!("{what}: {wrote}"),
        }
    }
}

fn keep_tail(mut stderr: ChildStderr, tail: &Mutex<Vec<u8>>) {
    let mut chunk = [0; 1024];
    while let Ok(read) = stderr.read(&mut chunk) {
        if read == 0 {
            break;
        }
        let mut kept = lock(tail);
        kept.extend_from_slice(&chunk[..read]);
        if kept.len() > STDERR_TAIL {
            let excess = kept.len() - STDERR_TAIL;
            kept.drain(..excess);
        }
    }
}

#[derive(Deserialize)]
struct Machine {
    name: String,
    /// What ssh reaches it at: `host`, `user@host`, `user@host:port`, or a Host of
    /// `~/.ssh/config`.
    #[serde(default)]
    target: Option<String>,
    /// Whether it is reached through a command instead of ssh.
    #[serde(default)]
    via: bool,
}

#[derive(Deserialize)]
struct Machines {
    machines: Vec<Machine>,
}

fn target(name: &str) -> Result<String> {
    let output = Command::new("hc")
        .args(["machine", "ls", "--json"])
        .stdin(Stdio::null())
        .output()
        .map_err(|source| Error::Spawn {
            program: "hc".to_owned(),
            source,
        })?;
    if !output.status.success() {
        return Err(Error::Session(format!(
            "hc machine ls failed: {}",
            String::from_utf8_lossy(&output.stderr).trim_end()
        )));
    }
    let listed: Machines = serde_json::from_slice(&output.stdout)
        .map_err(|error| Error::Session(format!("cannot read hc machine ls --json: {error}")))?;
    let machine = listed
        .machines
        .into_iter()
        .find(|machine| machine.name == name)
        .ok_or_else(|| Error::Session(format!("no machine {name} in `hc machine ls`")))?;
    match machine.target {
        _ if machine.via => Err(Error::Session(format!(
            "machine {name} is reached through a command, not ssh, which `hc machine ls` does not \
             show: give the command that reaches it with `BusLocation::Command`"
        ))),
        Some(target) => Ok(target),
        None => Err(Error::Session(format!(
            "machine {name} has no ssh target in `hc machine ls`"
        ))),
    }
}

fn this_machines(consumer: &str) -> Result<String> {
    if consumer.contains('@') {
        return Ok(consumer.to_owned());
    }
    let bus = Bus::open_default()
        .map_err(|error| Error::Session(format!("cannot read this machine's bus id: {error}")))?;
    Ok(format!("{consumer}@{}", bus.id()))
}

/// `ssh` to `target`, which runs `herdr-bus serve <consumer>` there. It never prompts, for a
/// password or a host key, gives up on a machine that does not answer within 15 s, and ends
/// the session once the link has been silent for 45 s. It never stays behind as the master of
/// a shared connection (`ControlPersist`), which would keep its stderr open after the session.
fn ssh(target: &str, consumer: &str) -> Command {
    let mut command = Command::new("ssh");
    command.args([
        "-o",
        "BatchMode=yes",
        "-o",
        "ControlMaster=no",
        "-o",
        "ConnectTimeout=15",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=3",
    ]);
    let destination = match target.rsplit_once(':') {
        // An IPv6 address holds colons of its own: with a port, it is in brackets, which ssh
        // does not read.
        Some((destination, port))
            if (!destination.contains(':') || destination.ends_with(']'))
                && !port.is_empty()
                && port.bytes().all(|b| b.is_ascii_digit()) =>
        {
            command.args(["-p", port]);
            destination.replace(['[', ']'], "")
        }
        _ => target.to_owned(),
    };
    command.args(["--", &destination, &remote_line(consumer)]);
    command
}

fn through(argv: &[String], consumer: &str) -> Result<(Command, String)> {
    let Some((program, arguments)) = argv.split_first() else {
        return Err(Error::Session(
            "BusLocation::Command needs a program to run".to_owned(),
        ));
    };
    let consumer = this_machines(consumer)?;
    let mut command = Command::new(program);
    command.args(arguments).arg(remote_line(&consumer));
    Ok((command, format!("through {program}")))
}

/// The line `ssh`, or the command of a `BusLocation::Command`, runs on the other machine: `serve`
/// in a login shell, whose `PATH` holds what the user installed, which the command's own line
/// may lack.
fn remote_line(consumer: &str) -> String {
    let serve = format!("exec herdr-bus serve -- {}", quote(consumer));
    format!("sh -lc {}", quote(&serve))
}

fn quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', r"'\''"))
}

fn unexpected(line: &Line) -> Error {
    match line {
        Line::Error(error) => Error::Session(error.clone()),
        line => Error::Session(format!("serve answered out of turn: {line:?}")),
    }
}

#[cfg(test)]
#[path = "lib.tests.rs"]
mod tests;
