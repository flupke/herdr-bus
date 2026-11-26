//! The `herdr-bus` command: every operation of the library, for programs that cannot link it
//! and for people at a shell. With `--json`, it prints one JSON document on stdout, and an
//! error as `{"error": "…"}` there; its exit code is 1 on any error, and 2 when a request's
//! deadline passes with no reply. `serve` instead holds a session with a consumer, in JSON
//! lines over stdin and stdout.
//!
//! [`run`] is the whole command, which the `herdr-bus` binary only calls. An app that links this
//! crate can run as `herdr-bus` too: it calls `run` when it is invoked through a symbolic link
//! of that name, and ships one binary, with the command at the revision it links.

mod ask;
mod serve;
mod watch;

use std::ffi::OsString;
use std::fmt::Write as _;
use std::io::{IsTerminal, Read, Write as _};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand, ValueEnum};
use herdr_bus::{
    Bus, Connection, ConnectionId, Declared, Delivery, Message, MessageQuery, Outcome, Output, Seq,
    Start, Stored, Uuid,
};
use jiff::{SignedDuration, Timestamp};
use serde::Serialize;
use serde_json::{Value, json};

const SKILL: &str = include_str!("../SKILL.md");

#[derive(Parser)]
#[command(
    name = "herdr-bus",
    version,
    about = "A message bus in one SQLite file"
)]
struct Cli {
    /// Print one JSON document on stdout, errors included.
    #[arg(long, global = true)]
    json: bool,
    #[arg(
        long,
        global = true,
        value_name = "PATH",
        help = "The bus file [default: $HERDR_BUS_DB, else $XDG_DATA_HOME/herdr-bus/bus.db]"
    )]
    db: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Declare an output, or update its description and types.
    Declare {
        output: String,
        #[arg(long, default_value = "")]
        description: String,
        /// The message types it carries, comma-separated.
        #[arg(long, value_delimiter = ',')]
        types: Vec<String>,
    },
    /// Publish a message on an output, and print its seq.
    Publish(Publish),
    /// Ask a request on an output, and wait for its reply: print the reply's data, or exit 2
    /// once its deadline passed with no reply, saying whether a consumer took it.
    Request(Ask),
    #[command(
        about = "Take a request before handling it, and print what it found: taken, \
                 taken_before (taken already, after a crash say), replied, or expired (past its \
                 deadline: ack it and skip it)"
    )]
    Take { seq: Seq },
    /// Reply to a request, taking it if nobody did; the first reply stands.
    Reply {
        seq: Seq,
        /// The reply's data, as JSON [default: read from stdin]
        #[arg(long)]
        data: Option<String>,
    },
    /// Connect a consumer to an output, or to every output with '*' (a tap).
    Connect {
        /// `<name>`, a consumer of this machine, or `<name>@<machine id>`.
        consumer: String,
        /// An output, or '*'.
        output: String,
        /// Where a new connection starts; an existing one keeps its cursor.
        #[arg(long, value_enum, default_value_t = From::Now)]
        from: From,
    },
    /// Print a consumer's messages past its connections' cursors.
    Pull {
        /// `<name>`, a consumer of this machine, or `<name>@<machine id>`.
        consumer: String,
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    /// Acknowledge a message, and every one before it, on a connection.
    Ack { connection: ConnectionId, seq: Seq },
    /// Hold a session with a consumer over stdin and stdout, in JSON lines with or without
    /// --json, until stdin closes.
    Serve {
        /// `<name>`, a consumer of this machine, or `<name>@<machine id>`.
        consumer: String,
    },
    /// Remove a connection.
    Disconnect { connection: ConnectionId },
    /// Enable a connection.
    Enable { connection: ConnectionId },
    /// Disable a connection: it delivers nothing, and keeps its cursor.
    Disable { connection: ConnectionId },
    /// Delete an output and its connections; its messages and the taps stay. Declaring it
    /// again brings it back.
    Delete { output: String },
    /// List the outputs, with their connections.
    Outputs,
    /// List the connections, with their cursor and lag.
    Connections,
    /// List messages, newest first.
    Messages {
        /// Only those published on this output.
        #[arg(long)]
        output: Option<String>,
        /// Only those this connection reads.
        #[arg(long)]
        connection: Option<ConnectionId>,
        /// Only those before this seq.
        #[arg(long, value_name = "SEQ")]
        before: Option<Seq>,
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    /// Print one message.
    Message { seq: Seq },
    /// Print this machine's bus id, which ends the names of its consumers.
    Id,
    /// Print the skill that teaches an agent to use the bus, a SKILL.md.
    Skill,
}

#[derive(Args)]
struct Publish {
    output: String,
    #[arg(
        long,
        value_name = "UUID",
        help = "A UUID, stable across the producer's retries: an id already on the output adds \
                nothing [default: a fresh UUIDv7]"
    )]
    id: Option<Uuid>,
    #[arg(long = "type")]
    kind: String,
    #[arg(long)]
    subject: Option<String>,
    /// When the event happened, in RFC 3339 [default: now]
    #[arg(long)]
    time: Option<Timestamp>,
    /// The message's data, as JSON [default: read from stdin]
    #[arg(long)]
    data: Option<String>,
    /// A link for people, as LABEL=URL; repeat it for several.
    #[arg(long = "link", value_name = "LABEL=URL", value_parser = parse_link)]
    links: Vec<(String, String)>,
}

#[derive(Args)]
struct Ask {
    #[command(flatten)]
    message: Publish,
    /// How long to wait for the reply, such as 500ms, 5s or 10m: the request's deadline, by
    /// the bus's clock, is now plus this. Asked again under the same --id, it keeps its first
    /// deadline.
    #[arg(long, value_name = "DURATION")]
    timeout: SignedDuration,
}

#[derive(Clone, Copy, ValueEnum)]
enum From {
    Now,
    Beginning,
}

#[derive(Serialize)]
struct Published {
    seq: Seq,
    id: Uuid,
}

#[derive(Serialize)]
struct Asked {
    seq: Seq,
    id: Uuid,
    #[serde(flatten)]
    outcome: Fate,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum Fate {
    Reply(Value),
    Timeout { taken: bool },
}

#[derive(Serialize)]
struct Took {
    seq: Seq,
    state: herdr_bus::Take,
}

#[derive(Serialize)]
struct Replied {
    seq: Seq,
    replied: Timestamp,
}

struct Report {
    json: String,
    text: String,
    timed_out: bool,
}

type Done = Result<Option<Report>, String>;

/// Runs the `herdr-bus` command with `args`, as `herdr-bus` with these arguments would: it reads
/// stdin, prints on stdout and stderr, and returns the command's exit code. `args` starts with
/// the program's name, as [`std::env::args_os`] does, whose file name clap's usage and errors
/// show. It never exits the process, `--help` and `--version` included.
///
/// Return from `main` with its code: after `serve`, a thread may still be blocked reading
/// stdin, holding its lock.
pub fn run(args: impl IntoIterator<Item = impl Into<OsString>>) -> ExitCode {
    let args: Vec<OsString> = args.into_iter().map(Into::into).collect();
    // Before clap, which fails on a usage error: its error is JSON too. Past `--`, `--json` is
    // a value, as clap reads it.
    let json = args
        .iter()
        .take_while(|arg| *arg != "--")
        .any(|arg| arg == "--json");
    let outcome = match Cli::try_parse_from(args) {
        Ok(cli) => execute(cli),
        // `--help` and `--version`, which clap prints on stdout.
        Err(error) if !error.use_stderr() => {
            let _ = error.print();
            return ExitCode::SUCCESS;
        }
        Err(error) => Err(error.render().to_string()),
    };
    match print(&outcome, json) {
        // A reader that closes the pipe early has what it wanted.
        Err(error) if error.kind() != std::io::ErrorKind::BrokenPipe => ExitCode::FAILURE,
        _ => exit_code(&outcome),
    }
}

fn print(outcome: &Done, json: bool) -> std::io::Result<()> {
    match outcome {
        Ok(None) => Ok(()),
        Ok(Some(report)) if json => writeln!(std::io::stdout(), "{}", report.json),
        Ok(Some(report)) if report.timed_out => write!(std::io::stderr(), "{}", report.text),
        Ok(Some(report)) => write!(std::io::stdout(), "{}", report.text),
        Err(error) => {
            let error = error.trim_start_matches("error: ").trim_end();
            if json {
                writeln!(std::io::stdout(), "{}", json!({ "error": error }))
            } else {
                writeln!(std::io::stderr(), "herdr-bus: {error}")
            }
        }
    }
}

fn exit_code(outcome: &Done) -> ExitCode {
    match outcome {
        Err(_) => ExitCode::FAILURE,
        Ok(Some(Report {
            timed_out: true, ..
        })) => ExitCode::from(2),
        Ok(_) => ExitCode::SUCCESS,
    }
}

fn execute(cli: Cli) -> Done {
    if let Command::Skill = cli.command {
        return Ok(Some(Report::new(
            &json!({ "skill": SKILL }),
            SKILL.to_owned(),
        )));
    }
    let path = match cli.db {
        Some(path) => path,
        None => Bus::default_path().map_err(|error| error.to_string())?,
    };
    let bus = Bus::open(&path).map_err(|error| error.to_string())?;
    match cli.command {
        Command::Serve { consumer } => {
            return serve::serve(bus, &path, &consumer)
                .map(|()| None)
                .map_err(|error| error.to_string());
        }
        Command::Request(ask) => ask_report(&bus, &path, ask),
        Command::Id => Ok(Report::new(
            &json!({ "id": bus.id() }),
            format!("{}\n", bus.id()),
        )),
        Command::Declare { .. }
        | Command::Publish(_)
        | Command::Pull { .. }
        | Command::Ack { .. }
        | Command::Message { .. }
        | Command::Messages { .. }
        | Command::Take { .. }
        | Command::Reply { .. } => run_log(&bus, cli.command),
        Command::Outputs => outputs_report(&bus),
        Command::Delete { output } => delete_report(&bus, &output),
        _ => run_connections(&bus, cli.command),
    }
    .map(Some)
    .map_err(|error| error.to_string())
}

fn run_log(bus: &Bus, command: Command) -> Result<Report, Box<dyn std::error::Error>> {
    Ok(match command {
        Command::Declare {
            output,
            description,
            types,
        } => output_report(&bus.declare(&Output::new(output).describe(description).types(types))?),
        Command::Publish(publish) => {
            let output = publish.output.clone();
            let message = publish.message()?;
            let seq = bus.publish(&output, &message)?;
            Report::new(
                &Published {
                    seq,
                    id: message.id,
                },
                format!("{seq}\t{}\n", message.id),
            )
        }
        Command::Take { seq } => {
            let state = bus.take(seq)?;
            Report::new(&Took { seq, state }, format!("{state}\n"))
        }
        Command::Reply { seq, data } => {
            let reply = bus.reply(seq, read_data(data)?)?;
            Report::new(
                &Replied {
                    seq,
                    replied: reply.replied,
                },
                format!("{seq}\t{}\n", reply.replied),
            )
        }
        Command::Pull { consumer, limit } => pull_report(&bus.pull(&consumer, limit)?),
        Command::Ack { connection, seq } => {
            bus.ack(connection, seq)?;
            connection_report(&bus.connection(connection)?)
        }
        Command::Message { seq } => {
            let stored = bus.message(seq)?;
            Report::new(
                &stored,
                format!("{}\n", serde_json::to_string_pretty(&stored)?),
            )
        }
        Command::Messages {
            output,
            connection,
            before,
            limit,
        } => messages_report(&bus.messages(&MessageQuery {
            output,
            connection,
            before,
            limit,
        })?),
        _ => unreachable!("execute sends the wiring commands to run_connections"),
    })
}

fn outputs_report(bus: &Bus) -> Result<Report, Box<dyn std::error::Error>> {
    let outputs = bus.outputs()?;
    let mut text = String::new();
    for output in &outputs {
        text.push_str(&output_lines(output));
    }
    Ok(Report::new(&outputs, text))
}

fn delete_report(bus: &Bus, output: &str) -> Result<Report, Box<dyn std::error::Error>> {
    let removed = bus.delete(output)?;
    let mut text = String::new();
    for id in &removed {
        let _ = writeln!(text, "{id}");
    }
    Ok(Report::new(
        &json!({ "deleted": output, "disconnected": removed }),
        text,
    ))
}

fn run_connections(bus: &Bus, command: Command) -> Result<Report, Box<dyn std::error::Error>> {
    Ok(match command {
        Command::Connections => {
            let connections = bus.connections()?;
            let text = connections.iter().map(connection_line).collect();
            Report::new(&connections, text)
        }
        Command::Connect {
            consumer,
            output,
            from,
        } => {
            let start = match from {
                From::Now => Start::Now,
                From::Beginning => Start::Beginning,
            };
            connection_report(&bus.connection(bus.connect(&consumer, &output, start)?)?)
        }
        Command::Disconnect { connection } => {
            bus.disconnect(connection)?;
            Report::new(&json!({ "id": connection }), String::new())
        }
        Command::Enable { connection } | Command::Disable { connection } => {
            bus.set_enabled(connection, matches!(command, Command::Enable { .. }))?;
            connection_report(&bus.connection(connection)?)
        }
        _ => unreachable!("execute runs the other commands itself, or sends them to run_log"),
    })
}

fn ask_report(
    bus: &Bus,
    path: &std::path::Path,
    ask: Ask,
) -> Result<Report, Box<dyn std::error::Error>> {
    let timeout = std::time::Duration::try_from(ask.timeout)
        .map_err(|_| format!("--timeout {} is negative", ask.timeout))?;
    let output = ask.message.output.clone();
    let message = ask.message.message()?;
    let id = message.id;
    let (seq, outcome) = ask::ask(bus, path, &output, &message, timeout)?;
    Ok(match outcome {
        Outcome::Replied(reply) => {
            let text = format!("{}\n", reply.data);
            let outcome = Fate::Reply(reply.data);
            Report::new(&Asked { seq, id, outcome }, text)
        }
        Outcome::TimedOut { taken } => {
            let said = if taken {
                "taken: a consumer took it, and may still handle it"
            } else {
                "not taken: nobody will handle it"
            };
            Report {
                timed_out: true,
                ..Report::new(
                    &Asked {
                        seq,
                        id,
                        outcome: Fate::Timeout { taken },
                    },
                    format!("herdr-bus: request {seq} timed out, {said}\n"),
                )
            }
        }
        Outcome::Pending { .. } => unreachable!("ask waits until the request has an outcome"),
    })
}

fn read_data(data: Option<String>) -> Result<Value, Box<dyn std::error::Error>> {
    let data = match data {
        Some(data) => data,
        None if std::io::stdin().is_terminal() => {
            return Err("give the data with --data, or as JSON on stdin".into());
        }
        None => {
            let mut data = String::new();
            std::io::stdin().read_to_string(&mut data)?;
            data
        }
    };
    Ok(serde_json::from_str(&data).map_err(|error| format!("data is not JSON: {error}"))?)
}

impl Publish {
    fn message(self) -> Result<Message, Box<dyn std::error::Error>> {
        let mut message = Message::new(self.kind, read_data(self.data)?);
        if let Some(id) = self.id {
            message = message.id(id);
        }
        if let Some(subject) = self.subject {
            message = message.subject(subject);
        }
        if let Some(time) = self.time {
            message = message.time(time);
        }
        for (label, url) in self.links {
            message = message.link(label, url);
        }
        Ok(message)
    }
}

fn parse_link(text: &str) -> Result<(String, String), String> {
    text.split_once('=')
        .map(|(label, url)| (label.to_owned(), url.to_owned()))
        .ok_or_else(|| format!("{text:?} is not LABEL=URL"))
}

impl Report {
    fn new<T: Serialize + ?Sized>(value: &T, text: String) -> Self {
        let json = serde_json::to_string(value).expect("the bus's types serialize");
        Self {
            json,
            text,
            timed_out: false,
        }
    }
}

fn output_report(output: &Declared) -> Report {
    Report::new(output, output_lines(output))
}

fn output_lines(output: &Declared) -> String {
    let mut text = format!(
        "{}\t{}\t{}\n",
        output.name,
        output.types.join(","),
        output.description
    );
    for connection in &output.connections {
        text.push('\t');
        text.push_str(&connection_line(connection));
    }
    text
}

fn connection_report(connection: &Connection) -> Report {
    Report::new(connection, connection_line(connection))
}

fn connection_line(connection: &Connection) -> String {
    let disabled = if connection.enabled { "" } else { "\tdisabled" };
    format!(
        "{}\t{} -> {}\tcursor {}\tlag {}{disabled}\n",
        connection.id, connection.output, connection.consumer, connection.cursor, connection.lag
    )
}

fn pull_report(deliveries: &[Delivery]) -> Report {
    let mut text = String::new();
    for delivery in deliveries {
        let message = &delivery.message;
        let _ = writeln!(
            text,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}",
            delivery.connection,
            delivery.seq,
            delivery.output,
            message.kind,
            message.id,
            message.subject.as_deref().unwrap_or(""),
            message.data
        );
    }
    Report::new(deliveries, text)
}

fn messages_report(messages: &[Stored]) -> Report {
    let mut text = String::new();
    for stored in messages {
        let message = &stored.message;
        let _ = writeln!(
            text,
            "{}\t{}\t{}\t{}\t{}",
            stored.seq,
            stored.output,
            message.kind,
            message.id,
            message.subject.as_deref().unwrap_or("")
        );
    }
    Report::new(messages, text)
}

#[cfg(test)]
#[path = "lib.tests.rs"]
mod tests;
