//! The `herdr-bus` command: every operation of the library, for programs that cannot link it
//! and for people at a shell. With `--json`, it prints one JSON document on stdout, and an
//! error as `{"error": "…"}` there; it exits 1 on any error. `serve` instead holds a session
//! with a consumer, in JSON lines over stdin and stdout.

mod serve;

use std::fmt::Write as _;
use std::io::{IsTerminal, Read, Write as _};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand, ValueEnum};
use herdr_bus::{
    Bus, Connection, ConnectionId, Declared, Delivery, Message, MessageQuery, Output, Seq, Start,
    Stored, Uuid,
};
use jiff::Timestamp;
use serde::Serialize;
use serde_json::{Value, json};

/// The skill `herdr-bus skill` prints. It names each command and leaves their options to their
/// `--help`, so that it cannot drift from them; a test checks the commands it names.
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

#[derive(Clone, Copy, ValueEnum)]
enum From {
    Now,
    Beginning,
}

/// What `publish` prints.
#[derive(Serialize)]
struct Published {
    seq: Seq,
    id: Uuid,
}

/// What a command prints: its JSON document, its fields in their documented order, or lines
/// for people.
struct Report {
    json: String,
    text: String,
}

/// What a command did: its report, or nothing for `serve`, which wrote its session already.
type Outcome = Result<Option<Report>, String>;

fn main() -> ExitCode {
    // Before clap, which fails on a usage error: its error is JSON too. Past `--`, `--json` is
    // a value, as clap reads it.
    let json = std::env::args_os()
        .take_while(|arg| arg != "--")
        .any(|arg| arg == "--json");
    let outcome = match Cli::try_parse() {
        Ok(cli) => run(cli),
        Err(error) if !error.use_stderr() => error.exit(),
        Err(error) => Err(error.render().to_string()),
    };
    let printed = match &outcome {
        Ok(None) => Ok(()),
        Ok(Some(report)) if json => writeln!(std::io::stdout(), "{}", report.json),
        Ok(Some(report)) => write!(std::io::stdout(), "{}", report.text),
        Err(error) => {
            let error = error.trim_start_matches("error: ").trim_end();
            if json {
                writeln!(std::io::stdout(), "{}", json!({ "error": error }))
            } else {
                writeln!(std::io::stderr(), "herdr-bus: {error}")
            }
        }
    };
    match printed {
        // A reader that closes the pipe early has what it wanted.
        Err(error) if error.kind() != std::io::ErrorKind::BrokenPipe => ExitCode::FAILURE,
        _ if outcome.is_err() => ExitCode::FAILURE,
        _ => ExitCode::SUCCESS,
    }
}

fn run(cli: Cli) -> Outcome {
    // It needs no bus file, and creates none.
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
        Command::Id => Ok(Report::new(
            &json!({ "id": bus.id() }),
            format!("{}\n", bus.id()),
        )),
        Command::Declare { .. }
        | Command::Publish(_)
        | Command::Pull { .. }
        | Command::Ack { .. }
        | Command::Message { .. }
        | Command::Messages { .. } => run_log(&bus, cli.command),
        Command::Outputs => outputs_report(&bus),
        Command::Delete { output } => delete_report(&bus, &output),
        _ => run_connections(&bus, cli.command),
    }
    .map(Some)
    .map_err(|error| error.to_string())
}

/// The commands of producers and consumers, and reading the log.
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
        _ => unreachable!("run sends the wiring commands to run_connections"),
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

/// The commands that make, change and list connections.
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
        _ => unreachable!("run runs the other commands itself, or sends them to run_log"),
    })
}

impl Publish {
    fn message(self) -> Result<Message, Box<dyn std::error::Error>> {
        let data = match self.data {
            Some(data) => data,
            None if std::io::stdin().is_terminal() => {
                return Err("give the message's data with --data, or as JSON on stdin".into());
            }
            None => {
                let mut data = String::new();
                std::io::stdin().read_to_string(&mut data)?;
                data
            }
        };
        let data: Value =
            serde_json::from_str(&data).map_err(|error| format!("data is not JSON: {error}"))?;
        let mut message = Message::new(self.kind, data);
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
        Self { json, text }
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
#[path = "main.tests.rs"]
mod tests;
