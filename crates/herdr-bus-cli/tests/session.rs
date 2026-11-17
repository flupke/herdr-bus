//! `herdr-bus serve` and the client crate's `Client`, through the built binary. The session's lines
//! are compared with hand-written literals: consumers in other languages read them.

use std::ffi::OsString;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use herdr_bus::{Bus, Message, Output, Start, Timestamp, Uuid};
use herdr_bus_client::{BusLocation, Client, Error, Outcome};
use serde_json::{Value, json};

/// A `herdr-bus serve` the test speaks to line by line.
struct Serve {
    child: Child,
    input: Option<ChildStdin>,
    output: BufReader<ChildStdout>,
}

impl Serve {
    fn start(db: &Path, args: &[&str]) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_herdr-bus"))
            .arg("--db")
            .arg(db)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        Self {
            input: child.stdin.take(),
            output: BufReader::new(child.stdout.take().unwrap()),
            child,
        }
    }

    fn send(&mut self, line: &str) {
        writeln!(self.input.as_mut().unwrap(), "{line}").unwrap();
    }

    /// The next line `serve` sends: the test waits for it.
    fn receive(&mut self) -> Value {
        let mut line = String::new();
        assert_ne!(self.output.read_line(&mut line).unwrap(), 0, "serve ended");
        serde_json::from_str(&line).unwrap()
    }
}

impl Drop for Serve {
    fn drop(&mut self) {
        drop(self.input.take());
        let _ = self.child.wait();
    }
}

/// A bus where the consumer `c` reads `out` from the beginning, and `out` has published `names`.
fn bus(db: &Path, names: &[&str]) -> Bus {
    let bus = Bus::open(db).unwrap();
    bus.declare(&Output::new("out")).unwrap();
    bus.connect("c", "out", Start::Beginning).unwrap();
    for name in names {
        publish(&bus, name);
    }
    bus
}

/// Publishes a message named `name` in its data, its id the number in `name`.
fn publish(bus: &Bus, name: &str) {
    let n: u128 = name.trim_start_matches('m').parse().unwrap();
    let time: Timestamp = "2026-10-31T09:30:00Z".parse().unwrap();
    let message = Message::new("t", json!({ "n": name }))
        .id(Uuid::from_u128(n))
        .time(time);
    bus.publish("out", &message).unwrap();
}

fn names(messages: &Value) -> Vec<&str> {
    messages["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|delivery| delivery["message"]["data"]["n"].as_str().unwrap())
        .collect()
}

#[test]
fn serve_sends_messages_only_when_asked() {
    let dir = tempfile::tempdir().unwrap();
    let db = &dir.path().join("bus.db");
    let bus = bus(db, &["m1", "m2", "m3"]);
    let mut serve = Serve::start(db, &["serve", "c"]);
    let notify = json!({ "notify": {} });

    assert_eq!(serve.receive(), json!({ "ready": {} }));
    // Messages wait already: one notify.
    assert_eq!(serve.receive(), notify);
    serve.send(r#"{"get_messages": {"limit": 2}}"#);
    // The answer comes next: nothing came unasked before it.
    let first = serve.receive();
    assert_eq!(
        first["messages"][0],
        json!({
            "connection": 1,
            "seq": 1,
            "output": "out",
            "message": {
                "id": "00000000-0000-0000-0000-000000000001",
                "type": "t",
                "subject": null,
                "time": "2026-10-31T09:30:00Z",
                "data": { "n": "m1" },
                "links": [],
            },
        })
    );
    assert_eq!(names(&first), ["m1", "m2"]);
    // m3 is past what the session delivered.
    assert_eq!(serve.receive(), notify);

    // An ack of a message not delivered yet is refused, and moves nothing; so are a line that
    // is not one of the session and a limit of 0, and the session goes on.
    serve.send(r#"{"ack": {"connection": 1, "seq": 3}}"#);
    assert!(serve.receive()["error"].is_string());
    serve.send(r#"{"get_messages": {"limit": 0}}"#);
    assert!(serve.receive()["error"].is_string());
    serve.input.as_mut().unwrap().write_all(b"\xff\n").unwrap();
    assert!(serve.receive()["error"].is_string());
    serve.send(r#"{"ack": {"connection": 1, "seq": 2}}"#);
    assert_eq!(
        serve.receive(),
        json!({ "acked": { "connection": 1, "seq": 2 } })
    );
    serve.send(r#"{"get_messages": {"limit": 10}}"#);
    assert_eq!(names(&serve.receive()), ["m3"]);

    // A write wakes the session: one notify, however many messages follow before the
    // consumer asks. An ack is no ask: no notify follows its answer, m5 and m6 or not.
    publish(&bus, "m4");
    assert_eq!(serve.receive(), notify);
    publish(&bus, "m5");
    publish(&bus, "m6");
    serve.send(r#"{"ack": {"connection": 1, "seq": 2}}"#);
    assert_eq!(
        serve.receive(),
        json!({ "acked": { "connection": 1, "seq": 2 } })
    );
    serve.send(r#"{"get_messages": {"limit": 10}}"#);
    // m3 was not acknowledged: it comes back.
    assert_eq!(names(&serve.receive()), ["m3", "m4", "m5", "m6"]);
}

#[test]
fn a_session_follows_its_consumers_connections_as_they_come_and_go() {
    let dir = tempfile::tempdir().unwrap();
    let db = &dir.path().join("bus.db");
    let bus = bus(db, &[]);
    // A consumer with no connection yet: its session waits.
    let mut serve = Serve::start(db, &["serve", "later"]);
    assert_eq!(serve.receive(), json!({ "ready": {} }));
    serve.send(r#"{"get_messages": {"limit": 10}}"#);
    assert_eq!(serve.receive(), json!({ "messages": [] }));

    // Connected during the session, it is notified.
    bus.connect("later", "out", Start::Beginning).unwrap();
    publish(&bus, "m1");
    assert_eq!(serve.receive(), json!({ "notify": {} }));
    serve.send(r#"{"get_messages": {"limit": 10}}"#);
    assert_eq!(names(&serve.receive()), ["m1"]);

    // Its output deleted while it handled a message: the ack has nothing left to move, and
    // there is nothing more to deliver.
    bus.delete("out").unwrap();
    serve.send(r#"{"ack": {"connection": 2, "seq": 1}}"#);
    assert_eq!(
        serve.receive(),
        json!({ "acked": { "connection": 2, "seq": 1 } })
    );
    serve.send(r#"{"get_messages": {"limit": 10}}"#);
    assert_eq!(serve.receive(), json!({ "messages": [] }));
}

/// A message line of the session: only `type` and `data` are required.
fn ask(n: u128, timeout_ms: u64) -> String {
    let id = Uuid::from_u128(n);
    format!(
        r#"{{"request": {{"output": "out", "message": {{"id": "{id}", "type": "t", "data": {n}}}, "timeout_ms": {timeout_ms}}}}}"#
    )
}

#[test]
fn serve_publishes_and_asks_and_answers_requests() {
    let dir = tempfile::tempdir().unwrap();
    let db = &dir.path().join("bus.db");
    let bus = bus(db, &[]);
    let mut serve = Serve::start(db, &["serve", "c"]);
    assert_eq!(serve.receive(), json!({ "ready": {} }));

    serve.send(r#"{"publish": {"output": "out", "message": {"type": "t", "data": 1}}}"#);
    let published = serve.receive();
    assert_eq!(published["published"]["seq"], json!(1));
    assert!(published["published"]["id"].is_string(), "{published}");
    assert_eq!(serve.receive(), json!({ "notify": {} }));

    // Nobody takes it before its deadline, which is now.
    serve.send(&ask(2, 0));
    let id = |n: u128| json!(Uuid::from_u128(n));
    assert_eq!(
        serve.receive(),
        json!({ "requested": { "seq": 2, "id": id(2) } })
    );
    assert_eq!(
        serve.receive(),
        json!({ "timeout": { "to": 2, "id": id(2), "taken": false } })
    );
    serve.send(r#"{"take": {"seq": 2}}"#);
    assert_eq!(
        serve.receive(),
        json!({ "taken": { "seq": 2, "state": "expired" } })
    );

    // Three wait at once, and the session goes on meanwhile.
    for n in 3..=5 {
        serve.send(&ask(n, 3_600_000));
        assert_eq!(
            serve.receive(),
            json!({ "requested": { "seq": n, "id": id(n) } })
        );
    }
    serve.send(r#"{"get_messages": {"limit": 10}}"#);
    let messages = serve.receive();
    let request = &messages["messages"][2]["request"];
    assert_eq!(
        (request["taken"].clone(), request["replied"].clone()),
        (json!(false), json!(false))
    );
    assert!(
        messages["messages"][0].get("request").is_none(),
        "{messages}"
    );

    // Replies come in any order, each as it is given, each naming its request.
    serve.send(r#"{"take": {"seq": 4}}"#);
    assert_eq!(
        serve.receive(),
        json!({ "taken": { "seq": 4, "state": "taken" } })
    );
    serve.send(r#"{"reply": {"to": 4, "data": {"answer": 4}}}"#);
    assert_eq!(serve.receive(), json!({ "replied": { "to": 4 } }));
    assert_eq!(
        serve.receive(),
        json!({ "reply": { "to": 4, "id": id(4), "data": { "answer": 4 } } })
    );
    // Another program replies: the session learns it from the bus.
    bus.reply(herdr_bus::Seq(5), json!("five")).unwrap();
    assert_eq!(
        serve.receive(),
        json!({ "reply": { "to": 5, "id": id(5), "data": "five" } })
    );
    serve.send(r#"{"reply": {"to": 3, "data": null}}"#);
    assert_eq!(serve.receive(), json!({ "replied": { "to": 3 } }));
    assert_eq!(
        serve.receive(),
        json!({ "reply": { "to": 3, "id": id(3), "data": null } })
    );

    // A line the session does not know is refused.
    serve.send(r#"{"cancel": {"seq": 3}}"#);
    assert!(serve.receive()["error"].is_string());
}

#[test]
fn serve_refuses_a_name_that_is_no_consumers() {
    let dir = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_herdr-bus"))
        .arg("--db")
        .arg(dir.path().join("bus.db"))
        .args(["serve", "c@box"])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    // No `ready`: the session never started.
    assert!(output.stdout.is_empty());
}

/// The variable that names, in a test run by [`run_alone`], the folder [`Fakes`] made.
const FAKES: &str = "HERDR_BUS_TEST_FAKES";

/// A folder of fake commands for the client, first on `PATH`, beside a bus file where the
/// consumer `app` of this machine reads `out`, which holds no message yet:
///
/// - `herdr-bus`, the built one, but which, for the consumer `app` of any machine, publishes `late` on `out`
///   once `serve` has answered an empty `messages`: a client that pulls then gets `late` only
///   if it waited for it. It holds the first messages `serve` sends `app` until it has
///   published `next` and `serve` has notified it, so that this notify reaches the client
///   before the answer to its next call. For the consumer `dies`, it answers `ready`, then
///   ends as a dead link does.
/// - `hc`, whose `machine ls --json` lists `box`, reached by ssh at `me@box.example:2222`;
///   `down`, at `down.example`; and `relay`, reached through a command it does not show.
/// - `ssh`, which writes its arguments to `ssh-args`, one a line, prints a line as a login
///   shell's profile can, and runs the line it would run on the machine here, without the
///   login shell, whose real profile could print anything. For
///   `down.example`, it fails as a refused connection does. It stands for the command of a
///   `BusLocation::Command` too, which runs its last argument there.
struct Fakes {
    dir: tempfile::TempDir,
}

impl Fakes {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let fakes = Self { dir };
        let bus = Bus::open(&fakes.db()).unwrap();
        bus.declare(&Output::new("out")).unwrap();
        bus.connect("app", "out", Start::Beginning).unwrap();
        let real = env!("CARGO_BIN_EXE_herdr-bus");
        fakes.script(
            "herdr-bus",
            &format!(
                r#"if [ "$3" = dies ]; then
  echo '{{"ready":{{}}}}'
  echo "link died" >&2
  exit 255
fi
case "$3" in
  app|app@*) ;;
  *) exec '{real}' "$@" ;;
esac
late= next=
'{real}' "$@" | while IFS= read -r line; do
  case "$line" in
    '{{"messages":[]}}')
      printf '%s\n' "$line"
      if [ -z "$late" ]; then
        late=1
        '{real}' publish out --type t --data '{{"n": "late"}}' >/dev/null
      fi ;;
    '{{"messages":'*)
      if [ -z "$next" ]; then
        next=1
        '{real}' publish out --type t --data '{{"n": "next"}}' >/dev/null
        IFS= read -r notify
        printf '%s\n%s\n' "$line" "$notify"
      else
        printf '%s\n' "$line"
      fi ;;
    *) printf '%s\n' "$line" ;;
  esac
done"#
            ),
        );
        fakes.script(
            "hc",
            r#"printf '%s' '{"machines": [
  {"name": "box", "target": "me@box.example:2222", "via": false, "state": "connected"},
  {"name": "down", "target": "down.example", "via": false, "state": "connected"},
  {"name": "relay", "via": true, "state": "connected"}
]}'"#,
        );
        let args = fakes.dir.path().join("ssh-args");
        fakes.script(
            "ssh",
            &format!(
                r#"printf '%s\n' "$@" > '{}'
for last; do :; done
case "$*" in
  *down.example*) echo "ssh: connect to host down.example port 22: Connection refused" >&2; exit 255 ;;
esac
echo "Welcome to the machine"
exec sh -c "$(printf '%s' "$last" | sed 's/^sh -lc/sh -c/')""#,
                args.display()
            ),
        );
        fakes
    }

    /// Reopens what [`Fakes::new`] made, in a test [`run_alone`] runs.
    fn here() -> PathBuf {
        PathBuf::from(std::env::var_os(FAKES).expect("run by run_alone"))
    }

    fn db(&self) -> PathBuf {
        self.dir.path().join("bus.db")
    }

    fn script(&self, name: &str, body: &str) {
        use std::os::unix::fs::PermissionsExt;
        let path = self.dir.path().join(name);
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    /// Runs the ignored test `name` of this binary in a process of its own, with the fakes
    /// first on `PATH` and their bus as `HERDR_BUS_DB`: a test cannot set its own process's
    /// environment, which the client reads.
    fn run_alone(&self, name: &str) {
        let mut path = OsString::from(self.dir.path());
        path.push(":");
        path.push(std::env::var_os("PATH").unwrap_or_default());
        let output = Command::new(std::env::current_exe().unwrap())
            .args([name, "--exact", "--ignored", "--nocapture"])
            .env("PATH", path)
            .env("HERDR_BUS_DB", self.db())
            .env(FAKES, self.dir.path())
            .output()
            .unwrap();
        let printed = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.status.success(), "{printed}");
        assert!(printed.contains("1 passed"), "{printed}");
    }
}

fn data_names(deliveries: &[herdr_bus::Delivery]) -> Vec<&str> {
    deliveries
        .iter()
        .map(|delivery| delivery.message.data["n"].as_str().unwrap())
        .collect()
}

#[test]
fn the_client_waits_for_a_message_then_returns_it() {
    Fakes::new().run_alone("client_here");
}

#[test]
#[ignore = "the_client_waits_for_a_message_then_returns_it runs it, in a process of its own"]
fn client_here() {
    let client = Client::connect(BusLocation::Local, "app").unwrap();
    // `out` holds nothing until `serve` has answered the pull empty.
    let deliveries = client.pull(10).unwrap();
    assert_eq!(data_names(&deliveries), ["late"]);
    // A notify for `next` waits before the ack's answer: the ack is answered all the same.
    client
        .ack(deliveries[0].connection, deliveries[0].seq)
        .unwrap();
    let next = client.pull(10).unwrap();
    assert_eq!(data_names(&next), ["next"]);
    client.ack(next[0].connection, next[0].seq).unwrap();
    drop(client);
    let bus = Bus::open(&Fakes::here().join("bus.db")).unwrap();
    assert!(bus.pull("app", 10).unwrap().is_empty());

    // A session that does not start says why, in what the command wrote.
    let Err(Error::Session(why)) = Client::connect(BusLocation::Local, "app@box") else {
        panic!("a bad consumer started a session");
    };
    assert!(why.contains("bad consumer"), "{why}");

    // A session that ends says why too, at each call from then on, a write to it or a read.
    let client = Client::connect(BusLocation::Local, "dies").unwrap();
    for _ in 0..2 {
        let Err(Error::Session(why)) = client.ack(deliveries[0].connection, deliveries[0].seq)
        else {
            panic!("a session that ended took an ack");
        };
        assert!(why.contains("ended") && why.contains("link died"), "{why}");
    }
}

#[test]
fn the_client_reaches_another_machine_through_ssh() {
    Fakes::new().run_alone("client_on_a_machine");
}

#[test]
#[ignore = "the_client_reaches_another_machine_through_ssh runs it, in a process of its own"]
fn client_on_a_machine() {
    let fakes = Fakes::here();
    let id = Bus::open(&fakes.join("bus.db")).unwrap().id();
    let client = Client::connect(BusLocation::Machine("box".to_owned()), "app").unwrap();
    // The bare name went as this machine's consumer, whose connection reads `out`.
    assert_eq!(data_names(&client.pull(10).unwrap()), ["late"]);
    let args = std::fs::read_to_string(fakes.join("ssh-args")).unwrap();
    assert_eq!(
        args.lines().collect::<Vec<_>>(),
        [
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
            "-p",
            "2222",
            "--",
            "me@box.example",
            &format!(r"sh -lc 'exec herdr-bus serve -- '\''app@{id}'\'''"),
        ]
    );
    drop(client);

    for (machine, says) in [
        ("down", "Connection refused"),
        ("relay", "BusLocation::Command"),
        ("nowhere", "no machine nowhere"),
    ] {
        let Err(Error::Session(why)) =
            Client::connect(BusLocation::Machine(machine.to_owned()), "app")
        else {
            panic!("{machine}: a session started");
        };
        assert!(why.contains(machine) && why.contains(says), "{why}");
    }
}

#[test]
fn the_client_reaches_another_machine_through_a_command() {
    Fakes::new().run_alone("client_through_a_command");
}

#[test]
#[ignore = "the_client_reaches_another_machine_through_a_command runs it, in a process of its own"]
fn client_through_a_command() {
    let fakes = Fakes::here();
    let id = Bus::open(&fakes.join("bus.db")).unwrap().id();
    // The fake `ssh` runs its last argument as the command of `hc machine add --via` does.
    let via = |args: &[&str]| BusLocation::Command(args.iter().map(|&arg| arg.into()).collect());
    let client = Client::connect(via(&["ssh", "-a", "relay", "-C"]), "app").unwrap();
    // The bare name went as this machine's consumer, whose connection reads `out`.
    assert_eq!(data_names(&client.pull(10).unwrap()), ["late"]);
    let args = std::fs::read_to_string(fakes.join("ssh-args")).unwrap();
    assert_eq!(
        args.lines().collect::<Vec<_>>(),
        [
            "-a",
            "relay",
            "-C",
            &format!(r"sh -lc 'exec herdr-bus serve -- '\''app@{id}'\'''"),
        ]
    );
    drop(client);

    let Err(Error::Session(why)) = Client::connect(via(&["ssh", "down.example"]), "app") else {
        panic!("a command that failed started a session");
    };
    assert!(
        why.contains("through ssh") && why.contains("Connection refused"),
        "{why}"
    );
    assert!(matches!(
        Client::connect(via(&[]), "app"),
        Err(Error::Session(_))
    ));
}

#[test]
fn the_client_shares_one_session_between_threads() {
    Fakes::new().run_alone("client_shared");
}

#[test]
#[ignore = "the_client_shares_one_session_between_threads runs it, in a process of its own"]
fn client_shared() {
    let bus = Bus::open(&Fakes::here().join("bus.db")).unwrap();
    bus.declare(&Output::new("asks")).unwrap();
    bus.connect("answerer", "asks", Start::Beginning).unwrap();
    // The asker reads nothing: a consumer with no connection.
    let asker = Client::connect(BusLocation::Local, "asker").unwrap();
    let answerer = Client::connect(BusLocation::Local, "answerer").unwrap();
    let hour = std::time::Duration::from_secs(3600);
    let question = |n: u128| Message::new("t", json!(n)).id(Uuid::from_u128(n));

    std::thread::scope(|scope| {
        let askers: Vec<_> = (1..=3)
            .map(|n| {
                let asker = &asker;
                scope.spawn(move || asker.request("asks", &question(n), hour).unwrap())
            })
            .collect();
        // One thread answers the three, the last asked first, while the askers wait and
        // another thread publishes.
        let answering = scope.spawn(|| {
            // A pull returns again what was not acknowledged.
            let mut pulled = std::collections::BTreeMap::new();
            while pulled.len() < 3 {
                for delivery in answerer.pull(10).unwrap() {
                    pulled.insert(delivery.seq, delivery);
                }
            }
            for delivery in pulled.values().rev() {
                assert_eq!(answerer.take(delivery.seq).unwrap(), herdr_bus::Take::Taken);
                let n = delivery.message.data.as_u64().unwrap();
                answerer.reply(delivery.seq, json!(n * 10)).unwrap();
                answerer.ack(delivery.connection, delivery.seq).unwrap();
            }
        });
        asker.publish("out", &question(9)).unwrap();
        answering.join().unwrap();
        for (n, asking) in (1..=3).zip(askers) {
            assert_eq!(asking.join().unwrap(), Outcome::Reply(json!(n * 10)));
        }
    });
    assert_eq!(
        asker
            .request("out", &question(4), std::time::Duration::ZERO)
            .unwrap(),
        Outcome::Timeout { taken: false }
    );

    // A close from another thread ends the calls of the session, those that wait and those that
    // come after (the client's own tests hold the first). The requests go on `out`, which the
    // answerer does not read: its pull waits too.
    std::thread::scope(|scope| {
        let waiting = scope.spawn(|| asker.request("out", &question(5), hour));
        let pulling = scope.spawn(|| answerer.pull(10));
        asker.close();
        answerer.close();
        for ended in [
            waiting.join().unwrap().map(drop),
            pulling.join().unwrap().map(drop),
        ] {
            let Err(Error::Session(why)) = ended else {
                panic!("a call outlived its session: {ended:?}");
            };
            assert!(why.contains("closed"), "{why}");
        }
    });
    assert!(matches!(
        asker.publish("out", &question(6)),
        Err(Error::Session(_))
    ));
}
