use std::ffi::OsString;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use herdr_bus::{Bus, Message, Output, Start, Timestamp, Uuid};
use herdr_bus_client::{BusLocation, Client, Error, Outcome};
use serde_json::{Value, json};

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

fn bus(db: &Path, names: &[&str]) -> Bus {
    let bus = Bus::open(db).unwrap();
    bus.declare(&Output::new("out")).unwrap();
    bus.connect("c", "out", Start::Beginning).unwrap();
    for name in names {
        publish(&bus, name);
    }
    bus
}

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
    assert_eq!(serve.receive(), notify);
    serve.send(r#"{"get_messages": {"limit": 2}}"#);
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
    assert_eq!(serve.receive(), notify);

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
    assert_eq!(names(&serve.receive()), ["m3", "m4", "m5", "m6"]);
}

#[test]
fn a_session_follows_its_consumers_connections_as_they_come_and_go() {
    let dir = tempfile::tempdir().unwrap();
    let db = &dir.path().join("bus.db");
    let bus = bus(db, &[]);
    let mut serve = Serve::start(db, &["serve", "later"]);
    assert_eq!(serve.receive(), json!({ "ready": {} }));
    serve.send(r#"{"get_messages": {"limit": 10}}"#);
    assert_eq!(serve.receive(), json!({ "messages": [] }));

    bus.connect("later", "out", Start::Beginning).unwrap();
    publish(&bus, "m1");
    assert_eq!(serve.receive(), json!({ "notify": {} }));
    serve.send(r#"{"get_messages": {"limit": 10}}"#);
    assert_eq!(names(&serve.receive()), ["m1"]);

    bus.delete("out").unwrap();
    serve.send(r#"{"ack": {"connection": 2, "seq": 1}}"#);
    assert_eq!(
        serve.receive(),
        json!({ "acked": { "connection": 2, "seq": 1 } })
    );
    serve.send(r#"{"get_messages": {"limit": 10}}"#);
    assert_eq!(serve.receive(), json!({ "messages": [] }));
}

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
    assert!(output.stdout.is_empty());
}

const FAKES: &str = "HERDR_BUS_TEST_FAKES";

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
    let deliveries = client.pull(10).unwrap();
    assert_eq!(data_names(&deliveries), ["late"]);
    client
        .ack(deliveries[0].connection, deliveries[0].seq)
        .unwrap();
    let next = client.pull(10).unwrap();
    assert_eq!(data_names(&next), ["next"]);
    client.ack(next[0].connection, next[0].seq).unwrap();
    drop(client);
    let bus = Bus::open(&Fakes::here().join("bus.db")).unwrap();
    assert!(bus.pull("app", 10).unwrap().is_empty());

    let Err(Error::Session(why)) = Client::connect(BusLocation::Local, "app@box") else {
        panic!("a bad consumer started a session");
    };
    assert!(why.contains("bad consumer"), "{why}");

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
    let via = |args: &[&str]| BusLocation::Command(args.iter().map(|&arg| arg.into()).collect());
    let client = Client::connect(via(&["ssh", "-a", "relay", "-C"]), "app").unwrap();
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
        let answering = scope.spawn(|| {
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
