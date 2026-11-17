//! The `herdr-bus` command's JSON, which herdr-bowtie and other programs read: each format is a
//! hand-written literal, so a change breaks this test as it would break them.

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Output, Stdio};

use serde_json::{Value, json};

fn herdr_bus(db: &Path, args: &[&str], stdin: Option<&str>) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_herdr-bus"))
        .arg("--db")
        .arg(db)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    input.write_all(stdin.unwrap_or("").as_bytes()).unwrap();
    drop(input);
    child.wait_with_output().unwrap()
}

/// Runs a command that succeeds, and returns its JSON.
fn json(db: &Path, args: &[&str], stdin: Option<&str>) -> Value {
    let output = herdr_bus(db, &[args, &["--json"]].concat(), stdin);
    assert!(
        output.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

/// `value` with its timestamps, which change with each run, replaced by `"<time>"`.
fn timeless(mut value: Value) -> Value {
    match &mut value {
        Value::Object(fields) => {
            for (key, field) in fields.iter_mut() {
                let times = [
                    "declared",
                    "last_seen",
                    "created",
                    "published",
                    "deadline",
                    "taken",
                    "replied",
                ];
                // A request's `taken` is a time, or null; a delivery's, a flag.
                if times.contains(&key.as_str()) && field.is_string() {
                    *field = json!("<time>");
                } else {
                    *field = timeless(field.take());
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                *item = timeless(item.take());
            }
        }
        _ => {}
    }
    value
}

#[test]
fn the_wiring_prints_the_documented_json() {
    let dir = tempfile::tempdir().unwrap();
    let db = &dir.path().join("bus.db");
    let id = json(db, &["id"], None)["id"].as_str().unwrap().to_owned();
    let declare = [
        "declare",
        "events",
        "--description",
        "What happened",
        "--types",
        "a,b",
    ];
    assert_eq!(
        timeless(json(db, &declare, None)),
        json!({
            "name": "events",
            "description": "What happened",
            "types": ["a", "b"],
            "declared": "<time>",
            "last_seen": "<time>",
            "connections": [],
        })
    );
    // A bare name is the consumer of this machine, whose id `id` prints.
    let connection = json!({
        "id": 1,
        "consumer": format!("inbox@{id}"),
        "output": "events",
        "enabled": true,
        "cursor": 0,
        "lag": 0,
        "created": "<time>",
    });
    assert_eq!(
        timeless(json(db, &["connect", "inbox", "events"], None)),
        connection
    );
    // A tap shows in `connections`, not under the outputs.
    json(db, &["connect", "hook", "*"], None);
    assert_eq!(
        timeless(json(db, &["outputs"], None))[0]["connections"],
        json!([connection])
    );
    assert_eq!(
        json(db, &["delete", "events"], None),
        json!({ "deleted": "events", "disconnected": [1] })
    );
    assert_eq!(json(db, &["disconnect", "2"], None), json!({ "id": 2 }));
}

#[test]
fn a_consumer_reads_the_documented_json() {
    let dir = tempfile::tempdir().unwrap();
    let db = &dir.path().join("bus.db");
    let id = json(db, &["id"], None)["id"].as_str().unwrap().to_owned();
    json(db, &["declare", "events"], None);
    json(db, &["connect", "inbox", "events"], None);
    assert_eq!(json(db, &["pull", "inbox"], None), json!([]));

    let publish = [
        "publish",
        "events",
        "--id",
        "0196F1C2-7A3E-7C41-9B2D-5E8F0A1B2C3D",
        "--type",
        "producer.thing.happened",
        "--subject",
        "One line",
        "--time",
        "2026-10-31T09:30:00Z",
        "--link",
        "Source=https://example.com/?a=b",
    ];
    let data = Some(r#"{"any": ["json"]}"#);
    let message_id = "0196f1c2-7a3e-7c41-9b2d-5e8f0a1b2c3d";
    assert_eq!(
        json(db, &publish, data),
        json!({ "seq": 1, "id": message_id })
    );
    // The same id again: exit 0, the same seq.
    assert_eq!(
        json(db, &publish, data),
        json!({ "seq": 1, "id": message_id })
    );

    let message = json!({
        "id": message_id,
        "type": "producer.thing.happened",
        "subject": "One line",
        "time": "2026-10-31T09:30:00Z",
        "data": { "any": ["json"] },
        "links": [{ "label": "Source", "url": "https://example.com/?a=b" }],
    });
    // The consumer's full name reads the same as its bare one.
    assert_eq!(
        json(db, &["pull", &format!("inbox@{id}")], None),
        json!([{ "connection": 1, "seq": 1, "output": "events", "message": message }])
    );
    assert_eq!(
        timeless(json(db, &["messages", "--output", "events"], None)),
        json!([{ "seq": 1, "output": "events", "published": "<time>", "message": message }])
    );

    // Without an id, the bus generates a UUIDv7: a new message each time.
    let generated = ["publish", "events", "--type", "t", "--data", "2"];
    let first = json(db, &generated, None);
    let second = json(db, &generated, None);
    assert_eq!(
        (first["seq"].clone(), second["seq"].clone()),
        (json!(2), json!(3))
    );
    let version = |published: &Value| published["id"].as_str().unwrap().as_bytes()[14];
    assert_eq!((version(&first), version(&second)), (b'7', b'7'));

    // A connection's messages are those of its output alone, newest first.
    json(db, &["declare", "news"], None);
    json(db, &["publish", "news", "--type", "t", "--data", "4"], None);
    let seqs = |args: &[&str]| -> Vec<Value> {
        let messages = json(db, args, None);
        let messages = messages.as_array().unwrap();
        messages
            .iter()
            .map(|stored| stored["seq"].clone())
            .collect()
    };
    assert_eq!(
        seqs(&["messages"]),
        [json!(4), json!(3), json!(2), json!(1)]
    );
    assert_eq!(
        seqs(&["messages", "--connection", "1"]),
        [json!(3), json!(2), json!(1)]
    );

    let acked = json(db, &["ack", "1", "3"], None);
    assert_eq!(
        (acked["cursor"].clone(), acked["lag"].clone()),
        (json!(3), json!(0))
    );
    assert_eq!(json(db, &["pull", "inbox"], None), json!([]));
}

#[test]
fn an_error_exits_1_with_its_json() {
    let dir = tempfile::tempdir().unwrap();
    let db = &dir.path().join("bus.db");
    json(db, &["declare", "events"], None);
    for args in [
        &["pull", "inbox@box", "--json"][..],
        &["connect", "inbox", "nowhere", "--json"][..],
        &[
            "publish", "events", "--id", "4f1c9e", "--type", "t", "--data", "1", "--json",
        ][..],
        &["ack", "not-a-number", "1", "--json"][..],
    ] {
        let output = herdr_bus(db, args, None);
        assert_eq!(output.status.code(), Some(1), "{args:?}");
        let error: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert!(
            error["error"].as_str().is_some_and(|e| !e.is_empty()),
            "{error}"
        );
    }
}

/// `--version`, as `--help`, succeeds on stdout, `--json` or not: `run` prints it itself, where
/// clap's `exit` would end an app that embeds the command.
#[test]
fn the_version_succeeds_on_stdout() {
    let dir = tempfile::tempdir().unwrap();
    for args in [&["--version"][..], &["--version", "--json"][..]] {
        let output = herdr_bus(&dir.path().join("bus.db"), args, None);
        assert_eq!(output.status.code(), Some(0), "{args:?}");
        assert_eq!(
            String::from_utf8_lossy(&output.stdout),
            format!("herdr-bus {}\n", env!("CARGO_PKG_VERSION")),
        );
        assert!(output.stderr.is_empty(), "{args:?}");
    }
}

/// Programs that start together all open a bus file none has created yet: `SQLite`'s busy
/// timeout does not cover the switch of a new file to WAL. Separate processes, since `SQLite`
/// shares its locks between the connections of one.
#[test]
fn programs_opening_a_new_file_at_once_all_open_it() {
    for _ in 0..12 {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("herdr-bus/bus.db");
        let programs: Vec<_> = (0..32)
            .map(|n| {
                Command::new(env!("CARGO_BIN_EXE_herdr-bus"))
                    .arg("--db")
                    .arg(&db)
                    .args(["declare", &format!("p{n}"), "--json"])
                    .stdout(Stdio::piped())
                    .spawn()
                    .unwrap()
            })
            .collect();
        for program in programs {
            let output = program.wait_with_output().unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stdout)
            );
        }
        assert_eq!(json(&db, &["outputs"], None).as_array().unwrap().len(), 32);
    }
}

#[test]
fn a_request_nobody_takes_exits_2_and_can_no_longer_be_taken() {
    let dir = tempfile::tempdir().unwrap();
    let db = &dir.path().join("bus.db");
    json(db, &["declare", "asks"], None);
    let ask = [
        "request",
        "asks",
        "--type",
        "t",
        "--data",
        "{}",
        "--timeout",
        "0s",
    ];
    let id = "00000000-0000-0000-0000-00000000000a";
    let output = herdr_bus(db, &[&ask[..], &["--id", id, "--json"]].concat(), None);
    assert_eq!(output.status.code(), Some(2));
    let printed: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        printed,
        json!({ "seq": 1, "id": id, "timeout": { "taken": false } })
    );
    // For people, on stderr: the request's fate.
    let output = herdr_bus(db, &ask, None);
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("not taken"));

    assert_eq!(
        json(db, &["take", "1"], None),
        json!({ "seq": 1, "state": "expired" })
    );
    assert_eq!(
        herdr_bus(db, &["reply", "1", "--data", "1"], None)
            .status
            .code(),
        Some(1)
    );
}

#[test]
fn a_request_prints_its_reply_and_its_consumer_reads_the_documented_json() {
    let dir = tempfile::tempdir().unwrap();
    let db = &dir.path().join("bus.db");
    json(db, &["declare", "asks"], None);
    json(db, &["connect", "answerer", "asks"], None);
    // A session of the answerer, to learn when the request is on the bus.
    let mut serve = Command::new(env!("CARGO_BIN_EXE_herdr-bus"))
        .arg("--db")
        .arg(db)
        .args(["serve", "answerer"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(serve.stdout.take().unwrap()).lines();
    assert_eq!(lines.next().unwrap().unwrap(), r#"{"ready":{}}"#);

    let id = "00000000-0000-0000-0000-00000000000b";
    let ask = [
        "request",
        "asks",
        "--type",
        "t",
        "--id",
        id,
        "--data",
        "{}",
        "--timeout",
        "1h",
    ];
    let asker = Command::new(env!("CARGO_BIN_EXE_herdr-bus"))
        .arg("--db")
        .arg(db)
        .args(ask)
        .arg("--json")
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    assert_eq!(lines.next().unwrap().unwrap(), r#"{"notify":{}}"#);
    drop(serve.stdin.take());
    serve.wait().unwrap();

    let request = json!({ "deadline": "<time>", "taken": false, "replied": false });
    let pulled = timeless(json(db, &["pull", "answerer"], None));
    assert_eq!(pulled[0]["request"], request);
    assert_eq!(
        json(db, &["take", "1"], None),
        json!({ "seq": 1, "state": "taken" })
    );
    assert_eq!(
        json(db, &["take", "1"], None),
        json!({ "seq": 1, "state": "taken_before" })
    );
    assert_eq!(
        timeless(json(db, &["reply", "1"], Some(r#"{"answer": 42}"#))),
        json!({ "seq": 1, "replied": "<time>" })
    );
    let output = asker.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(0));
    // In its documented order, which a reader of the bytes may rely on.
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!("{{\"seq\":1,\"id\":\"{id}\",\"reply\":{{\"answer\":42}}}}\n")
    );
    let printed = json!({ "seq": 1, "id": id, "reply": { "answer": 42 } });

    // A second reply leaves the first; asked again, the request prints it at once.
    json(db, &["reply", "1", "--data", "0"], None);
    assert_eq!(json(db, &ask, None), printed);
    assert_eq!(
        json(db, &["take", "1"], None),
        json!({ "seq": 1, "state": "replied" })
    );
    assert_eq!(
        timeless(json(db, &["message", "1"], None))["request"],
        json!({
            "deadline": "<time>",
            "taken": "<time>",
            "reply": { "data": { "answer": 42 }, "replied": "<time>" },
        })
    );
    assert_eq!(
        timeless(json(db, &["pull", "answerer"], None))[0]["request"],
        json!({ "deadline": "<time>", "taken": true, "replied": true })
    );
}
