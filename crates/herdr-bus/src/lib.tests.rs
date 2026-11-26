use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::json;
use tempfile::TempDir;

use super::*;

struct Fixture {
    _dir: TempDir,
    path: PathBuf,
    bus: Bus,
    connection: ConnectionId,
}

fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bus.db");
    let bus = Bus::open(&path).unwrap();
    bus.declare(&Output::new("events")).unwrap();
    let connection = bus.connect("inbox", "events", Start::Now).unwrap();
    Fixture {
        _dir: dir,
        path,
        bus,
        connection,
    }
}

fn message(name: &str) -> Message {
    Message::new("producer.thing.happened", json!({ "n": name }))
        .id(Uuid::new_v5(&Uuid::NAMESPACE_OID, name.as_bytes()))
}

fn names(deliveries: &[Delivery]) -> Vec<&str> {
    deliveries
        .iter()
        .map(|d| d.message.data["n"].as_str().unwrap())
        .collect()
}

#[test]
fn a_pulled_message_comes_back_until_acknowledged() {
    let Fixture { bus, path, .. } = &fixture();
    bus.publish("events", &message("a").subject("A")).unwrap();
    bus.publish("events", &message("b")).unwrap();

    let first = bus.pull("inbox", 50).unwrap();
    assert_eq!(names(&first), ["a", "b"]);
    assert_eq!(first[0].message.subject.as_deref(), Some("A"));
    assert_eq!(first[0].message.data, json!({ "n": "a" }));
    assert_eq!(names(&bus.pull("inbox", 50).unwrap()), ["a", "b"]);

    bus.ack(first[0].connection, first[0].seq).unwrap();
    let reopened = Bus::open(path).unwrap();
    assert_eq!(names(&reopened.pull("inbox", 50).unwrap()), ["b"]);

    reopened.ack(first[1].connection, first[1].seq).unwrap();
    reopened.ack(first[0].connection, first[0].seq).unwrap();
    assert!(reopened.pull("inbox", 50).unwrap().is_empty());
}

#[test]
fn a_tap_reads_every_output_at_its_own_pace() {
    let Fixture {
        bus, connection, ..
    } = &fixture();
    let tap = bus.connect("hook", TAP, Start::Now).unwrap();
    bus.declare(&Output::new("news")).unwrap();
    bus.publish("events", &message("a")).unwrap();
    bus.publish("news", &message("b")).unwrap();

    let inbox = bus.pull("inbox", 50).unwrap();
    assert_eq!(names(&inbox), ["a"]);
    bus.ack(*connection, inbox[0].seq).unwrap();

    let tapped = bus.pull("hook", 50).unwrap();
    assert_eq!(names(&tapped), ["a", "b"]);
    assert!(tapped.iter().all(|d| d.connection == tap));
    let outputs: Vec<_> = tapped.iter().map(|d| d.output.as_str()).collect();
    assert_eq!(outputs, ["events", "news"]);
    bus.ack(tap, tapped[0].seq).unwrap();
    assert_eq!(names(&bus.pull("hook", 50).unwrap()), ["b"]);
    assert_eq!(bus.connection(tap).unwrap().lag, 1);
}

#[test]
fn a_connection_from_now_skips_what_its_output_already_holds() {
    let Fixture { bus, .. } = &fixture();
    bus.publish("events", &message("old")).unwrap();
    bus.connect("late", "events", Start::Now).unwrap();
    bus.connect("all", "events", Start::Beginning).unwrap();
    bus.publish("events", &message("new")).unwrap();

    assert_eq!(names(&bus.pull("late", 50).unwrap()), ["new"]);
    assert_eq!(names(&bus.pull("all", 50).unwrap()), ["old", "new"]);
}

#[test]
fn positions_show_a_message_to_pull_exactly_where_a_pull_returns_one() {
    let Fixture {
        bus, connection, ..
    } = &fixture();
    let waiting = |consumer: &str| {
        let positions = bus.positions(consumer).unwrap();
        let shown = positions
            .iter()
            .any(|at| at.newest.is_some_and(|newest| newest > at.cursor));
        assert_eq!(shown, !bus.pull(consumer, 50).unwrap().is_empty());
        shown
    };
    bus.connect("hook", TAP, Start::Now).unwrap();
    bus.declare(&Output::new("news")).unwrap();
    assert_eq!((waiting("inbox"), waiting("hook")), (false, false));

    bus.publish("news", &message("a")).unwrap();
    assert_eq!((waiting("inbox"), waiting("hook")), (false, true));

    bus.publish("events", &message("b")).unwrap();
    assert!(waiting("inbox"));
    bus.set_enabled(*connection, false).unwrap();
    assert!(!waiting("inbox"));
}

#[test]
fn a_pull_reads_connections_in_order_up_to_its_limit() {
    let Fixture { bus, .. } = &fixture();
    bus.declare(&Output::new("other")).unwrap();
    bus.publish("other", &message("old")).unwrap();
    bus.connect("inbox", "other", Start::Beginning).unwrap();
    bus.publish("other", &message("o1")).unwrap();
    bus.publish("events", &message("p1")).unwrap();
    bus.publish("events", &message("p2")).unwrap();

    let all = bus.pull("inbox", 50).unwrap();
    assert_eq!(names(&all), ["p1", "p2", "old", "o1"]);
    assert_eq!(names(&bus.pull("inbox", 3).unwrap()), ["p1", "p2", "old"]);
}

#[test]
fn connecting_again_keeps_the_connection_as_it_was() {
    let Fixture {
        bus, connection, ..
    } = &fixture();
    let seq = bus.publish("events", &message("a")).unwrap();
    bus.publish("events", &message("b")).unwrap();
    bus.ack(*connection, seq).unwrap();
    bus.set_enabled(*connection, false).unwrap();

    let again = bus.connect("inbox", "events", Start::Beginning).unwrap();
    assert_eq!(again, *connection);
    let kept = bus.connection(again).unwrap();
    assert_eq!((kept.cursor, kept.enabled), (seq, false));
    assert!(bus.pull("inbox", 50).unwrap().is_empty());

    bus.set_enabled(*connection, true).unwrap();
    assert_eq!(names(&bus.pull("inbox", 50).unwrap()), ["b"]);
}

#[test]
fn publishing_an_id_again_returns_its_seq_and_adds_nothing() {
    let Fixture { bus, .. } = &fixture();
    let seq = bus.publish("events", &message("a")).unwrap();
    let other = Message::new("producer.other", json!(2)).id(message("a").id);
    assert_eq!(bus.publish("events", &other).unwrap(), seq);
    let next = bus.publish("events", &message("b")).unwrap();
    assert_eq!(next, Seq(seq.0 + 1));
    let pulled = bus.pull("inbox", 50).unwrap();
    assert_eq!(names(&pulled), ["a", "b"]);
    assert_eq!(pulled[0].message.kind, "producer.thing.happened");
}

#[test]
fn an_ack_of_a_message_the_connection_does_not_read_is_refused() {
    let Fixture {
        bus, connection, ..
    } = &fixture();
    bus.declare(&Output::new("other")).unwrap();
    let elsewhere = bus.publish("other", &message("x")).unwrap();
    bus.publish("events", &message("a")).unwrap();

    assert!(matches!(
        bus.ack(*connection, elsewhere),
        Err(Error::NotOnConnection { .. })
    ));
    assert!(matches!(
        bus.ack(*connection, Seq(elsewhere.0 + 100)),
        Err(Error::NotOnConnection { .. })
    ));
    assert_eq!(names(&bus.pull("inbox", 50).unwrap()), ["a"]);
}

#[test]
fn a_bare_name_is_the_consumer_of_this_machine() {
    let Fixture {
        bus, connection, ..
    } = &fixture();
    let full = format!("inbox@{}", bus.id());
    let other_form = format!("inbox@{}", bus.id().simple().to_string().to_uppercase());
    assert_eq!(
        bus.connect(&full, "events", Start::Now).unwrap(),
        *connection
    );
    assert_eq!(
        bus.connect(&other_form, "events", Start::Now).unwrap(),
        *connection
    );
    assert_eq!(bus.connection(*connection).unwrap().consumer, full);

    let far = format!("inbox@{}", Uuid::from_u128(7));
    let other = bus.connect(&far, "events", Start::Now).unwrap();
    assert_ne!(other, *connection);
    let seq = bus.publish("events", &message("a")).unwrap();
    bus.ack(*connection, seq).unwrap();
    assert!(bus.pull(&full, 50).unwrap().is_empty());
    assert_eq!(names(&bus.pull(&far, 50).unwrap()), ["a"]);

    for bad in [
        "",
        "inbox@",
        "inbox@box",
        "@0196f1c2-7a3e-7c41-9b2d-5e8f0a1b2c3d",
    ] {
        assert!(
            matches!(bus.pull(bad, 50), Err(Error::BadConsumer(_))),
            "{bad:?}"
        );
    }
}

#[test]
fn only_a_declared_output_takes_messages_and_connections() {
    let Fixture { bus, .. } = &fixture();
    for bad in ["", TAP] {
        assert!(matches!(
            bus.declare(&Output::new(bad)),
            Err(Error::BadOutput(_))
        ));
    }
    assert!(matches!(
        bus.publish("nowhere", &message("a")),
        Err(Error::UnknownOutput(_))
    ));
    assert!(matches!(
        bus.connect("inbox", "nowhere", Start::Now),
        Err(Error::UnknownOutput(_))
    ));
}

#[test]
fn a_deleted_output_keeps_its_messages_and_comes_back_as_it_was() {
    let Fixture {
        bus, connection, ..
    } = &fixture();
    let tap = bus.connect("hook", TAP, Start::Now).unwrap();
    let seq = bus.publish("events", &message("a")).unwrap();

    assert_eq!(bus.delete("events").unwrap(), [*connection]);
    assert!(bus.outputs().unwrap().is_empty());
    assert!(matches!(
        bus.publish("events", &message("b")),
        Err(Error::DeletedOutput(_))
    ));
    assert!(matches!(
        bus.connect("inbox", "events", Start::Now),
        Err(Error::DeletedOutput(_))
    ));
    assert!(matches!(bus.delete("events"), Err(Error::DeletedOutput(_))));
    assert!(bus.pull("inbox", 50).unwrap().is_empty());
    let tapped = bus.pull("hook", 50).unwrap();
    assert_eq!(names(&tapped), ["a"]);
    assert_eq!(tapped[0].connection, tap);

    let back = bus.declare(&Output::new("events")).unwrap();
    assert!(back.connections.is_empty());
    assert_eq!(bus.publish("events", &message("a")).unwrap(), seq);
    let history = MessageQuery {
        output: Some("events".to_owned()),
        ..MessageQuery::default()
    };
    assert_eq!(bus.messages(&history).unwrap().len(), 1);
}

#[test]
fn a_file_of_a_newer_schema_is_refused_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bus.db");
    let newer = rusqlite::Connection::open(&path).unwrap();
    newer.pragma_update(None, "user_version", 99).unwrap();
    drop(newer);
    let before = std::fs::read(&path).unwrap();

    assert!(matches!(
        Bus::open(&path),
        Err(Error::NewerSchema { found: 99, .. })
    ));
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert!(!dir.path().join("bus.db-wal").exists());
}

#[test]
fn concurrent_publishers_and_consumers_lose_nothing() {
    const PER_PRODUCER: usize = 200;
    let Fixture { bus, path, .. } = &fixture();
    bus.connect("hook", TAP, Start::Now).unwrap();
    let producing = AtomicUsize::new(2);
    let consume = |consumer: &str| {
        let reader = Bus::open(path).unwrap();
        let mut pulled = Vec::new();
        loop {
            let done = producing.load(Ordering::SeqCst) == 0;
            let batch = reader.pull(consumer, 7).unwrap();
            for delivery in &batch {
                pulled.push(delivery.message.id);
                reader.ack(delivery.connection, delivery.seq).unwrap();
            }
            if done && batch.is_empty() {
                return pulled;
            }
        }
    };
    let (inbox, tapped) = std::thread::scope(|scope| {
        for producer in ["p", "q"] {
            let producing = &producing;
            scope.spawn(move || {
                let bus = Bus::open(path).unwrap();
                for n in 0..PER_PRODUCER {
                    bus.publish("events", &message(&format!("{producer}{n}")))
                        .unwrap();
                }
                producing.fetch_sub(1, Ordering::SeqCst);
            });
        }
        let inbox = scope.spawn(|| consume("inbox"));
        let tapped = scope.spawn(|| consume("hook"));
        (inbox.join().unwrap(), tapped.join().unwrap())
    });
    for pulled in [inbox, tapped] {
        assert_eq!(pulled.len(), 2 * PER_PRODUCER);
        assert_eq!(
            pulled.iter().collect::<HashSet<_>>().len(),
            2 * PER_PRODUCER
        );
    }
}

const HOUR: std::time::Duration = std::time::Duration::from_secs(3600);

#[test]
fn a_request_is_taken_once_and_keeps_its_first_reply() {
    let Fixture { bus, .. } = &fixture();
    let seq = bus.request("events", &message("ask"), HOUR).unwrap();
    let Outcome::Pending { deadline } = bus.outcome(seq).unwrap() else {
        panic!("a request just asked is pending");
    };
    assert_eq!(
        bus.request("events", &message("ask"), HOUR * 2).unwrap(),
        seq
    );
    assert_eq!(bus.outcome(seq).unwrap(), Outcome::Pending { deadline });
    let request = |bus: &Bus| bus.pull("inbox", 50).unwrap()[0].request.unwrap();
    assert_eq!(
        request(bus),
        RequestState {
            deadline,
            taken: false,
            replied: false
        }
    );

    assert_eq!(bus.take(seq).unwrap(), Take::Taken);
    assert!(request(bus).taken);
    assert_eq!(bus.take(seq).unwrap(), Take::TakenBefore);

    assert_eq!(
        bus.reply(seq, json!({"answer": 1})).unwrap().data,
        json!({"answer": 1})
    );
    assert_eq!(
        bus.reply(seq, json!({"answer": 2})).unwrap().data,
        json!({"answer": 1})
    );
    assert!(request(bus).replied);
    assert_eq!(bus.take(seq).unwrap(), Take::Replied);
    let Outcome::Replied(reply) = bus.outcome(seq).unwrap() else {
        panic!("a request replied to has its reply");
    };
    assert_eq!(reply.data, json!({"answer": 1}));
    assert_eq!(
        bus.message(seq).unwrap().request.unwrap().reply,
        Some(reply)
    );
}

#[test]
fn a_request_past_its_deadline_that_nobody_took_is_never_handled() {
    let Fixture { bus, .. } = &fixture();
    let seq = bus
        .request("events", &message("late"), std::time::Duration::ZERO)
        .unwrap();
    assert_eq!(bus.take(seq).unwrap(), Take::Expired);
    assert!(matches!(bus.reply(seq, json!(1)), Err(Error::Expired(_))));
    assert_eq!(
        bus.outcome(seq).unwrap(),
        Outcome::TimedOut { taken: false }
    );
    assert_eq!(bus.take(seq).unwrap(), Take::Expired);
}

#[test]
fn a_request_taken_before_its_deadline_still_takes_its_reply_after_it() {
    let Fixture { bus, .. } = &fixture();
    let seq = bus.request("events", &message("slow"), HOUR).unwrap();
    assert_eq!(bus.take(seq).unwrap(), Take::Taken);
    bus.sql
        .execute(
            "UPDATE requests SET deadline = ?1",
            ["2000-01-01T00:00:00Z"],
        )
        .unwrap();
    assert_eq!(bus.outcome(seq).unwrap(), Outcome::TimedOut { taken: true });
    assert_eq!(bus.take(seq).unwrap(), Take::TakenBefore);

    bus.reply(seq, json!("done")).unwrap();
    assert_eq!(bus.request("events", &message("slow"), HOUR).unwrap(), seq);
    let Outcome::Replied(reply) = bus.outcome(seq).unwrap() else {
        panic!("the reply stands");
    };
    assert_eq!(reply.data, json!("done"));
}

#[test]
fn only_a_request_is_taken_or_replied_to() {
    let Fixture { bus, .. } = &fixture();
    let plain = bus.publish("events", &message("plain")).unwrap();
    assert!(matches!(
        bus.request("events", &message("plain"), HOUR),
        Err(Error::PlainMessage { .. })
    ));
    for seq in [plain, Seq(plain.0 + 100)] {
        assert!(matches!(bus.take(seq), Err(Error::NotARequest(_))));
        assert!(matches!(
            bus.reply(seq, json!(1)),
            Err(Error::NotARequest(_))
        ));
        assert!(matches!(bus.outcome(seq), Err(Error::NotARequest(_))));
    }
    assert_eq!(bus.pull("inbox", 50).unwrap()[0].request, None);
    assert!(matches!(
        bus.request("nowhere", &message("a"), HOUR),
        Err(Error::UnknownOutput(_))
    ));
}
