use herdr_bus::{Seq, Uuid};

use super::*;

/// The end of a session answers every caller that waits, whatever it waits for: an answer, a
/// request's reply, a notify. A caller waiting for nothing would hang.
#[test]
fn the_end_answers_every_caller_that_waits() {
    let waiters = Waiters::default();
    let (outcome, came) = channel();
    let requested = waiters.expect(Some(outcome)).unwrap();
    let answered = waiters.expect(None).unwrap();
    waiters.dispatch(Line::Requested {
        seq: Seq(1),
        id: Uuid::nil(),
    });
    assert!(matches!(requested.recv(), Ok(Ok(Line::Requested { .. }))));
    std::thread::scope(|scope| {
        let pulling = scope.spawn(|| waiters.wait_for_notify());
        waiters.end("closed".to_owned());
        assert!(matches!(pulling.join().unwrap(), Err(Error::Session(_))));
    });
    for receiver in [came, answered] {
        assert!(matches!(receiver.recv(), Ok(Err(Error::Session(why))) if why == "closed"));
    }
    assert!(waiters.expect(None).is_err());
}
