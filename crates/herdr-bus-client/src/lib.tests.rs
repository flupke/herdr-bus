use std::process::Command;

use super::*;

/// The line `ssh` runs on another machine reaches `herdr-bus` with the consumer as one
/// argument, whatever it holds: `sh` itself reads it back.
#[test]
fn the_remote_line_passes_the_consumer_through_two_shells() {
    for consumer in ["app", "it's", "a b", "$HOME", "'", "-x"] {
        // No login shell here: the test's profile could print.
        let line = remote_line(consumer)
            .replace("sh -lc", "sh -c")
            .replace("exec herdr-bus", "printf %s");
        let output = Command::new("sh").args(["-c", &line]).output().unwrap();
        assert_eq!(
            String::from_utf8(output.stdout).unwrap(),
            format!("serve--{consumer}")
        );
    }
}

/// A link that went silent, that reads nothing and says nothing after `ready`, does not hold a
/// request past its deadline and the grace, a request too large for the pipe included, nor
/// the calls after it.
#[test]
fn a_request_on_a_silent_link_returns_by_its_deadline() {
    let mut silent = Command::new("sh");
    silent.args(["-c", r#"echo '{"ready":{}}'; exec sleep 600"#]);
    let mut client = Client::start(silent, "here").unwrap();
    client.grace = Duration::ZERO;
    let large = Message::new("t", serde_json::json!("x".repeat(512 * 1024)));
    for message in [large, Message::new("t", serde_json::json!(1))] {
        let asked = client.request("asks", &message, Duration::ZERO);
        assert!(matches!(asked, Err(Error::Silent(_))), "{asked:?}");
    }
}
