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
