use std::collections::BTreeSet;

use clap::CommandFactory;

use super::*;

/// The skill names every command, and no command that is gone: a command added, renamed or
/// removed fails here until the skill says so.
#[test]
fn the_skill_names_the_commands_there_are() {
    // A command, in code: `herdr-bus pull`, a line break allowed before the command.
    let named: BTreeSet<&str> = SKILL
        .split("`herdr-bus")
        .skip(1)
        .filter_map(|after| {
            after
                .trim_start()
                .split(|c: char| !c.is_ascii_lowercase())
                .next()
        })
        .filter(|word| !word.is_empty())
        .collect();
    let cli = Cli::command();
    let commands: BTreeSet<&str> = cli.get_subcommands().map(clap::Command::get_name).collect();
    assert_eq!(named, commands);
}
