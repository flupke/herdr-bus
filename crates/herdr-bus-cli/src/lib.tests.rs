use std::collections::BTreeSet;

use clap::CommandFactory;

use super::*;

#[test]
fn the_skill_names_the_commands_there_are() {
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
