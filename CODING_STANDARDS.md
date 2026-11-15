# Coding standards

The rules every change to herdr-bus follows, in its code, tests, docs and the text its agents
read. Code review checks the change against each rule, and a finding names the rule it breaks.
Leave out what the checks already enforce: clippy with `-Dwarnings`, and the complexity gate of
`make lint`.

Keep this file under 100 lines. When a topic outgrows it, move the topic to a page of
`.agents/wiki/` and leave a line here that says when to read the page.

## Names

- Name things with the terms of `CONTEXT.md`, in code, docs and command output, and use none of
  the synonyms it lists to avoid.

## Design

- herdr-bus is open source: what it runs assumes none of the maintainer's own tools or paths.
  The client finds the command on `PATH`, a login shell's on another machine; it runs `hc`
  and `ssh` only for `BusLocation::Machine`, to find that machine's target and reach it, and
  for `BusLocation::Command` only the command the app gave.
- The bus stays agnostic: it knows no app, agent, pane or mention. A message's `type` and `data`
  are its producer's. A type several programs share goes in a crate of its own, which the bus
  crate does not depend on (`SPEC.md`, Shared types).
- `SPEC.md` describes what is built: a change to the model, the schema, the library or the
  command changes it in the same change.
- The command's `--json` output and the schema are read by other programs: a change to either
  is a change to their interface. A schema change is a new migration, never an edit of an
  applied one.
- `crates/herdr-bus-cli/SKILL.md`, which `herdr-bus skill` prints, teaches agents the bus: a change
  to how it is used changes the skill in the same change. The skill names commands and never
  repeats their options, which stay in each command's `--help`.
- Writes run in one short `BEGIN IMMEDIATE` transaction each; reads that need a consistent view
  run in one transaction.

## Rust

- Keep crates small: a tiny crate is welcome, and a big one is worth splitting.
- Use `#[must_use]` only when ignoring a return value is likely to cause a bug, or where clippy
  asks for it on a builder.
- Use the smallest visibility that permits the required use. The `unreachable_pub` lint catches
  a needless `pub`; review catches a `pub(crate)` that only its own module uses.
- A crate takes the workspace's lints and package fields: `[lints] workspace = true`, and
  `version`, `edition`, `rust-version` and `license` as `.workspace = true`, and its
  dependencies from `[workspace.dependencies]`.
- Apps link these crates: keep `rusqlite` on the version the apps use (`Cargo.toml`).

## Tests

- `.agents/wiki/unit-tests.md` holds the testing policy and the kinds of tautological test:
  read it before you review a test, and hold each test the change adds or keeps to it.

## Docs

- `README.md` is for people deciding whether to use herdr-bus: what it is, how to install the
  command, how an app takes the crates. History goes in change descriptions, decisions in
  `.agents/adr/`, knowledge for agents in the wiki, and how to develop here in `AGENTS.md`.

## Text for agents

- Text that agents read follows the `writing-for-agents` skill when it is installed:
  `AGENTS.md`, this file and the wiki. Load the skill before you write or review such text.
