# Coding standards

The rules every change to herdr-bus follows, in its code, tests, docs and the text its agents
read. Code review checks the change against each rule, and a finding names the rule it breaks.
Leave out what the checks already enforce: clippy with `-Dwarnings`, and the complexity gate of
`make lint`.

Keep this file under 100 lines.

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

Hold each test the change adds or keeps to these rules.

- Test the behaviour callers rely on once, at the lowest level that sees it: the delivery
  guarantees (a message comes back until acknowledged, a tap reads every output, two programs at
  once lose nothing and get nothing twice) and the formats other programs read (the command's
  `--json`, the shared types). A smaller suite counts as progress.
- Judge a test by two edits: break the behaviour (a wrong value, an effect skipped or done twice)
  and it fails; change only the internals, every observable result the same, and it still passes.
  Rewrite or delete a test that fails either.
- The expected value is written by hand from the behaviour the code owes, or comes from an
  independent source, and a format this repository owns is compared with a hand-written literal.
- Delete a tautological test, or rewrite it to assert what the code computes:
  - **Mirror**: the expected value is computed by the code under test or a copy of its logic.
  - **Echo**: the test reads back a field it set, a getter of a constructor argument, or a clone.
  - **Constant pin**: `assert_eq!(LIMIT, 50)`; build the boundary input from the constant instead.
  - **Library behaviour**: std, plain serde derives, SQLite's own behaviour.
  - **Vacuous assertion**: true for every value, or met by an error as well as a success.
- A test waits on an event, never on the clock: a thread's end, a flag the other side sets, and
  ordering to prove that nothing happened. nextest ends a test still running after two minutes.

## Comments

- A comment stays only when it says what the code cannot. The kinds that stay:
  - the `///` of the clap types of `crates/herdr-bus-cli/src/lib.rs`, which `--help` prints;
  - the `//!` front page of each crate's `lib.rs`, with its examples, which run as doc tests;
  - a doc comment on the public API of a library crate that says what its signature cannot: a
    guarantee, an effect, an error, a wait;
  - behaviour that an outside system forces: SQLite, ssh, the operating system, clap.
- A surprise in our own code is fixed in the code, by a name, a type, an extraction or a test.
- rustfmt lays out the struct variants of an enum on one line only when every variant has a doc
  comment, or none: delete or keep them together.

## Docs

- `README.md` is for people deciding whether to use herdr-bus: what it is, how to install the
  command, how an app takes the crates. History goes in change descriptions, the reason for a
  choice in `SPEC.md` or the change description, a tool procedure in a skill of
  `.agents/skills/`, and how to develop here in `AGENTS.md`.

## Text for agents

- Text that agents read follows the `writing-for-agents` skill when it is installed:
  `AGENTS.md`, this file and the skills of `.agents/skills/`. Load the skill before you write or
  review such text.
