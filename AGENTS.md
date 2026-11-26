# What this repository is

herdr-bus is a message bus the programs on one machine share, in one SQLite file, and the
message types they share over it. `SPEC.md` describes it: the model, the schema, the library and
the `herdr-bus` command. Its Rust crates live in `crates/<name>/`, which the workspace's
`members = ["crates/*"]` takes as they land: a new crate never touches the root `Cargo.toml`.

- `crates/herdr-bus`: the library apps link.
- `crates/herdr-bus-client`: the client of a session with `herdr-bus serve`, and the session's
  lines, which `serve` speaks too.
- `crates/herdr-bus-cli`: the `herdr-bus` command, `serve` included, and its skill. Apart, so
  that an app linking the library builds none of the command. Its lib's `run` is the whole
  command: the binary only calls it, and an app can embed it.
- `crates/herdr-words`: the `herdr.words` shared type. A new shared type gets a crate of its own.

Apps, herdr-bowtie and herdr-ruminate first, take these crates by `git`, as `README.md`,
"Using it from an app", says: a change to a public item of a library crate changes their code.

# Standards

Read `CODING_STANDARDS.md`, the coding standards, before you write a change.
While you work:

- When the user corrects a concept, fix its entry in `CONTEXT.md` in the same change.
- herdr-common, herdr-bowtie and herdr-progressive-reviewer came first, on the same stack,
  checked out beside this repository (`../herdr-common`, `../herdr-bowtie`): read how they do a
  thing before you design it here. herdr-bowtie's `crates/store` is the model for SQLite.
- Design types first, then implement their methods.

# Tests

The Tests section of `CODING_STANDARDS.md` says which tests earn their place. Every change
deletes the tests it makes obsolete or redundant. Acceptance criteria name outcomes the user
can see, not lists of test cases.

A change to the bus or its command is tested from a shell with the command, as its user would,
on a private bus file, never the user's: run the checks of the `shell-checks` skill
(`.agents/skills/shell-checks/SKILL.md`).

A change to the skill (`crates/herdr-bus-cli/SKILL.md`) is tested by a fresh subagent that knows
only `herdr-bus skill` and the commands' `--help`: on a private bus file, it builds a producer, a
consumer, a tap, an asker and an answerer, says which consumer name a script of this machine
gives to read another machine's bus, and reports where the skill left it unsure. Fix the skill
until it reports nothing that matters.

A change to docs alone is tested by searching README.md, AGENTS.md, CONTEXT.md, SPEC.md,
CODING_STANDARDS.md, the skills of `.agents/skills/` and the crates' doc comments for each
statement of what it changes, until all agree with it.

# Change workflow

Follow these steps for every change: a feature, a fix, or a change to docs.

1. Create a fresh jj change before implementation. Use the previous change as the fixed point
   for this change.
2. Implement the change, and check it as you go with the levels that fit it, cheapest first:
   1. `make lint`: clippy on every target, the complexity gate, and spelling.
   2. `make test CRATES="crate-a crate-b"`: the unit tests of the crates the change touches.
   3. `make test`: every unit test of the workspace.
   Once the change is complete, run `make fmt` and bring `SPEC.md` up to date: edit each line
   the change made false.
3. For a change the user can see, run it as its user would, look at it, and fix what you find.
4. Invoke the `code-review` skill against the fixed point; the subagents it starts are
   authorized. Check each finding against the code before acting on it, fix those that hold,
   and review again until a round finds nothing that you fix, for at most two rounds. Report
   any finding you leave unfixed, with the reason.
5. Once step 4 is over, run the gate, with the change as the working copy: `make check`. Fix
   whatever fails, even outside the change, run `make fmt`, and run the gate again until it
   passes. This repository runs no CI: before you push for a pull request, `jj git fetch`,
   rebase the change onto `main` and run the gate there.
6. Describe the change with `jj describe`: a plain imperative subject, then a first paragraph
   on why, then what changed for the user.
7. Keep later user-feedback fixes in the same change, and run the gate again once each set of
   them is done. Create another change only when the user asks for something new.

Run the checks in the dev shell, through `scripts/check`: `scripts/dev-shell scripts/check
check`. The dev shell brings the pinned Rust toolchain and the tools the checks run, in the
default workspace and in any other jj workspace alike; `scripts/check` keeps the whole output
under `target/checks/` and ends on the check's status. Keep the warning gate of the checks
(`-Dwarnings`) enabled; fix warnings in the current change instead of overriding it.

## When herdr-bowtie runs you

herdr-bowtie runs its workers on this repository too, each in its task's jj workspace. There,
its procedures replace steps 1, 4, 6 and 7: start, review, describe and ship as they say. Steps
2, 3 and 5 still apply.

# The user's bus

Tests and checks run on a bus file of their own, in a temporary folder, through `--db` or
`HERDR_BUS_DB`: never the user's (`~/.local/share/herdr-bus/bus.db`), which their programs
share. `make test` unsets `HERDR_BUS_DB`.

# Skill settings

## Agent skills

### Issue tracker

Issues live in GitHub Issues for flupke/herdr-bus (via `gh`). See
`.agents/settings/issue-tracker.md`.

### Triage labels

Default five-role vocabulary (`needs-triage`, `needs-info`, `ready-for-agent`,
`ready-for-human`, `wontfix`). See `.agents/settings/triage-labels.md`.

### Domain docs

Single-context: root `CONTEXT.md`, and no ADRs. See `.agents/settings/domain.md`.
