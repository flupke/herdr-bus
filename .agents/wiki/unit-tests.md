# Tests

Which tests to write and keep, and the tautological tests code review removes.

## Policy

Test the behaviour that matters once, at the lowest level that sees it. The bus's callers rely on
its delivery guarantees (a message comes back until acknowledged, a tap reads every output, two
programs at once lose nothing and get nothing twice) and on the formats other programs read (the
command's `--json`, the shared types): test those. Every change deletes the tests it makes
obsolete or redundant, and a smaller suite counts as progress.

A test earns its place when it checks behaviour a caller relies on, that the code does not
make obvious, at the lowest level that sees it, and that no other test checks. Judge it by two
edits: break the behaviour (a wrong value, an effect skipped or done twice) and it fails;
change only the internals, with every observable result the same, and it still passes. A test
that survives the first edit is tautological; one that fails the second is pinned to the
implementation: rewrite it to assert what callers observe, or delete it.

The library's tests (`crates/herdr-bus/src/lib.tests.rs`) hold the delivery guarantees; the
command's (`crates/herdr-bus-cli/tests/command.rs`) hold its JSON and its exit status, and the
session's (`crates/herdr-bus-cli/tests/session.rs`) the lines of `serve` and the client of
`crates/herdr-bus-client`, all through the built binary, which only the command's crate can
reach (`CARGO_BIN_EXE_herdr-bus`). Each test opens a bus file of its own in a temporary folder.

The client finds `herdr-bus`, `hc` and `ssh` on `PATH`, and its bus through `HERDR_BUS_DB`, but a
test cannot set its own process's environment: `unsafe_code` is forbidden. So a test of the
client puts fake commands first on a `PATH` of its own and runs an ignored test of the same
binary in a process of its own with it (`Fakes` in `tests/session.rs`). Never a real `ssh`.

## Tests that can go red

- The expected value is written by hand from the behaviour the code owes, or comes from an
  independent source: an external tool's output, a second code path.
- The assertion reads what production code produced: a return value, a file, the command's
  output.
- A format this repository owns is compared with a hand-written literal, so a rename breaks the
  test as it would break the programs that read it.

## Tautological tests

Code review deletes a tautological test, or rewrites it to assert what the code computes. The
kinds:

- **Mirror**: the expected value is computed by the code under test, or by a copy of its logic,
  so a bug lands on both sides.
- **Echo**: the test reads back a field it set, a getter of a constructor argument, or a clone.
- **Constant pin**: `assert_eq!(LIMIT, 50)`. Build the boundary input from the constant
  instead.
- **Library behaviour**: std, serde derives without custom attributes, SQLite's own behaviour.
  The dependency's own tests cover them.
- **Vacuous assertion**: true for every value the type can hold, or met by an error reply as
  well as a success.

## Waiting

A test never waits on the wall clock: no sleep, no loop that polls until a deadline, no window
that proves nothing happened. Wake the test with an event: a thread's end, a flag the other
side sets. Prove that nothing happened by ordering: a pull after the producer finished sees
everything it published. A test may block without a timeout of its own: nextest ends one still
running after two minutes (`.config/nextest.toml`) and reports it as failed.
