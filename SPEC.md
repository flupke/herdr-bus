# herdr-bus and herdr-patch

## Goal

Let Herdr mods and their native backends exchange messages through
configurable connections. A Slack/X/email reader marks messages with its
own triggers and publishes them on an output; Bowtie reads an input and
turns what arrives into tasks. Sentry, Honeycomb and others plug in the
same way later. Connections are made and inspected in one small mod.

Two pieces:

- **herdr-bus**: a Rust crate (library + CLI) storing messages in one
  SQLite file, used as an append-only log with one cursor per connection.
- **herdr-patch**: a herdr-app mod that lists registered endpoints,
  connects them, and shows messages, cursors and failures.

No daemon. Every producer and consumer opens the database itself.

## Model

- **Endpoint**: a named output (source) or input (sink), owned by one
  program. Address: `<owner>:<name>`, for example `slack:flagged`,
  `sentry:new-issues`, `bowtie:inbox`.
- **Message**: an immutable entry appended to an output. Messages are
  stored once, whoever reads them.
- **Connection**: output → input. It holds a cursor: the last message
  `seq` the consumer has handled on this connection. Several connections
  can read the same output independently; one input can have several
  connections feeding it.
- **Failure**: a message a consumer read but could not handle. Recorded,
  the cursor moves on, so one bad message never blocks a connection.

Producers don't know their consumers. Connecting later and replaying
history works because messages are kept.

Delivery is at-least-once: a consumer that crashes after handling a
message but before acknowledging it sees it again. Consumers are
idempotent on the message `id`.

## Storage

- One file: `$XDG_DATA_HOME/herdr-bus/bus.db` (default
  `~/.local/share/herdr-bus/bus.db`), overridable with `HERDR_BUS_DB`.
- `journal_mode=WAL`, `busy_timeout=5000`, `foreign_keys=ON`.
- Schema version in `PRAGMA user_version`; the crate migrates on open.

```sql
CREATE TABLE endpoints (
  address     TEXT PRIMARY KEY,          -- "slack:flagged"
  owner       TEXT NOT NULL,             -- "slack"
  kind        TEXT NOT NULL CHECK (kind IN ('out', 'in')),
  description TEXT NOT NULL DEFAULT '',
  types       TEXT NOT NULL DEFAULT '[]', -- JSON array of message types it emits/accepts
  registered  TEXT NOT NULL,             -- RFC 3339
  last_seen   TEXT NOT NULL
);

CREATE TABLE messages (
  seq       INTEGER PRIMARY KEY AUTOINCREMENT,
  endpoint  TEXT NOT NULL REFERENCES endpoints(address),
  id        TEXT NOT NULL,               -- producer-chosen, stable across retries
  type      TEXT NOT NULL,               -- "slack.message.flagged"
  subject   TEXT,                        -- short human line for listings
  time      TEXT NOT NULL,               -- when the event happened
  published TEXT NOT NULL,               -- when it was appended
  data      TEXT NOT NULL,               -- JSON
  links     TEXT NOT NULL DEFAULT '[]',  -- JSON array of {label, url}
  UNIQUE (endpoint, id)
);

CREATE TABLE connections (
  id       INTEGER PRIMARY KEY,
  source   TEXT NOT NULL REFERENCES endpoints(address),
  target   TEXT NOT NULL REFERENCES endpoints(address),
  enabled  INTEGER NOT NULL DEFAULT 1,
  cursor   INTEGER NOT NULL,             -- last handled messages.seq
  created  TEXT NOT NULL,
  UNIQUE (source, target)
);

CREATE TABLE failures (
  id         INTEGER PRIMARY KEY,
  connection INTEGER NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
  seq        INTEGER NOT NULL REFERENCES messages(seq),
  error      TEXT NOT NULL,
  attempts   INTEGER NOT NULL DEFAULT 1,
  state      TEXT NOT NULL CHECK (state IN ('failed', 'retry', 'dropped')),
  at         TEXT NOT NULL,
  UNIQUE (connection, seq)
);
```

`source` must be an `out` endpoint and `target` an `in` endpoint; the
crate checks it.

## Message envelope

CloudEvents-shaped, as stored in `messages`:

```json
{
  "id": "slack:C0123:1730367000.1234",
  "type": "slack.message.flagged",
  "subject": "#incidents: checkout 500s since deploy",
  "time": "2026-10-31T09:30:00Z",
  "data": { "channel": "incidents", "author": "…", "text": "…", "permalink": "…" },
  "links": [{ "label": "Slack", "url": "https://…" }]
}
```

`data` is free per type. Producers document their types in their
endpoint's `types`. Consumers ignore types they don't handle (and `ack`
them).

## Library API (Rust)

```rust
let bus = Bus::open_default()?;                    // or Bus::open(path)

// Every program registers its endpoints at startup. Idempotent; updates
// description, types and last_seen.
bus.register(Endpoint::output("slack:flagged").describe("Messages matching my triggers")
    .types(["slack.message.flagged"]))?;
bus.register(Endpoint::input("bowtie:inbox").types(["*"]))?;

// Producer. Returns the seq, or the existing seq if (endpoint, id) is already there.
bus.publish("slack:flagged", Message::new(id, "slack.message.flagged", data).subject(s))?;

// Consumer: the next messages for every enabled connection into this input,
// after each connection's cursor, plus failures marked for retry.
// Order: connections by id (creation order), then seq within each one.
// No fairness or priority between connections.
for d in bus.pull("bowtie:inbox", 50)? {
    match handle(&d.message) {
        Ok(()) => bus.ack(d.connection, d.seq)?,
        Err(e) => bus.fail(d.connection, d.seq, &e.to_string())?,
    }
}

// Wiring (used by the CLI and therefore herdr-patch).
bus.connect("slack:flagged", "bowtie:inbox", Start::Now)?;   // or Start::Beginning
bus.disconnect(connection_id)?;
bus.set_enabled(connection_id, false)?;
bus.retry(failure_id)?;   // state = retry: pull returns it again
bus.drop_failure(failure_id)?;
```

Rules:

- `ack(connection, seq)` moves the cursor to `seq` if it is higher; acks
  are idempotent. Acking a retried failure deletes the failure row.
- `fail` records or updates the failure (`attempts += 1`) and moves the
  cursor like `ack`.
- `pull` never moves cursors by itself.
- Writes run in short `BEGIN IMMEDIATE` transactions.

## CLI

Same crate, binary `herdr-bus`. Every command takes `--json` (machine
output, one JSON document on stdout) and `--db <path>`. Errors: exit 1,
`{"error": "…"}` on stdout in `--json` mode.

```
herdr-bus endpoints                          # registry, with lag per connection
herdr-bus register <address> --kind out|in [--description …] [--types a,b]
herdr-bus publish <output> --id … --type … [--subject …] [--time …] [--data JSON | stdin]
herdr-bus pull <input> [--limit N]
herdr-bus ack <connection> <seq>
herdr-bus fail <connection> <seq> --error …
herdr-bus connections
herdr-bus connect <source> <target> [--from now|beginning]
herdr-bus disconnect <connection>
herdr-bus enable|disable <connection>
herdr-bus messages [--endpoint …] [--connection …] [--before SEQ] [--limit N]
herdr-bus message <seq>
herdr-bus failures [--connection …] [--state failed|retry|dropped]
herdr-bus retry|drop <failure>
```

The CLI is the way in for non-Rust programs (the Python digest
collector, shell scripts) and for herdr-patch, since a mod can't link a
Rust library.

## Retention

History is kept forever in v1: nothing deletes messages, failures or
connections' past. Pruning is added later if the file grows enough to
matter; when it is, a message must only go once every connection from its
endpoint has a cursor past it and it has no open failure.

## herdr-patch (mod)

A herdr-app mod in `~/.config/herdr-app/mods/herdr-patch/`, opened as a
tab or pane (`tab.new {mod: "herdr-patch"}`).

- **Data:** everything through `exec("herdr-bus", [..., "--json"])`. Live
  refresh with `watchFolder` on the database's folder (the `-wal` file
  changes on every write), debounced; no fast polling.
- **Wiring view (v1):** outputs in a left column, inputs in a right
  column, grouped by owner; connections listed between them with lag
  (`latest seq − cursor`), failure count and an enabled switch. Connect:
  pick an output, then an input, then "from now" or "from the beginning".
  Disconnect from a connection's menu.
- **Inspection:** clicking an endpoint lists its messages (newest first,
  paged with `--before`); clicking a connection shows its messages with
  the cursor marked, and its failures with Retry and Drop. A message opens
  as its envelope with `data` pretty-printed and its links clickable.
- **Stale endpoints:** endpoints whose `last_seen` is old are dimmed, not
  hidden.
- `herdr-app check` passes before calling it done.

Later, not v1: a canvas with draggable nodes and wires (`paint_path`).

## Out of scope for v1

- Filters on connections (producers' own triggers decide what goes on an
  output).
- Remote machines (one local SQLite file).
- Waking consumers. Consumers poll `pull`; herdr-patch watches the file.
- Access control between programs.
- Deleting history (no `prune`; see Retention).
- Message schemas beyond the `types` list.

## Repository

Public repo `github.com/flupke/herdr-bus`, a Cargo workspace:

```
crates/herdr-bus/        library + `herdr-bus` CLI
examples/sentry-poller/  first producer, the reference integration
mods/herdr-patch/        the mod, installed into ~/.config/herdr-app/mods/
SPEC.md                  this file
```

herdr-bus is meant to be plugged into other mods' backends (the digest
collector, Bowtie, Honeycomb, …); those live in their own repos and
depend on the crate or call the CLI.

## First producer: sentry-poller

The example every later integration copies.

- Registers `sentry:new-issues` (out) at startup.
- Polls Sentry's REST API for issues first seen since its last run, per
  configured org/project, with a token from the environment
  (`SENTRY_AUTH_TOKEN`, loaded from `~/.secrets` like the digest).
- Publishes each as `sentry.issue.new`, `id` = `sentry:<issue id>`, so
  re-polling the same window is harmless (`UNIQUE (endpoint, id)`).
  `subject` is the issue title, `links` the issue URL, `data` the fields
  a fixer needs (project, culprit, level, first/last seen, count,
  permalink).
- Its own state (last poll time) lives in its own file, not in the bus.
- Runs once per invocation; scheduling is a systemd user timer.
