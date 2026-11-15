# herdr-bus and herdr-patch

## Goal

Let Herdr mods and their native backends exchange messages through
configurable connections. A producer publishes messages on its outputs;
a consumer reads its inputs; which output feeds which input is decided
outside both, in one small mod where connections are made and inspected.

Two pieces:

- **herdr-bus**: a Rust crate (library + CLI) storing messages in one
  SQLite file, used as an append-only log with one cursor per connection.
- **herdr-patch**: a herdr-app mod that lists registered endpoints,
  connects them, and shows messages and cursors.

No daemon. Every producer and consumer opens the database itself.

## Model

- **Endpoint**: a named output (source) or input (sink), owned by one
  program. Address: `<owner>:<name>`, for example `producer:events` or
  `consumer:inbox`.
- **Message**: an immutable entry appended to an output. Messages are
  stored once, whoever reads them.
- **Connection**: output → input. It holds a cursor: the last message
  `seq` the consumer has handled on this connection. Several connections
  can read the same output independently; one input can have several
  connections feeding it.

Producers don't know their consumers. Connecting later and replaying
history works because messages are kept.

Delivery is at-least-once: a consumer that crashes after handling a
message but before acknowledging it sees it again. Consumers are
idempotent on the message `id`.

The bus has two consumer operations, `pull` and `ack`, and no failure
handling. A message the consumer doesn't ack comes back on its next
`pull`. A consumer that can't handle a message decides what to do with it:
ack it to skip it, or leave it unacked to stop there.

## Storage

- One file: `$XDG_DATA_HOME/herdr-bus/bus.db` (default
  `~/.local/share/herdr-bus/bus.db`), overridable with `HERDR_BUS_DB`.
- `journal_mode=WAL`, `busy_timeout=5000`, `foreign_keys=ON`.
- Schema version in `PRAGMA user_version`; the crate migrates on open. A
  file whose version is newer than the crate knows is refused with a clear
  error, before any write (another program's newer build opened it first).

```sql
CREATE TABLE endpoints (
  address     TEXT PRIMARY KEY,          -- "producer:events"
  owner       TEXT NOT NULL,             -- "producer"
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
  type      TEXT NOT NULL,               -- "producer.thing.happened"
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
```

`source` must be an `out` endpoint and `target` an `in` endpoint; the
crate checks it.

## Message envelope

CloudEvents-shaped, as stored in `messages`:

```json
{
  "id": "producer:4f1c9e",
  "type": "producer.thing.happened",
  "subject": "One line for listings",
  "time": "2026-10-31T09:30:00Z",
  "data": { "any": "JSON the type defines" },
  "links": [{ "label": "Source", "url": "https://…" }]
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
bus.register(Endpoint::output("producer:events").describe("What this output carries")
    .types(["producer.thing.happened"]))?;
bus.register(Endpoint::input("consumer:inbox").types(["*"]))?;

// Producer. Returns the seq, or the existing seq if (endpoint, id) is already there.
bus.publish("producer:events", Message::new(id, "producer.thing.happened", data).subject(s))?;

// Consumer: the next messages for every enabled connection into this input,
// after each connection's cursor.
// Order: connections by id (creation order), then seq within each one.
// No fairness or priority between connections.
for d in bus.pull("consumer:inbox", 50)? {
    handle(&d.message)?;              // on error, stop: the next pull returns it again
    bus.ack(d.connection, d.seq)?;
}

// Wiring (used by the CLI and therefore herdr-patch).
bus.connect("producer:events", "consumer:inbox", Start::Now)?;   // or Start::Beginning
bus.disconnect(connection_id)?;
bus.set_enabled(connection_id, false)?;
```

Rules:

- `ack(connection, seq)` moves the cursor to `seq` if it is higher; acks
  are idempotent.
- `pull` never moves cursors by itself.
- `connect` on a connection that exists is a no-op: it keeps its cursor and
  `enabled`, ignores `Start`, and returns its id.
- `publish` of an `(endpoint, id)` already there succeeds and returns the
  existing seq; the CLI exits 0.
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
herdr-bus connections
herdr-bus connect <source> <target> [--from now|beginning]
herdr-bus disconnect <connection>
herdr-bus enable|disable <connection>
herdr-bus messages [--endpoint …] [--connection …] [--before SEQ] [--limit N]
herdr-bus message <seq>
```

`pull --json` prints an array, in `pull`'s order, of
`{"connection": 3, "seq": 43, "message": {<envelope>}}`, the envelope as in
Message envelope (`id`, `type`, `subject`, `time`, `data`, `links`); `[]`
when there is nothing.
`publish --json` prints `{"seq": 43}`.

The CLI is the way in for non-Rust programs (other languages, shell
scripts) and for herdr-patch, since a mod can't link a
Rust library.

## Retention

History is kept forever in v1: nothing deletes messages or
connections' past. Pruning is added later if the file grows enough to
matter; when it is, a message must only go once every connection from its
endpoint has a cursor past it.

## herdr-patch (mod)

A herdr-app mod in `~/.config/herdr-app/mods/herdr-patch/`, opened as a
tab or pane (`tab.new {mod: "herdr-patch"}`).

- **Data:** everything through `exec("herdr-bus", [..., "--json"])`. Live
  refresh with `watchFolder` on the database's folder (the `-wal` file
  changes on every write), debounced; no fast polling.
- **Wiring view (v1):** outputs in a left column, inputs in a right
  column, grouped by owner; connections listed between them with lag
  (`latest seq − cursor`) and an enabled switch. Connect:
  pick an output, then an input, then "from now" or "from the beginning".
  Disconnect from a connection's menu.
- **Inspection:** clicking an endpoint lists its messages (newest first,
  paged with `--before`); clicking a connection shows its messages with
  the cursor marked. A message opens
  as its envelope with `data` pretty-printed and its links clickable.
- **Stale endpoints:** endpoints whose `last_seen` is old are dimmed, not
  hidden.
- `herdr-app check` passes before calling it done.

Later, not v1: a canvas with draggable nodes and wires (`paint_path`).

## Out of scope for v1

- Filters on connections (producers decide what goes on an output).
- Remote machines in the bus (one local SQLite file). A program reads or
  writes another machine's bus by running the CLI there, through
  `hc exec --on <machine> herdr-bus …`.
- Waking consumers. Consumers poll `pull`; herdr-patch watches the file.
- Access control between programs.
- Deleting history (no `prune`; see Retention).
- Failure handling in the bus: no `fail`, `retry`, `drop`, dead-letter
  list or delivery limit. A message that keeps crashing its consumer
  blocks its connection, which shows as growing lag in herdr-patch.
- Message schemas beyond the `types` list.

## Repository

Public repo `github.com/flupke/herdr-bus`, a Cargo workspace:

```
crates/herdr-bus/        library + `herdr-bus` CLI
mods/herdr-patch/        the mod, installed into ~/.config/herdr-app/mods/
SPEC.md                  this file
```

herdr-bus is meant to be plugged into other mods' backends; those live
in their own repos and depend on the crate or call the CLI.
