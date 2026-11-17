# herdr-bus and herdr-patch

## Goal

Let Herdr mods, their native backends and other programs exchange messages
through configurable connections. A producer publishes messages on outputs;
a consumer reads the outputs it is connected to; which consumer reads which
output is decided by connecting them, by the consumer itself or in one small
mod where connections are made and inspected.

Two pieces:

- **herdr-bus**: Rust crates, a library and the `herdr-bus` command,
  storing messages in one SQLite file, used as an append-only log with one
  cursor per connection.
- **herdr-patch**: a herdr-app mod that lists the outputs and the
  consumers, connects them, and shows messages and cursors.

No daemon. Every producer and consumer opens the database itself. A
consumer that waits for its messages, on the bus's machine or another,
holds a session with `herdr-bus serve`: a process of its own, which ends
with the consumer (Sessions).

## Model

- **Bus**: one machine's SQLite file. It has an id, a random UUID made
  with the file, which `herdr-bus id` prints: the machine id of that
  machine's consumers.
- **Output**: a named place messages are published on, declared on the
  bus. Its name is a free string, not empty and not `*`, global to the bus:
  several programs may publish on one output.
- **Message**: an immutable entry appended to an output. Messages are
  stored once, whoever reads them.
- **Consumer**: a reader, named `<name>@<machine id>`, the machine id being
  the id of the bus of the machine it runs on. A bare `<name>` (no `@`)
  means a consumer of this machine: the bus completes it with its own id
  when a command or call comes in. A name with an `@` is split at the last
  one: the name must not be empty, and the machine id must be a UUID, else
  the consumer is refused. The bus stores and compares the full name, its
  machine id hyphenated, never the bare one, and checks its shape only.
- **Connection**: one consumer's cursor on one output: the last message
  `seq` the consumer has handled there. Connecting is how a consumer
  subscribes; there is no registration of consumers, and one with no
  connection has no messages. Several consumers read one output
  independently, and one consumer reads several outputs.
- **Tap**: a connection to every output, present and future (output `*`),
  with one cursor on the log's `seq`. It reads a copy: it blocks and
  rewrites nothing. A hook on everyone's messages is a tap.
- **Deleted output**: an output someone deleted, such as one of a program
  that was uninstalled. Its connections go, taps stay; its row stays,
  marked deleted, so that its messages stay, for the taps that have yet to
  read them and for history. Declaring it again brings it back, with its
  messages and without its connections.
- **Session**: a consumer's link to `herdr-bus serve <consumer>`, one JSON
  line each way over the command's stdin and stdout. It tells the consumer
  when it has messages, sends them only when asked, and takes its acks. It
  also publishes, and asks, takes and replies to requests, for a program on
  that machine or another.
- **Request**: a message published with a deadline, which takes one reply.
  It is a message of its output like any other: consumers pull and ack it,
  a tap sees it. Its deadline is set once, when it is first asked: the time
  of the bus's machine plus the asker's timeout. Asking again under the same
  `(output, id)` is the same request: its seq, its deadline, its reply.
- **Reply**: the one answer to a request, free JSON, kept with the request
  in the bus it was asked on. It is no message: no output carries it, and no
  tap receives it. The first reply stands. It is matched to its request by
  the request's seq, and read where the request was asked, by an asker on
  another machine through the session it asked through: it travels nowhere.
- **Take**: what the consumer that answers a request does before it works
  on it, in one write. A request past its deadline that nobody took cannot
  be taken, judged by the clock of the bus's machine in that write, never by
  the consumer's: a request its asker gave up on is never handled later.
  The bus does not record who took a request: one program answers the
  requests of an output, and a second take, `taken_before`, is its own
  before a crash.

Producers don't know their consumers. Connecting later and replaying
history works because messages are kept.

Delivery is at-least-once: a consumer that crashes after handling a
message but before acknowledging it sees it again. Consumers are
idempotent on the message `id`.

A request delivered again stays one request: a consumer that took it before
a crash is told so by its next `take`, and dedupes on the message `id` in its
own store; one replied to is told it has its reply.

The bus has two consumer operations on messages, `pull` and `ack`, two on
requests, `take` and `reply`, and no failure handling. A message the consumer doesn't ack comes back on its next
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
CREATE TABLE bus (
  id TEXT NOT NULL                       -- a random UUID, made with the file; one row
);

CREATE TABLE outputs (
  name        TEXT PRIMARY KEY,          -- "events"
  description TEXT NOT NULL DEFAULT '',
  types       TEXT NOT NULL DEFAULT '[]', -- JSON array of the message types it carries
  declared    TEXT NOT NULL,             -- RFC 3339
  last_seen   TEXT NOT NULL,
  deleted     TEXT                       -- RFC 3339 when deleted, else NULL
);

CREATE TABLE messages (
  seq       INTEGER PRIMARY KEY AUTOINCREMENT,
  output    TEXT NOT NULL REFERENCES outputs(name),
  id        TEXT NOT NULL,               -- UUID: the producer's, stable across retries, or generated
  type      TEXT NOT NULL,               -- "producer.thing.happened"
  subject   TEXT,                        -- short human line for listings
  time      TEXT NOT NULL,               -- when the event happened
  published TEXT NOT NULL,               -- when it was appended
  data      TEXT NOT NULL,               -- JSON
  links     TEXT NOT NULL DEFAULT '[]',  -- JSON array of {label, url}
  UNIQUE (output, id)
);
CREATE INDEX messages_by_output ON messages (output, seq);

CREATE TABLE connections (
  id       INTEGER PRIMARY KEY AUTOINCREMENT, -- never reused: a stale id acks nothing
  consumer TEXT NOT NULL,                -- always "<name>@<machine id>"
  output   TEXT NOT NULL,                -- an output's name, or '*' for a tap
  enabled  INTEGER NOT NULL DEFAULT 1,
  cursor   INTEGER NOT NULL,             -- last handled messages.seq
  created  TEXT NOT NULL,
  UNIQUE (consumer, output)
);

-- Migration 2.
CREATE TABLE requests (
  seq      INTEGER PRIMARY KEY REFERENCES messages(seq),
  deadline TEXT NOT NULL,                -- RFC 3339, by the clock of the bus's machine
  taken    TEXT,                         -- RFC 3339 when a consumer took it, else NULL
  reply    TEXT,                         -- the reply's data, JSON; NULL until replied
  replied  TEXT                          -- RFC 3339 when replied, else NULL
);
```

The bus id's row is made once, in the transaction that creates the
schema. `output` must be a declared output, or `*`; the crate checks it.

## Message envelope

CloudEvents-shaped, as stored in `messages`:

```json
{
  "id": "0196f1c2-7a3e-7c41-9b2d-5e8f0a1b2c3d",
  "type": "producer.thing.happened",
  "subject": "One line for listings",
  "time": "2026-10-31T09:30:00Z",
  "data": { "any": "JSON the type defines" },
  "links": [{ "label": "Source", "url": "https://…" }]
}
```

`data` is free per type. Producers document their types in their
output's `types`. Consumers ignore types they don't handle (and `ack`
them). As JSON, in a session's lines, only `type` and `data` are required:
without an `id`, the bus generates one, and without a `time`, it is now.

## Library API (Rust)

```rust
let bus = Bus::open_default()?;                    // or Bus::open(path)
let machine = bus.id();                            // this bus's id, a Uuid

// Every program declares its outputs at startup. Idempotent; updates
// description, types and last_seen.
bus.declare(&Output::new("events").describe("What this output carries")
    .types(["producer.thing.happened"]))?;

// Producer. Returns the seq, or the existing seq if (output, id) is already there.
// The id is optional: `Message::new` gives a fresh UUIDv7, `.id(uuid)` the producer's own.
// `time` is now unless `.time(…)` says when the event happened; `.link(label, url)` adds a link.
bus.publish("events", &Message::new("producer.thing.happened", data).id(uuid).subject(s))?;

// A consumer connects itself to what it reads: a bare name is this machine's.
bus.connect("inbox", "events", Start::Now)?;       // or Start::Beginning
bus.connect("hook", TAP, Start::Now)?;             // a tap, TAP being "*"

// Consumer: the next messages of each of its enabled connections, after each
// connection's cursor. Order: connections by id (creation order), then seq
// within each one. No fairness or priority between connections.
for d in bus.pull("inbox", 50)? {
    handle(&d.message)?;              // on error, stop: the next pull returns it again
    bus.ack(d.connection, d.seq)?;
}

// Request and reply. The asker: a request is published as `publish` does, with a
// deadline `timeout` from now by this machine's clock; the same id again is the
// same request. `outcome` reads what became of it: `Pending { deadline }`,
// `Replied(Reply { data, replied })`, or `TimedOut { taken }` once the deadline
// passed with no reply. The library does not wait: a session or the command does.
let seq = bus.request("asks", &Message::new("app.question", data).id(uuid), timeout)?;
bus.outcome(seq)?;

// The consumer that answers: take before the work, then reply, then ack.
match bus.take(d.seq)? {
    Take::Taken => { bus.reply(d.seq, answer)?; }
    Take::TakenBefore => { /* taken before a crash: dedupe on d.message.id */ }
    Take::Replied | Take::Expired => {}
}
bus.ack(d.connection, d.seq)?;

// Wiring and inspection (used by the CLI and therefore herdr-patch).
bus.disconnect(connection_id)?;
bus.set_enabled(connection_id, false)?;
bus.connections()?;                   // or bus.connection(id)
bus.outputs()?;                       // or bus.output(name)
bus.delete("events")?;                // returns the ids of the connections it removed
bus.messages(&MessageQuery { output: Some("events".into()), ..Default::default() })?;
bus.message(seq)?;
bus.positions("inbox")?;              // each enabled connection's cursor and newest seq, for serve

// A consumer that waits: a session with `herdr-bus serve` (Sessions), which the
// `herdr-bus-client` crate starts with `herdr-bus` from PATH, or with a login
// shell's PATH on another machine: over ssh for a machine of `hc machine ls`,
// or through a command the app gives that reaches the machine.
let client = Client::connect(BusLocation::Local, "inbox")?;
// or Client::connect(BusLocation::Machine("box".into()), "inbox")
// or Client::connect(BusLocation::Command(via), "inbox"), `via` a command that
// reaches the machine: ["fly", "ssh", "console", "-a", "app", "--pty=false", "-C"]
loop {
    for d in client.pull(50)? {       // waits while there is nothing
        handle(&d.message)?;
        client.ack(d.connection, d.seq)?;
    }
}
// Through the session, on its machine's bus: every method takes &self, and other
// threads call these while one waits in pull.
client.publish("events", &message)?;
match client.request("asks", &message, timeout)? {   // waits for the reply
    Outcome::Reply(data) => {}
    Outcome::Timeout { taken } => {}              // taken: false, nobody ever handles it
}
client.take(seq)?;
client.reply(seq, data)?;
client.close();                       // from any thread: every call returns an error
```

The library's public types: `Bus`, `Output` (what a program declares),
`Declared` (an output as the bus holds it), `Message`, `Link`,
`Stored { seq, output, published, message, request }`, `MessageQuery`,
`Delivery { connection, seq, output, message, request }`,
`Connection { id, consumer, output, enabled, cursor, lag, created }`,
`Position`, `Start`, `TAP`, `Seq`, `ConnectionId`,
`Request { deadline, taken, reply }` (a request as the bus holds it),
`Reply { data, replied }`, `RequestState { deadline, taken, replied }` (what
a delivery says of a request), `Take`, `Outcome`, `Error`, `Result`,
`Timestamp`, `Uuid` and `DB_VARIABLE`. Every `consumer` argument takes a
bare or full name, which the bus completes.

Rules:

- `ack(connection, seq)` moves the cursor to `seq` if it is higher; acks
  are idempotent. A `seq` that is not a message of the connection's output
  is refused: it would skip messages not yet delivered.
- `pull` never moves cursors by itself.
- `connect` on a connection that exists is a no-op: it keeps its cursor and
  `enabled`, ignores `Start`, and returns its id.
- A message `id` is a UUID, of any version; `publish` refuses anything else.
  It is optional: without one, the bus generates a UUIDv7. A producer that
  may retry a publish (a crash, a timeout) generates the id itself before
  the first attempt and reuses it; one with an ID of its own that is not a
  UUID derives one from it with UUIDv5.
- `publish` of an `(output, id)` already there succeeds and returns the
  existing seq; the CLI exits 0.
- `delete` marks the output deleted and removes the connections to it
  (`output = name`), in one write; taps stay. `publish`, `connect` and
  `delete` refuse a deleted output, and `outputs` leaves it out. `declare`
  brings it back.
- `request` publishes as `publish` does, with the same refusals, and writes
  the deadline in the same transaction. An id the output holds as a request
  returns its seq, and its first deadline stays; an id it holds as a plain
  message is refused. A timeout of 0 expires at once.
- `take(seq)` answers, in one write: `Taken`, the first take before the
  deadline; `TakenBefore`, taken already, as after a crash; `Replied`, it
  has its reply; `Expired`, past its deadline and never taken, which the
  consumer acks and skips. A deadline has passed once the clock reaches it.
- `reply(seq, data)` returns the reply that stands: the first, when there is
  one already. A request never taken is taken in the same write; one past its
  deadline and never taken is refused. A request taken is replied to even
  past its deadline, so that the same id asked again returns the reply.
- `outcome(seq)` is `TimedOut` only once the deadline has passed with no
  reply, read under the write lock, so that a take that read the clock
  before the deadline has committed: `TimedOut { taken: false }` is final.
- `take`, `reply` and `outcome` refuse a seq that is no request.
- Writes run in short `BEGIN IMMEDIATE` transactions.
- `herdr_bus_client::Client::pull` returns what `Bus::pull` would, and
  where that is nothing, waits for a notify and asks again. Once the
  session ends, every waiting and later call returns an error: the caller
  connects again, and the new session's first pull returns at once what
  waited, and a request asked again under its id is the same request.
- Every method of `Client` takes `&self`, and `Client` is `Send + Sync`: one
  thread waits in `pull` while others publish, ask, take, reply and ack, and
  several wait in `request` at once. One thread reads what `serve` sends and
  hands it on: answers in the order the lines were sent, a reply or timeout
  by its seq, a notify to the waiting pull. Another writes the lines, so
  that no call blocks on a link that stopped reading. `close()`, from any
  thread, ends the session.
- `Client::request` returns by its deadline whatever the link does: when
  `serve` has said nothing `GRACE` (2 s) past it, it returns `Error::Silent`,
  and the request may have been taken. An older `serve` answers a line it
  does not know with an error, which the client returns as it is.
- `Client::connect` with `Machine(name)` reads `hc machine ls --json`
  (`{"machines": [{"name", "target", "via", "state", …}]}`), and runs
  `ssh -o BatchMode=yes -o ControlMaster=no -o ConnectTimeout=15
  -o ServerAliveInterval=15 -o ServerAliveCountMax=3`
  to the machine's `target` (`host`, `user@host`, `user@host:port`,
  `[IPv6]:port`, or a Host of `~/.ssh/config`), which runs `sh -lc 'exec herdr-bus serve
  -- <consumer>'` there. ssh never prompts, gives up on a machine that does
  not answer within 15 s, and ends a link silent for 45 s. An unknown
  machine is refused, and so is one reached through a command (`via`),
  which `hc machine ls` does not show: the app gives that command as
  `Command`.
- `Client::connect` with `Command(argv)` runs `argv[0]` with the rest of
  `argv` and one last argument, the same `sh -lc 'exec herdr-bus serve --
  <consumer>'`, as the command of `hc machine add --via` takes it: the app
  gives how to reach the machine, never how to run `herdr-bus`. An empty
  `argv` is refused.
- For `Machine` and `Command`, a bare consumer name is completed with the
  id of this machine's bus (`HERDR_BUS_DB` honoured) before it goes. A
  session that ends, before `ready` or after, fails with where `serve` ran
  (the machine's name for a machine, the program for a command) and what
  the command wrote on stderr, at each call from then on. Lines that are
  not the session's before `ready`, as a login shell's profile prints,
  are skipped.

## CLI

Crate `herdr-bus-cli`, binary `herdr-bus`, apart from the library so that
an app linking the library builds none of it. Every command takes
`--json` (machine output, one JSON document on stdout) and `--db <path>`.
Errors: exit 1, `{"error": "…"}` on stdout in `--json` mode. A request
whose deadline passed with no reply: exit 2.

```
herdr-bus declare <output> [--description …] [--types a,b]
herdr-bus publish <output> --type … [--id UUID] [--subject …] [--time …] [--link LABEL=URL …]
                  [--data JSON | stdin]
herdr-bus request <output> --type … --timeout 5s [--id UUID] [--subject …] [--time …]
                  [--link LABEL=URL …] [--data JSON | stdin]
herdr-bus take <seq>
herdr-bus reply <seq> [--data JSON | stdin]
herdr-bus connect <consumer> <output>|'*' [--from now|beginning]
herdr-bus pull <consumer> [--limit N]
herdr-bus ack <connection> <seq>
herdr-bus serve <consumer>                   # a session, until stdin closes (Sessions)
herdr-bus disconnect <connection>
herdr-bus enable|disable <connection>
herdr-bus delete <output>
herdr-bus outputs                            # the outputs, with their connections
herdr-bus connections                        # with lag per connection
herdr-bus messages [--output …] [--connection …] [--before SEQ] [--limit N]
herdr-bus message <seq>
herdr-bus id                                 # this bus's id
herdr-bus skill                              # a SKILL.md that teaches an agent to use the bus
```

`pull --json` prints an array, in `pull`'s order, of
`{"connection": 3, "seq": 43, "output": "events", "message": {<envelope>}}`,
the envelope as in Message envelope (`id`, `type`, `subject`, `time`,
`data`, `links`); `[]` when there is nothing.
A delivery of a request also carries `"request": {"deadline": "…", "taken":
bool, "replied": bool}`; a plain message's has no `request` key.
`publish --json` prints `{"seq": 43, "id": "<uuid>"}`, the id given or generated.
`request` asks, then waits in its own process on the bus's machine, with no
`serve`, watching the file from before it publishes: it prints the reply's
data and exits 0, or, once the deadline passed with no reply, exits 2 and
says on stderr whether the request was taken. `--timeout` takes a duration
such as `500ms`, `5s` or `10m`. With `--json`: `{"seq", "id", "reply":
<data>}`, or `{"seq", "id", "timeout": {"taken": bool}}`. `take` prints the
state, `taken`, `taken_before`, `replied` or `expired`, all exit 0;
`--json`: `{"seq", "state"}`. `reply --json` prints `{"seq", "replied":
"<time>"}`, the time of the reply that stands.
`messages` and `message` print `{"seq", "output", "published",
"message"}`, and on a request `"request": {"deadline", "taken": "<time>" |
null, "reply": {"data", "replied"} | null}`; `declare` and `outputs` the
output: `name`, `description`,
`types`, `declared`, `last_seen`, and its `connections`, those on it by
name (a tap shows in `connections` only);
`connect`, `ack`, `enable`, `disable` and `connections` the connection:
`id`, `consumer` (in full), `output`, `enabled`, `cursor`, `lag` (messages
of its output past its cursor), `created`. `disconnect` prints `{"id"}`;
`delete` prints `{"deleted": "<output>", "disconnected": [<ids>]}`; `id`
prints `{"id": "<uuid>"}`, and without `--json` the UUID on a line.

`skill` prints a skill for agents (`crates/herdr-bus-cli/SKILL.md`): the
model, how to produce, consume, tap and wait, and the mistakes to avoid. It
names each command and leaves their options to `--help`, so the two never
disagree; a test checks that it names exactly the commands there are.

The binary must run on every machine whose bus a program reaches, with
`hc exec` or a session, and be on `PATH` there, a login shell's for a
session from another machine: the client crate runs `herdr-bus` by that
name. Install it with `cargo install --git … herdr-bus-cli` (README.md); a
program that ships its own build puts it on `PATH`. Such a program can embed
the command instead of shipping a second binary: the crate's lib has
`herdr_bus_cli::run(args) -> ExitCode`, the whole command, which the
binary only calls, `args` starting with the program's name; the program
calls it when it is invoked through a symbolic link named `herdr-bus` on
`PATH`, returns from `main` with its code, and ships one binary, with the
command at the revision it links.

The CLI is the way in for non-Rust programs (other languages, shell
scripts) and for herdr-patch, since a mod can't link a
Rust library.

## Sessions

`herdr-bus serve <consumer>` holds a session with one consumer: JSON lines
over its stdin and stdout, until stdin closes. It reads every enabled
connection of the consumer, those made during the session included. The
consumer runs it on the bus's machine, as a child process or through any
command that gives a pipe, such as `ssh box herdr-bus serve
inbox@<its machine id>` or the command of `hc machine add --via`. `hc exec` cannot carry one: it sends its input
first and returns the output at the end. A consumer that reads another
machine's bus gives its full name there, its own machine's id: a bare one
would name a consumer of that machine.

`serve` first sends `{"ready": {}}`. A name that is no consumer's is
refused before it: `serve` exits 1.

Consumer → `serve`:

- `{"get_messages": {"limit": 10}}`: at most `limit` messages, as `pull`
  returns them. A `limit` of 0 is refused.
- `{"ack": {"connection": 3, "seq": 43}}`: as `ack`.
- `{"publish": {"output": "events", "message": {…}}}`: as `publish`.
  `message` is the envelope, of which only `type` and `data` are required.
- `{"request": {"output": "asks", "message": {…}, "timeout_ms": 5000}}`: as
  `request`. Its outcome comes later, unasked.
- `{"take": {"seq": 44}}`: as `take`.
- `{"reply": {"to": 44, "data": …}}`: as `reply`.

`serve` → consumer:

- `{"notify": {}}`: the consumer has messages past those the session
  delivered. Sent once, until the consumer next sends `get_messages`.
- `{"messages": [<delivery>, …]}`: the answer to `get_messages`, deliveries
  in `pull --json`'s shape, `[]` when there are none.
- `{"acked": {"connection": 3, "seq": 43}}`: the answer to `ack`.
- `{"published": {"seq": 43, "id": "…"}}`: the answer to `publish`, the id
  given or generated.
- `{"requested": {"seq": 44, "id": "…"}}`: the answer to `request`, at once.
- `{"reply": {"to": 44, "id": "…", "data": …}}` or `{"timeout": {"to": 44,
  "id": "…", "taken": false}}`: later, exactly one of the two for each
  `request` line the session answered, unasked. `timeout` comes once the
  deadline has passed with no reply, by `Bus::outcome`: `taken: false`
  guarantees that the request is never handled.
- `{"taken": {"seq": 44, "state": "taken"}}`: the answer to `take`, the
  state one of `taken`, `taken_before`, `replied` and `expired`.
- `{"replied": {"to": 44}}`: the answer to `reply`.
- `{"error": "…"}`: the answer to a line `serve` cannot carry out, or does
  not know; the session goes on. An ack of a seq past the
  highest the session delivered on that connection is refused: the consumer
  cannot have handled it.

A consumer with no connection, or whose connections went with their
output, has no messages to give: `[]`. An ack on a connection removed since
its messages were delivered is `acked`: it has nothing left to move.

Each line the consumer sends gets exactly one answer, in the order sent.
Notifies, and the `reply` or `timeout` of each request, come in between.
Several requests wait at once on one session, and their replies come in any
order. `serve` sends no message unasked: the consumer's `limit` is its
backpressure. Cursors and requests stay in the file: a session that breaks
off loses nothing. What the consumer did not acknowledge comes back in the
next, and a request asked there again under its id is the same request.

A program that only publishes and asks, and reads no output, holds its
session under a consumer name with no connection: `serve` checks the
name's shape alone, and such a consumer has no messages. Lines have no cap
on their size: requests and replies reach a few hundred KB.

`serve` learns of writes by watching the file's folder: every write
modifies the `-wal` file, and no read does. The modification comes before
the write can be read, so it looks at the file 30 ms after it, and once a
second without one, as a backstop. Where it cannot watch the folder, it
says so on stderr and looks once a second. It also wakes at the nearest
deadline of the requests it waits on, and looks for their replies where it
looks for new messages. `herdr-bus request` waits the same way.

## Shared types

A message's `type` and `data` are its producer's: the bus knows no type.
A type that several programs publish or read is defined once, in a small
crate of its own beside the bus, which the bus crate does not depend on.

- `herdr.words` (`crates/herdr-words`): words someone wrote to an actor.
  `data`: `author`, `"user"` for the person at the keyboard; `to`, the
  actor written to, `pane:<the pane's ID as Herdr gives it>` for an agent
  in a pane, or `null`; `text`, the words as written.

## Retention

History is kept forever in v1: nothing deletes messages, requests and
their replies, or connections' past, a deleted output's messages included. Pruning is
added later if the file grows enough to
matter; when it is, a message must only go once every connection that
reads it, taps included, has a cursor past it.

## herdr-patch (mod)

A herdr-app mod in `~/.config/herdr-app/mods/herdr-patch/`, opened as a
tab or pane (`tab.new {mod: "herdr-patch"}`).

- **Data:** everything through `exec("herdr-bus", [..., "--json"])`. Live
  refresh with `watchFolder` on the database's folder, debounced; no fast
  polling. A mod's `exec` returns its output at the end, so it cannot hold
  a session. Every write modifies the `-wal` file; reads, the mod's own
  included, create, delete and close it and the `-shm` file but never
  modify the `-wal` file, so only that change means new data. It comes
  before the write is readable: refresh once the folder has been quiet
  for some tens of milliseconds after it.
- **Wiring view (v1):** outputs in a left column, consumers in a right
  column (those `connections` names, grouped by machine id, this bus's
  first); connections listed between them with lag (the messages of its
  output past its cursor) and an enabled switch, taps from a `*` row atop
  the outputs. Connect: pick a consumer, or type a new name, then an output
  or `*`, then "from now" or "from the beginning". Disconnect from a
  connection's menu; delete an output from its own.
- **Inspection:** clicking an output lists its messages (newest first,
  paged with `--before`); clicking a connection shows its messages with
  the cursor marked. A message opens
  as its envelope with `data` pretty-printed and its links clickable.
- **Stale outputs:** outputs whose `last_seen` is old are dimmed, not
  hidden.
- `herdr-app check` passes before calling it done.

Later, not v1: a canvas with draggable nodes and wires (`paint_path`).

## Out of scope for v1

- Filters on connections (producers decide what goes on an output).
- Remote machines in the bus (one local SQLite file per machine). A
  program writes another machine's bus by running the CLI there, through
  `hc exec --on <machine> herdr-bus …`, or through a session there, and
  reads it, and asks there, through a session there, over ssh or a command
  that reaches the machine, as `<name>@<its own machine id>`. The bus checks a consumer's
  name only for its shape: it never asks whether that machine exists.
- `pull --wait`, or a `wait` command: a consumer that waits holds a
  session.
- Identity in requests: the bus carries no sender. A program that must
  know who asks signs its requests itself.
- Replies as messages, or delivered to taps; cancelling a request; and
  telling a consumer that is down from a slow one: both are a timeout.
- Access control between programs, and registering consumers: a
  consumer is known by its connections alone.
- Deleting history (no `prune`; see Retention).
- Failure handling in the bus: no `fail`, `retry`, `drop`, dead-letter
  list or delivery limit. A message that keeps crashing its consumer
  blocks its connection, which shows as growing lag in herdr-patch.
- Message schemas beyond the `types` list.

## Repository

Public repo `github.com/flupke/herdr-bus`, a Cargo workspace:

```
crates/herdr-bus/        the library
crates/herdr-bus-client/ the client of a session, and the session's lines
crates/herdr-bus-cli/    the `herdr-bus` CLI, `serve` included
crates/herdr-words/      the `herdr.words` type (Shared types)
mods/herdr-patch/        the mod, installed into ~/.config/herdr-app/mods/
SPEC.md                  this file
```

herdr-bus is meant to be plugged into other mods' backends; those live
in their own repos and depend on the crate or call the CLI.
