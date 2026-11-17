---
name: herdr-bus
description: Pass messages between programs through herdr-bus, the message bus each machine keeps. Use to publish on an output, connect a consumer to an output, pull and acknowledge a consumer's messages, ask a request and wait for its reply, answer requests, tap every output, read another machine's bus, or look at what a bus holds.
---

# herdr-bus

herdr-bus is a message bus in one SQLite file per machine, with no daemon: every program opens
the file itself, through the `herdr-bus` command or the Rust crate. This page says how to use it
well. Each command's options are in its own help, `herdr-bus <command> --help`: read it before
you run a command for the first time.

## The model

- An **output** is a named place messages are published on, such as `reviewer-comments`. Its
  name is the bus's, whoever publishes on it: several programs may.
- A **message** is an envelope, `id`, `type`, `subject`, `time`, `data` and `links`, appended
  for good to one output, at a **seq**: its place in the log, which grows across every output.
  Its producer defines its `type` and `data`; the bus knows no type.
- A **consumer** is a reader, named `<name>@<machine id>`: the machine id is the id of the bus
  of the machine it runs on, which `herdr-bus id` prints there, on the machine's own bus: with
  `--db` or `HERDR_BUS_DB` naming another file, it prints that file's id. On its own machine, its bare `<name>`
  is enough: the bus completes it with its own id.
- A **connection** is one consumer's cursor on one output: the last message it acknowledged
  there. Connecting is how a consumer subscribes. A **tap** is a connection to every output,
  present and future: its output is `*`.
- A **request** is a message with a deadline, which takes one **reply**: free JSON, kept with
  the request in the bus it was asked on. A consumer **takes** a request before it handles it.

Producers don't know their consumers, and a consumer with no connection simply has no messages.

## Running it

- The command is `herdr-bus`, on `PATH`. To write on another machine's bus, run it there:
  `hc exec --on <machine> herdr-bus …`.
- Read its output with `--json`: one JSON document on stdout, the record the command acted on,
  or an array of them for a list. Any error exits 1, with `{"error": "…"}` on stdout.
- The bus file is the machine's, shared by the user's programs. Try things, and test, on a file
  of your own in a temporary folder, with `--db <path>` or `HERDR_BUS_DB`.
- Each machine has its own bus. A program writes on its own machine's bus; one that reads
  another machine's holds a session there (Reading another machine's bus).

## Producing

1. Declare your output each time your program starts, with `herdr-bus declare`. Declaring
   again keeps the output and its connections, but replaces its description and types: give
   the same ones each time. The types tell readers what the output carries: the bus does not
   check them.
2. Publish each message with `herdr-bus publish`: its `type`, and its `data` as JSON, in an
   option or on stdin. It prints the message's seq and id.

A message's id is a UUID. When you give none, the bus gives a fresh one. A program that may retry a
publish, after a crash or a timeout, makes the id itself before the first attempt and passes the
same one each time: a publish of an id already on the output adds nothing, keeps the message
that is there, and prints its seq. A program whose own ids are not UUIDs derives each one from
its own id with UUIDv5, in a namespace of its own that never changes, so that the same thing
always gets the same id.

## Consuming

1. Pick your consumer's name, and connect it to each output it reads with `herdr-bus connect
   <consumer> <output>`: from now by default, or from the beginning of the output's history.
   Connecting again changes nothing: the connection keeps its cursor. The output must be
   declared already: its producer declares it, never you, since a declaration replaces its
   description and types. When yours starts first, connecting fails: try again later.
2. Pull with `herdr-bus pull <consumer>`: a batch of the messages past each of its connections'
   cursors, connections in the order they were made, then by seq. Each delivery is
   `{"connection": 1, "seq": 43, "output": "…", "message": {"id", "type", "subject", "time",
   "data", "links"}}`. A pull moves no cursor.
3. Handle each message in that order, then acknowledge it with `herdr-bus ack <connection>
   <seq>`, the two numbers its delivery carries. An ack covers every earlier message of that
   connection too. Pull again until a pull returns nothing.

A program that runs on and waits for more messages holds a session instead, which tells it when
to ask (Waiting for messages).

A message you do not acknowledge comes back on the next pull, after a crash too: delivery is at
least once, so handle a message twice without harm, keyed on its id. The bus has no failure
handling. A message you cannot handle is yours to decide: acknowledge it to skip it, or stop
there and leave it for the next pull. A message whose type you don't handle, acknowledge.

Never acknowledge a message before you handled it: the bus refuses a seq the connection does not
read, but not a seq you skipped.

## Asking and answering

A program that needs an answer asks a request with `herdr-bus request <output>`: a message as
`herdr-bus publish` takes it, and a timeout. It waits, then prints the reply's data and exits 0.
When the deadline passes first, it exits 2, apart from errors (exit 1), and says whether a
consumer took the request:

- not taken: nobody will ever handle it. The bus judges the deadline by its own clock, and a
  request past it that nobody took can no longer be taken. To try again, ask a new request,
  under a new id: the same id gives the same timeout at once.
- taken: a consumer took it, and may still be handling it. Check its effect before you ask
  again.

With `--json`, it prints `{"seq": 44, "id": "…", "reply": <the reply's data>}`, or, with exit
2, `{"seq": 44, "id": "…", "timeout": {"taken": false}}`, on stdout either way.

The request is matched to its reply by its seq; you never name an output for the reply. Give
the request an id of your own, made before the first attempt, when you may ask again
after a crash or an error: asking again under the same id is the same request, with its first
deadline, its data and its reply, which comes at once when it has one. An id names one
question: a different question gets a new id.

A program that answers is a consumer connected to the output requests come on, and the only
program that answers the requests of that output: the bus does not record who took a request,
so a second answering consumer would see each request the first took as `taken_before`. A
delivery of a request carries `"request": {"deadline", "taken", "replied"}`; a plain message's
has no `request`. For each request, in this order:

1. Take it with `herdr-bus take <seq>`, before any work. It prints one of (`--json`:
   `{"seq": 44, "state": "taken"}`):
   - `taken`: it is yours. Handle it.
   - `taken_before`: you took it already, before a crash. Look up the message's id in your own
     records: handle it only if you did not finish it.
   - `replied`: it has its reply. Nothing to do.
   - `expired`: its asker gave up, and nobody may handle it. Skip it.
2. Reply with `herdr-bus reply <seq>`, the data as JSON (`--json`: `{"seq": 44, "replied":
   "<time>"}`). The first reply stands: a second one changes nothing. A reply to a request you
   took is kept past its deadline, so that its asker gets it when it asks again; a reply to an
   expired request is refused.
3. Acknowledge it, as any message: whatever `take` said.

A reply is no message: no output carries it, and no tap sees it.

## Hooking into everyone's messages

A tap reads every output, one declared after it included, with a cursor of its own: `herdr-bus
connect <consumer> '*'`, from now or from the beginning of the log as any connection. It reads a
copy: what it acknowledges changes no other connection. Each delivery names its `output`.

## Waiting for messages

A program that waits for its messages holds a session: it runs `herdr-bus serve <consumer>`, and
speaks to it in JSON lines, one line on its stdin, one line back on its stdout. A Rust program
has a client for it in the `herdr-bus-client` crate.

- `serve` first sends `{"ready": {}}`.
- It sends `{"notify": {}}` when the consumer has messages the session has not delivered yet,
  right after `ready` when some wait already, once until you next ask for them. It sends no
  message unasked.
- Ask with `{"get_messages": {"limit": 10}}`, the `limit` required: the answer is
  `{"messages": [ … ]}`, as a pull returns them: from the cursors, so the messages you have
  not acknowledged come again. Handle and acknowledge each batch before you ask again.
- On a notify, ask until the answer is `[]`, then wait for the next notify. A notify may already
  wait for a batch you drained: its ask returns `[]`, which costs nothing.
- Acknowledge each one you handled with `{"ack": {"connection": 1, "seq": 43}}`: the answer is
  `{"acked": {"connection": 1, "seq": 43}}`. An ack of a message the session has not delivered
  is refused.
- Every line you send gets exactly one answer, in the order sent; notifies, and the replies and
  timeouts of your requests, come in between. A line `serve` cannot carry out gets
  `{"error": "…"}`, and the session goes on.
- Close its stdin to end the session. Messages you did not acknowledge come back in the next.

A session also writes on its machine's bus, which is how a program on another machine asks
there. A program that only publishes and asks holds it under a consumer name with no
connection. `message` is an envelope, of which only `type` and `data` are required:

- `{"publish": {"output": "events", "message": {"type": "…", "data": …}}}`: the answer is
  `{"published": {"seq": 43, "id": "…"}}`.
- `{"request": {"output": "asks", "message": {…}, "timeout_ms": 5000}}`: the answer,
  `{"requested": {"seq": 44, "id": "…"}}`, comes at once. Later comes exactly one of
  `{"reply": {"to": 44, "id": "…", "data": …}}` and `{"timeout": {"to": 44, "id": "…",
  "taken": false}}`, its `taken` as `herdr-bus request` says it. Several requests wait at
  once, and their replies come in any order: match each by `to`.
- `{"take": {"seq": 44}}`: the answer is `{"taken": {"seq": 44, "state": "taken"}}`, the state
  one of the four `herdr-bus take` prints.
- `{"reply": {"to": 44, "data": …}}`: the answer is `{"replied": {"to": 44}}`.

Keep the session open until each request has its reply or timeout: a session that ends sends
no more of them. It loses no request, though: connect again, and ask again under the same id.

## Reading another machine's bus

Run the session on that machine, through anything that gives a pipe, such as `ssh`. Give your
consumer's full name there, `<name>@<your own machine id>`, the id `herdr-bus id` prints on
your machine: a bare name would be completed with the other machine's id, and name another
consumer. Connect that full name to the outputs you read there, with `herdr-bus connect` run on
that machine. The Rust client completes a bare name with your machine's id itself.

To ask a program on another machine, ask through a session there, with `request` lines: the
request and its reply stay in that machine's bus, and the session brings the reply back.

## Looking at the bus

`herdr-bus outputs` lists the outputs with their connections (a tap shows only in `herdr-bus
connections`), `herdr-bus connections` each connection with its consumer, its cursor and its
lag (the messages of its output past its cursor), `herdr-bus messages` the messages, newest
first, and `herdr-bus message <seq>` one of them. A connection whose lag keeps growing has a
consumer that stopped. A consumer that never gets a message may have no connection, under a
misspelt name for one: a name the bus has never seen is no error, so look for yours in
`herdr-bus connections`. `herdr-bus id` prints this bus's id.

`herdr-bus disconnect`, `herdr-bus enable` and `herdr-bus disable` change a connection: other
programs rely on theirs, so change one only when you made it or were asked to. The log keeps
every message: nothing deletes one.

`herdr-bus delete` deletes an output that is no longer used, such as one of a program that was
uninstalled, and the connections to it; the taps stay. Its messages stay, for the taps that
have yet to read them and for history: declaring it again brings it back, with its messages and
without its connections. As at every declare, give its description and types again.

`herdr-bus skill` prints this page.
