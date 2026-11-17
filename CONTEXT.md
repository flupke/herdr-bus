# herdr-bus

A message bus the programs on one machine share, in one SQLite file, and the message types they
share over it.

## Language

**Bus**:
One machine's SQLite file (`bus.db`) and the crate that opens it, with an id, a random UUID made
with the file: the machine id of that machine's consumers. Every program opens it itself: there is
no daemon. Another machine's bus is reached by running the command there, or through a session
there.
_Avoid_: broker, queue, server

**Output**:
A named place messages are published on, declared on the bus. Its name is global to the bus:
several programs may publish on one output.
_Avoid_: endpoint, channel, topic, source

**Deleted output**:
An output someone deleted: its connections are gone, taps stay, and its row stays, marked
deleted, with its messages, until it is declared again.
_Avoid_: tombstone (the mechanism, not the concept), undeclared output

**Consumer**:
A reader of the bus, named `<name>@<machine id>`, the id of the bus of the machine it runs on. A
bare `<name>` is a consumer of the machine whose bus is asked: the bus completes it, and always
stores and compares the full name. A consumer is known by its connections alone.
_Avoid_: input, inbox (a consumer may be named so, but the concept is the consumer), endpoint,
subscriber, owner (a consumer belongs to no program prefix), address (a consumer has a name)

**Message**:
An immutable entry of the log, published on one output: its envelope (`id`, `type`, `subject`,
`time`, `data`, `links`) and its `seq`, its place in the log.
_Avoid_: event, record

**Connection**:
One consumer's cursor on one output: the last message the consumer acknowledged there.
Connecting is how a consumer subscribes. A disabled one delivers nothing.
_Avoid_: subscription, route, wire

**Tap**:
A connection to every output, present and future: output `*`, one cursor on the log's `seq`.
_Avoid_: wildcard subscription, firehose

**Pull**:
Reading a consumer's messages past each of its enabled connections' cursors. It moves no cursor.
_Avoid_: poll, fetch, receive

**Ack**:
Acknowledging a message on a connection, which moves its cursor there: the message, and those
before it, never come back.
_Avoid_: commit, confirm

**Session**:
A consumer's link to `herdr-bus serve <consumer>`, one JSON line each way over the command's stdin
and stdout, on the bus's machine or through a pipe to it: `serve` notifies the consumer when it has
messages, sends them only when asked, and takes its acks. Through it, a program also publishes,
asks, takes and replies on that bus. A line the consumer sends is a line, never a request.
_Avoid_: subscription, connection (a connection is a consumer's cursor on an output), stream

**Request**:
A message published with a deadline, by the clock of the bus's machine, which takes one reply. It
is pulled, acknowledged and tapped as any message. Asked again under its id, it is the same
request.
_Avoid_: query, RPC

**Reply**:
The one answer to a request: free JSON, kept with the request in the bus it was asked on, and
matched to it by the request's seq. It is no message: no output carries it. The first stands.
_Avoid_: response, result. An answer is what `serve` sends back for a line; the program that
replies to requests is their answerer.

**Take**:
What a consumer does to a request before it handles it, in one write: it says whether the request
is the consumer's to handle, was taken already, has its reply, or expired, past its deadline with
nobody having taken it. An expired request can never be taken. The bus does not record who took
it: one program answers an output's requests.
_Avoid_: claim, lock, lease

**Shared type**:
A message type several programs publish or read, defined once in a crate of its own beside the
bus, which the bus does not know: `herdr.words`.
_Avoid_: schema, bus type
