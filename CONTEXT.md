# herdr-bus

A message bus the programs on one machine share, in one SQLite file, and the message types they
share over it. One line per term: what it is, then the synonyms never to use.

- **Bus**: one machine's SQLite file (`bus.db`), with an id that is its consumers' machine id; every program opens it itself. Never: broker, queue, server.
- **Output**: a named place messages are published on, global to the bus. Never: endpoint, channel, topic, source.
- **Deleted output**: an output someone deleted, its row and messages kept for the taps until it is declared again. Never: tombstone (the mechanism), undeclared output.
- **Consumer**: a reader of the bus, named `<name>@<machine id>`, known by its connections alone. Never: input, inbox (as the concept; a consumer may be named so), endpoint, subscriber, owner, address.
- **Message**: an immutable entry of the log, published on one output, with its envelope and its `seq`. Never: event, record.
- **Connection**: one consumer's cursor on one output, which a disabled connection does not deliver. Never: subscription, route, wire.
- **Tap**: a connection to every output, present and future: output `*`. Never: wildcard subscription, firehose.
- **Pull**: reading a consumer's messages past its enabled connections' cursors, moving none. Never: poll, fetch, receive.
- **Ack**: acknowledging a message on a connection, which moves its cursor past it. Never: commit, confirm.
- **Session**: a consumer's link to `herdr-bus serve <consumer>`, one JSON line each way; what the consumer sends is a line, and what `serve` sends back an answer. Never: subscription, connection (a consumer's cursor), stream, request (for a line).
- **Request**: a message published with a deadline, by the clock of the bus's machine, which takes one reply. Never: query, RPC.
- **Reply**: the one answer to a request, free JSON kept with it, carried by no output; the program that replies is the answerer. Never: response, result.
- **Take**: what a consumer does to a request before it handles it, in one write that says whether it is the consumer's to handle. Never: claim, lock, lease.
- **Shared type**: a message type several programs share, defined in a crate of its own that the bus does not know: `herdr.words`. Never: schema, bus type.
