# A consumer is known by its connections alone

The first design had endpoints: a program registered its outputs and its inputs, each at an
address `<owner>:<name>`, and a connection fed an output to an input. Every consumer planned for
the bus had exactly one input, used as its mailbox, and the client of a session needed the
owner of its inputs to know what to read.

The bus has outputs, consumers and connections, and no inputs. A program declares an output
under a plain name, global to the bus, which several programs may publish on. A consumer is a
name. A connection is one consumer's cursor on one output, or on every output for a tap:
connecting is how a consumer subscribes, and nothing registers a consumer. A consumer pulls,
and holds a session, under its name, and gets the messages of all its connections; each
delivery names the output it came from.

Considered: keeping inputs, so that a consumer has several named places to read from, each with
connections of its own. It kept the address, the kind of an endpoint and the owner in the model
and in every command, for a need no consumer has: one that reads several outputs tells their
messages apart by the output and the type each delivery carries.
