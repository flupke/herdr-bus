# A consumer's name ends with the id of its machine's bus

A program may run on several machines and read one machine's bus from each: two computers that
both read a worker machine's requests, say. Under one name they are one consumer, with one
cursor: each message is handled twice, or by whichever pulled first. The bus cannot tell two
readers under one name from one reader that restarted.

A consumer's name is `<name>@<machine id>`, the machine id being the id of the bus of the
machine the consumer runs on: a random UUID made with the bus file, which `herdr-bus id`
prints. A bare `<name>` is a consumer of the machine whose bus is asked: the bus completes it
with its own id, and always stores and compares the full name. The Rust client completes a bare
name with the id of its own machine's bus before it reaches another machine. The bus checks the
shape of a name, never whether the machine exists.

Considered:

- Free names, each app adding what tells its machines apart, such as the hostname. They read
  well in a listing, but each app invents its own scheme, and one that forgets shares a cursor
  without an error.
- The machine id alone as the name. Every program of a machine would be one consumer, and a
  listing would not say who reads.
- `hc`'s machine names. They are nicknames local to the machine that runs `hc`, which a rename
  changes, and `hc` does not list the machine it runs on.
