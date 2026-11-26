# herdr-bus

A message bus for the programs on one machine, in one SQLite file, with no daemon: herdr mods'
backends, agents' tools, scripts. A program declares its outputs and publishes messages on them.
A consumer, named `<name>@<machine id>` or by its bare name on its own machine, connects to the
outputs it reads: each connection holds a cursor, the consumer acknowledges each message it
handled, and one it did not comes back on its next pull, after a crash too. A tap reads every
output, present and future. A request is a message with a deadline that takes one reply, which
its asker waits for. [SPEC.md](SPEC.md) holds the model, the schema, the library and the
command.

## Crates

- `herdr-bus`: the library.
- `herdr-bus-client`: the client of a session (below), for a Rust consumer that waits.
- `herdr-bus-cli`: the `herdr-bus` command, which gives every operation of the library, with
  `--json`, to programs that cannot link Rust, and holds sessions (`herdr-bus serve`). An app
  can ship it inside its own binary instead: invoked through a symbolic link named `herdr-bus`
  on `PATH`, it calls `herdr_bus_cli::run(std::env::args_os())`.
- `herdr-words`: the `herdr.words` message type, words someone wrote to an actor, for the
  programs that publish and read it to share one definition.

## The command

Install it from git, on each machine whose bus a program reaches, since a program on another
machine runs the binary there:

```sh
cargo install --locked --git https://github.com/flupke/herdr-bus herdr-bus-cli
```

`herdr-bus skill` prints a skill that teaches an agent to use the bus, for Claude Code for one:

```sh
mkdir -p ~/.claude/skills/herdr-bus && herdr-bus skill > ~/.claude/skills/herdr-bus/SKILL.md
```

The client crate runs it from `PATH`, a login shell's on another machine.

```sh
herdr-bus declare events
herdr-bus connect inbox events           # inbox, a consumer of this machine
echo '{"n": 1}' | herdr-bus publish events --type producer.thing.happened --json
                                         # {"seq": 1, "id": "<a fresh UUIDv7>"}
herdr-bus pull inbox --json              # [{"connection": 1, "seq": 1, "output": "events", …}]
herdr-bus ack 1 1
```

A program that needs an answer asks a request, and the consumer that answers takes it before
it works on it, then replies:

```sh
herdr-bus request asks --type app.question --timeout 5s --data '{"q": 1}'
                                         # waits; prints the reply's data, or exits 2 once
                                         # the deadline passed, saying whether it was taken
herdr-bus take 2                         # taken, taken_before, replied or expired
herdr-bus reply 2 --data '{"a": 2}'
```

A message's id is a UUID. A producer that may retry a publish gives its own with `--id`,
generated before the first attempt, so that the retry adds nothing; one whose own ids are not
UUIDs derives them with UUIDv5.

The bus file is `$HERDR_BUS_DB`, else `$XDG_DATA_HOME/herdr-bus/bus.db`, else
`~/.local/share/herdr-bus/bus.db`; `--db` names another. Each machine has its own, with an id,
which `herdr-bus id` prints: a program writes on its own machine's bus, and reads another
machine's through a session there, as `<name>@<its own machine's id>`.

A consumer that waits for its messages holds a session: it runs `herdr-bus serve <consumer>`,
through `ssh` or any command that gives a pipe for another machine's bus, and speaks to it in
JSON lines. `serve` notifies it when it has messages, sends them when it asks, at most as many
as it asks for, and takes its acknowledgements. Through a session, a program also publishes,
asks, takes and replies on that machine's bus ([SPEC.md](SPEC.md), Sessions).

`herdr-bus delete` deletes an output and its connections; its messages and the taps stay, and
declaring it again brings it back.

## Using it from an app

No crate is published. An app, such as herdr-bowtie or herdr-ruminate, takes them as `git`
dependencies, its `Cargo.lock` pinning the revision (`cargo update -p herdr-bus` moves it), so
that a clone of the app alone builds:

```toml
[dependencies]
herdr-bus = { git = "https://github.com/flupke/herdr-bus" }
herdr-words = { git = "https://github.com/flupke/herdr-bus" }
# A consumer that waits for its messages:
herdr-bus-client = { git = "https://github.com/flupke/herdr-bus" }
```

A consumer that waits pulls through `herdr_bus_client::Client`, whose `pull` waits while there
is nothing:

```rust
let client = Client::connect(BusLocation::Local, "inbox")?;   // or Machine("box".into()), or Command(…)
loop {
    for delivery in client.pull(50)? {
        handle(&delivery.message)?;
        client.ack(delivery.connection, delivery.seq)?;
    }
}
```

It runs `herdr-bus serve` from `PATH`; for `Machine`, a machine of `hc machine ls`, it runs it
over `ssh` from a login shell's `PATH` there, as this machine's consumer. A machine that hc
reaches through a command of its own (`hc machine add --via`) is reached with `Command`, given
that command: the client appends the line to run there as its last argument. Every method takes
`&self`, so that threads share one session: one waits in `pull` while others `publish`, `ack`,
`take` and `reply`, and several wait in `request` at once. Once the session ends, or `close()`
ends it from any thread, every call returns an error, and the app connects again: nothing is
lost.

The bus does no cross-machine work: a program reaches another machine's bus through a session
there, or by running the command there, such as through `hc exec --on <machine> herdr-bus …`.

`herdr_bus::Timestamp`, a message's `time`, is `jiff`'s, re-exported. The bus links SQLite
through `rusqlite` 0.38 with its `bundled` feature. An app that links SQLite through `rusqlite`
too must take the same version: two versions of it in one build fail to link.

## License

MIT: see [LICENSE](LICENSE).
