# Checking from a shell

A change to the bus or its command is checked as its user would check it: with the command, from
a shell, on a private bus file. Build it with `scripts/dev-shell cargo build -p herdr-bus-cli`, then point every
command at a file of a temporary folder, never the user's:

```sh
export HERDR_BUS_DB="$(mktemp -d)/bus.db"
b=target/debug/herdr-bus
```

The checks the user runs, each with what it must show:

1. A round trip: `$b declare out`, `$b connect c out`, then `echo '{"n":1}' | $b publish out
   --type t --json` prints its seq and id, `$b pull c --json` shows it with `"output":"out"`,
   `$b ack <connection> <seq>`, and the next pull prints `[]`. `$b connections` shows the
   consumer as `c@<the id $b id prints>`, and `$b pull "c@$($b id)"` reads the same.
2. A tap: `$b connect hook '*'`, then declare another output and publish on both: `$b pull
   hook` shows both messages, each with its output, and acking one there leaves the other
   connections' pulls as they were.
3. A crash: a consumer that pulls and is killed before its ack,
   `bash -c "$b pull c; kill -9 \$\$"`: the next pull shows the message again; once acked, it
   never comes back.
4. Two programs at once: two background loops of `publish` with the same ids (`--id`, UUIDs
   made once beforehand, such as with `uuidgen`), and a loop of `pull --json` and `ack` of its
   last delivery, until the publishers are done and a pull is empty. Every id arrives once:
   `sort | uniq -d` of the pulled ids prints nothing, and their count is the number of ids.
5. An app that takes the crates by git: a new crate in a temporary folder whose `Cargo.toml`
   takes `herdr-bus` and `herdr-words` by `git` (a `file://` URL and the change's bookmark
   before it is pushed), and `rusqlite` 0.38 `bundled` as herdr-bowtie does, builds and runs
   a round trip with `cargo run`, and builds no `clap`. Cargo's own git cannot open a
   clone with `extensions.relativeworktrees` set, as `git worktree add` with relative paths
   sets it: fetch with `CARGO_NET_GIT_FETCH_WITH_CLI=true`.
6. A session: `$b serve c` in a terminal prints `{"ready":{}}`, then `{"notify":{}}` once `c`
   has messages; typing `{"get_messages":{"limit":10}}` prints them, and an `{"ack":…}` of one
   prints `{"acked":…}`. A publish from another shell prints one notify, however many follow
   before the next `get_messages`. `$b serve c@box` exits 1 before `ready`. The same through
   `ssh localhost`, with the full name `c@<this bus's id>`.
7. Two machines on one output: `$b connect c out` and `$b connect "c@<another UUID>" out`
   make two connections, with cursors of their own: acking on one leaves the other's pull as
   it was.
8. An app that embeds the command: a new crate in a temporary folder that takes `herdr-bus-cli`
   by `git`, as in check 5, and whose `main` calls `herdr_bus_cli::run(std::env::args_os())`
   when the file name of its `argv[0]` is `herdr-bus`. Through a link of that name, each of
   `--help`, `--version`, a round trip with `--json` and without, a usage error with
   `--json` and without, and `serve`, prints what `$b` prints, on the same stream, and exits
   with the same code. Run the same script of commands with each, on a bus file of its own,
   ids and times masked with `sed`, and `diff` the outputs.
9. A deletion: `$b delete out` prints the connections it removed; `$b outputs` no longer
   shows it, `$b publish out …` fails, a tap still pulls its messages, and `$b declare out`
   brings it back, with its messages and without its connections.

10. Ask and answer: `$b declare asks` and `$b connect answerer asks`, then `$b request asks
    --type t --timeout 10s --data '{}'` blocks in one terminal. In another, `$b pull answerer
    --json` shows it with `"request": {"deadline": …, "taken": false, "replied": false}`; `$b
    take <seq>` prints `taken`, and `$b reply <seq> --data '{"ok":true}'` makes the first
    terminal print `{"ok":true}` and exit 0. The same `--id` asked again prints it at once, a
    second `reply` leaves it, and `take` prints `replied`.
11. Nobody answers: `$b request asks --type t --timeout 2s --data '{}'` with no consumer exits
    2 after two seconds and says `not taken` on stderr; `$b take <seq>` then prints `expired`,
    and `$b reply <seq>` fails. A request taken and never replied exits 2 and says `taken`. A
    consumer killed after its `take` and before its `ack` pulls the request again, marked
    `"taken": true`, and `take` prints `taken_before`.
12. A session asks and answers: in `$b serve asker`, typed `{"request": {"output": "asks",
    "message": {"type": "t", "data": 1}, "timeout_ms": 60000}}` lines, three of them, each get
    `requested` at once; `take` and `reply` lines in `$b serve answerer`, in any order, bring
    each `reply` line to the asker's session as it is given, naming its request by `to`, while
    a `get_messages` there is still answered. A `publish` line puts a message on the bus. A
    `timeout_ms` of 0 brings `timeout` with `"taken": false`. The same through `ssh localhost`.
13. An older bus file: a file made by the fixed point's binary (`cargo install … --rev <fixed
    point>`, as below), with messages, opens with the change's binary, which migrates it, and
    keeps its messages.

A change that must leave the command's output as it was runs that script with a binary of the
fixed point too (`cargo install --git file://… --rev <fixed point> --root <folder>
herdr-bus-cli`), and `diff`s it with the change's.

## Watching the file

`herdr-bus serve` learns of new messages by watching the bus file's folder, and herdr-patch
refreshes the same way; its 30 ms wait and 1 s backstop come from what follows. Seen with inotify
from a second process:

- Every write modifies `bus.db-wal` (`IN_MODIFY`), whether another program holds the file open
  or not.
- No read modifies `bus.db-wal`. A read by the only program that has the file open creates
  `bus.db-wal` and `bus.db-shm`, modifies `bus.db-shm`, and deletes both when it closes; with
  another program holding the file, a read still closes all three files after a write-open
  (`IN_CLOSE_WRITE`).

So a watcher reacts to `IN_MODIFY` of `bus.db-wal` alone: on any event, a consumer that pulls
through the command would wake itself with its own pull. Watch the folder, not the file, which
is deleted whenever the last program closes the bus.

The modification comes before the write is readable: SQLite writes the commit frame, syncs it,
then marks it committed in `-shm`, through mapped memory, which sends no event. A pull right
after the event read the previous seq every time on ext4 (100 of 100 commits of the command; 198
of 200 of a writer that keeps the file open, 49 of 200 on tmpfs); 30 ms after it, it missed none
of 50. A watcher pulls once the folder has been quiet for some tens of milliseconds after the
event, and a long-lived one also pulls now and then, which bounds a miss on a slow sync.
