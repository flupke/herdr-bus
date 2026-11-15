# Another machine's bus is read through a session over ssh

A consumer on one machine waits for the messages of another machine's bus. The bus is one local
file with no daemon and no network, and a program reaches another machine by running the
command there. `hc exec --on <machine>` runs one command, but cannot hold a session: it takes
its input whole and returns the output when the program ends (hc 0.1.21).

A consumer that waits holds a session with `herdr-bus serve <consumer>`, in JSON lines over the
command's stdin and stdout, on its own machine too. For another machine, the client crate takes
the machine's name as `hc machine ls` gives it, reads its ssh target there, and runs the command
through `ssh`, from a login shell's `PATH`. ssh never prompts, gives up on a machine that does
not answer, and ends a link that went silent, so that a waiting pull returns an error and the
caller connects again. A machine `hc` reaches through a command of its own, not ssh, is reached
through that command, which the app gives, since `hc machine ls` does not show it; the client
gives it the same line as its last argument. A command that needs no session, such as a
publish, still goes through `hc exec`.

Considered:

- Pulling through `hc exec` at an interval. Each pull is a round trip to the machine, and a
  message waits for the next one.
- A daemon on each machine that listens on the network. The bus would need a process to keep
  alive and a port to protect, where it has neither.
