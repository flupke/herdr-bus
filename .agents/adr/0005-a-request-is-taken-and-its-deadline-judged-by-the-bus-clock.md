# A request is taken, and its deadline judged by the clock of the bus's machine

An asker that times out must know whether its request will still be handled: herdr-bowtie tells
an agent "the call never reached the daemon, call again" apart from "it may have reached it,
check its effect first". A consumer that handles a request after its asker gave up does work
nobody waits for; one that sees a request again after a crash must not handle it twice. The
asker and the consumer may run on machines whose clocks differ.

The consumer that answers takes a request before it works on it, in one write. A request past
its deadline that nobody took cannot be taken, judged by the clock of the bus's machine inside
that write, never by the consumer's. The asker's timeout is read under the same write lock once
the deadline has passed, and says whether the request was taken: `taken: false` guarantees that
nobody will ever handle it. A second take says the request was taken already, and the consumer
dedupes on the message's id in its own store.

Considered:

- No take: the consumer replies when it is done, and the asker's timeout says nothing of the
  request's fate. A consumer slow to start, or one that pulled a request again after a crash,
  would handle requests their askers gave up on, and the asker could not tell whether to call
  again.
- The deadline judged by the consumer, from the request's deadline and its own clock. Two
  machines' clocks differ, so a request could be both expired for its asker and taken by its
  consumer.
