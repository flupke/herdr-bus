# A reply is kept with its request

herdr-bowtie's tool calls wait for an answer from its daemon, from the same machine or another,
and its messaging is to run on the bus. Request and reply built outside the bus, by each program
from plain messages, would need every program to agree on where replies go, how they are named
and how a late or second reply is dropped.

A request is a message published with a deadline. Its one reply is free JSON, kept with it in a
table beside the log (`requests`), in the bus the request was asked on, and matched to it by the
request's seq. It is no message: no output carries it, and no tap receives it. The first reply
stands. An asker on another machine reads it through the session it asked through: the reply
travels nowhere.

Considered: the reply as a message on a reply output, named by the asker. Taps and herdr-patch
would show replies with no work, and a reply would travel as any message. But the file could not
hold a request to one reply, every app would need a naming rule for reply outputs and their
connections, and an asker on another machine would need a connection to its reply output there.
