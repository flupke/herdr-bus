//! The session's lines: one JSON document a line, each way over the stdin and stdout of
//! `herdr-bus serve`, which speaks them too.
//!
//! `serve` first sends [`Line::Ready`]. Each [`Call`] then gets exactly one answer, in the
//! order sent. [`Line::Notify`], and the [`Line::Reply`] or [`Line::Timeout`] of each request
//! the session asked, come in between; nothing else comes unasked.

use herdr_bus::{ConnectionId, Delivery, Message, Seq, Take, Uuid};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A line the consumer sends `serve`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Call {
    /// At most `limit` of the consumer's messages, as [`Bus::pull`](herdr_bus::Bus::pull)
    /// returns them.
    GetMessages { limit: usize },
    /// The consumer handled the message at `seq` on `connection`, and every one before it
    /// there.
    Ack { connection: ConnectionId, seq: Seq },
    /// Publishes `message` on `output`, as [`Bus::publish`](herdr_bus::Bus::publish).
    Publish { output: String, message: Message },
    /// Asks `message` on `output` as a request, as
    /// [`Bus::request`](herdr_bus::Bus::request), and waits for its outcome: a
    /// [`Line::Reply`] or a [`Line::Timeout`] comes once `serve` has one.
    Request {
        output: String,
        message: Message,
        timeout_ms: u64,
    },
    /// Takes the request at `seq` before handling it, as [`Bus::take`](herdr_bus::Bus::take).
    Take { seq: Seq },
    /// Replies to the request at `to`, as [`Bus::reply`](herdr_bus::Bus::reply).
    Reply { to: Seq, data: Value },
}

/// A line `serve` sends the consumer.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Line {
    /// The session started: `serve`'s first line.
    Ready {},
    /// The consumer has messages past those the session delivered: get them. Sent once until
    /// the consumer next sends [`Call::GetMessages`].
    Notify {},
    /// The answer to [`Call::GetMessages`].
    Messages(Vec<Delivery>),
    /// The answer to [`Call::Ack`]: the cursor moved, or its connection is gone.
    Acked { connection: ConnectionId, seq: Seq },
    /// The answer to [`Call::Publish`]: the message's seq and id, the id given or generated.
    Published { seq: Seq, id: Uuid },
    /// The answer to [`Call::Request`]: the request's seq and id. Its reply or timeout comes
    /// later.
    Requested { seq: Seq, id: Uuid },
    /// The reply to the request at `to`, which the session asked: unasked, once.
    Reply { to: Seq, id: Uuid, data: Value },
    /// The deadline of the request at `to`, which the session asked, passed with no reply:
    /// unasked, once. `taken: false` is final: nobody will ever handle it.
    Timeout { to: Seq, id: Uuid, taken: bool },
    /// The answer to [`Call::Take`].
    Taken { seq: Seq, state: Take },
    /// The answer to [`Call::Reply`]: the request has its reply, this one or an earlier one.
    Replied { to: Seq },
    /// The answer to a line `serve` could not carry out; the session goes on.
    Error(String),
}
