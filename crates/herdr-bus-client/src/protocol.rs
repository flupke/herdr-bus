//! The session's lines: one JSON document a line, each way over the stdin and stdout of
//! `herdr-bus serve`, which speaks them too.
//!
//! `serve` first sends [`Line::Ready`]. Each [`Request`] then gets exactly one answer, in the
//! order sent; [`Line::Notify`] comes in between, and nothing else comes unasked.

use herdr_bus::{ConnectionId, Delivery, Seq};
use serde::{Deserialize, Serialize};

/// A line the consumer sends `serve`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Request {
    /// At most `limit` of the consumer's messages, as [`Bus::pull`](herdr_bus::Bus::pull)
    /// returns them.
    GetMessages { limit: usize },
    /// The consumer handled the message at `seq` on `connection`, and every one before it
    /// there.
    Ack { connection: ConnectionId, seq: Seq },
}

/// A line `serve` sends the consumer.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Line {
    /// The session started: `serve`'s first line.
    Ready {},
    /// The consumer has messages past those the session delivered: get them. Sent once until
    /// the consumer next sends [`Request::GetMessages`].
    Notify {},
    /// The answer to [`Request::GetMessages`].
    Messages(Vec<Delivery>),
    /// The answer to [`Request::Ack`]: the cursor moved, or its connection is gone.
    Acked { connection: ConnectionId, seq: Seq },
    /// The answer to a request `serve` could not carry out; the session goes on.
    Error(String),
}
