//! Requests: messages with a deadline, which take one reply, kept with the request.
//!
//! Every judgment of a deadline is made by the clock of the bus's machine, inside the write it
//! decides, never by a consumer's: a request past its deadline that nobody took can no longer
//! be taken, so its asker's timeout, read under the same lock, says for good that nobody will
//! handle it.

use std::fmt;
use std::time::Duration;

use jiff::Timestamp;
use rusqlite::{OptionalExtension, Row, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::codec::{json, parsed_or_null, to_json};
use crate::messages::{append, seq_of};
use crate::outputs::require;
use crate::{Bus, Error, Message, Result, Seq};

/// A request as the bus holds it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Request {
    /// When its asker stops waiting, set once, when it was first asked: by then a consumer
    /// must have taken it, or none can.
    pub deadline: Timestamp,
    /// When a consumer took it, if one did.
    pub taken: Option<Timestamp>,
    /// Its one reply, once it has it.
    pub reply: Option<Reply>,
}

/// The one reply to a request: the first stands.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Reply {
    /// Free JSON, which the request's type defines.
    pub data: Value,
    pub replied: Timestamp,
}

/// Where a request stands, as a delivery of it says: [`Delivery::request`](crate::Delivery).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestState {
    pub deadline: Timestamp,
    /// Whether a consumer took it already: one that crashed after its take, say.
    pub taken: bool,
    /// Whether it has its reply.
    pub replied: bool,
}

/// What [`Bus::take`] found.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Take {
    /// The consumer took it: it handles it, and replies.
    Taken,
    /// It was taken already, by this consumer before a crash, say: the consumer looks up the
    /// message's id in its own store before it handles it again.
    TakenBefore,
    /// It has its reply: there is nothing left to do.
    Replied,
    /// Its deadline passed and nobody took it: its asker gave up, and nobody may handle it.
    Expired,
}

/// What became of a request, for its asker: [`Bus::outcome`].
#[derive(Clone, Debug, PartialEq)]
pub enum Outcome {
    /// Neither replied nor past its deadline yet.
    Pending { deadline: Timestamp },
    /// Its reply, whenever it came.
    Replied(Reply),
    /// Its deadline passed with no reply. `taken: false` is final: nobody will ever handle it.
    TimedOut { taken: bool },
}

impl Bus {
    /// Publishes `message` on `output` as a request, as [`Bus::publish`] does, with a deadline
    /// `timeout` from now, and returns its seq. Asked again under the same id, it is the same
    /// request: its seq, its deadline and its reply. An id the output holds as a plain message
    /// is refused.
    pub fn request(&self, output: &str, message: &Message, timeout: Duration) -> Result<Seq> {
        self.write(|tx| {
            require(tx, output)?;
            if let Some(seq) = seq_of(tx, output, message.id)? {
                return match read(tx, seq)? {
                    Some(_) => Ok(seq),
                    None => Err(Error::PlainMessage {
                        output: output.to_owned(),
                        id: message.id,
                    }),
                };
            }
            let seq = append(tx, output, message)?;
            let deadline = Timestamp::now()
                .saturating_add(timeout)
                .unwrap_or(Timestamp::MAX);
            tx.execute(
                "INSERT INTO requests (seq, deadline) VALUES (?1, ?2)",
                params![seq.0, deadline.to_string()],
            )?;
            Ok(seq)
        })
    }

    /// Takes the request at `seq` before handling it, unless it was taken already, has its
    /// reply, or is past its deadline untaken: see [`Take`].
    pub fn take(&self, seq: Seq) -> Result<Take> {
        self.write(|tx| {
            let request = read(tx, seq)?.ok_or(Error::NotARequest(seq))?;
            let now = Timestamp::now();
            Ok(if request.reply.is_some() {
                Take::Replied
            } else if request.taken.is_some() {
                Take::TakenBefore
            } else if now >= request.deadline {
                Take::Expired
            } else {
                tx.execute(
                    "UPDATE requests SET taken = ?2 WHERE seq = ?1",
                    params![seq.0, now.to_string()],
                )?;
                Take::Taken
            })
        })
    }

    /// Gives the request at `seq` its reply, and returns the reply that stands: the first one,
    /// when it had one already. A request nobody took is taken in the same write, unless it is
    /// past its deadline, which is refused. A request taken is replied to even past its
    /// deadline, so that asking it again returns the reply.
    pub fn reply(&self, seq: Seq, data: Value) -> Result<Reply> {
        self.write(|tx| {
            let request = read(tx, seq)?.ok_or(Error::NotARequest(seq))?;
            if let Some(reply) = request.reply {
                return Ok(reply);
            }
            let now = Timestamp::now();
            if request.taken.is_none() && now >= request.deadline {
                return Err(Error::Expired(seq));
            }
            tx.execute(
                "UPDATE requests SET taken = coalesce(taken, ?2), reply = ?3, replied = ?2
                 WHERE seq = ?1",
                params![seq.0, now.to_string(), to_json(&data)],
            )?;
            Ok(Reply { data, replied: now })
        })
    }

    /// What became of the request at `seq`, for its asker, who waits on it: its reply, a
    /// timeout, or neither yet.
    pub fn outcome(&self, seq: Seq) -> Result<Outcome> {
        let outcome = self.read(|tx| judge(tx, seq))?;
        match outcome {
            // Read again under the write lock: a take that read the clock before the deadline
            // may still be committing, and `taken: false` must be final.
            Outcome::TimedOut { .. } => self.write(|tx| judge(tx, seq)),
            outcome => Ok(outcome),
        }
    }
}

/// The request at `seq` in `tx`, or none when it is no request.
fn read(tx: &Transaction, seq: Seq) -> Result<Option<Request>> {
    Ok(tx
        .query_row(
            "SELECT deadline, taken, reply, replied FROM requests WHERE seq = ?1",
            [seq.0],
            |row| request_columns(row, 0),
        )
        .optional()?
        .flatten())
}

/// The outcome of the request at `seq`, by the clock now.
fn judge(tx: &Transaction, seq: Seq) -> Result<Outcome> {
    let request = read(tx, seq)?.ok_or(Error::NotARequest(seq))?;
    Ok(match request.reply {
        Some(reply) => Outcome::Replied(reply),
        None if Timestamp::now() < request.deadline => Outcome::Pending {
            deadline: request.deadline,
        },
        None => Outcome::TimedOut {
            taken: request.taken.is_some(),
        },
    })
}

/// The request in the four columns from `first` on, `deadline`, `taken`, `reply` and
/// `replied`: none when `deadline` is NULL, as for a plain message in a join.
pub(crate) fn request_columns(row: &Row<'_>, first: usize) -> rusqlite::Result<Option<Request>> {
    let Some(deadline) = parsed_or_null(row, first)? else {
        return Ok(None);
    };
    let reply = match parsed_or_null::<Timestamp>(row, first + 3)? {
        Some(replied) => Some(Reply {
            data: json(row, first + 2)?,
            replied,
        }),
        None => None,
    };
    Ok(Some(Request {
        deadline,
        taken: parsed_or_null(row, first + 1)?,
        reply,
    }))
}

impl Request {
    /// What a delivery says of it.
    pub(crate) fn state(&self) -> RequestState {
        RequestState {
            deadline: self.deadline,
            taken: self.taken.is_some(),
            replied: self.reply.is_some(),
        }
    }
}

impl fmt::Display for Take {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Take::Taken => "taken",
            Take::TakenBefore => "taken_before",
            Take::Replied => "replied",
            Take::Expired => "expired",
        })
    }
}
