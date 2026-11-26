use jiff::Timestamp;
use rusqlite::types::Value;
use rusqlite::{OptionalExtension, Row, Transaction, params, params_from_iter};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::codec::{json, parsed, to_json};
use crate::connections::output_of;
use crate::outputs::require;
use crate::requests::request_columns;
use crate::{Bus, ConnectionId, Request, Result, Seq, TAP};

/// A message's envelope, `CloudEvents`-shaped: what a producer publishes and a consumer pulls.
///
/// As JSON, only `type` and `data` are required, as with [`Message::new`]: without an `id`, it
/// gets a fresh `UUIDv7`, without a `time`, now.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Message {
    /// A fresh `UUIDv7`, or the producer's own UUID, stable across its retries: publishing an
    /// id already on the output adds nothing.
    #[serde(default = "Uuid::now_v7")]
    pub id: Uuid,
    /// Such as `producer.thing.happened`; its producer defines its `data`.
    #[serde(rename = "type")]
    pub kind: String,
    /// A short line for listings.
    pub subject: Option<String>,
    /// When the event happened.
    #[serde(default = "Timestamp::now")]
    pub time: Timestamp,
    pub data: serde_json::Value,
    #[serde(default)]
    pub links: Vec<Link>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Link {
    pub label: String,
    pub url: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct Stored {
    pub seq: Seq,
    pub output: String,
    /// When it was appended.
    pub published: Timestamp,
    pub message: Message,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request: Option<Request>,
}

#[derive(Clone, Debug)]
pub struct MessageQuery {
    pub output: Option<String>,
    /// Only those this connection reads.
    pub connection: Option<ConnectionId>,
    /// Only those before this one, to page back.
    pub before: Option<Seq>,
    pub limit: usize,
}

impl Message {
    /// A message whose event happens now, with a fresh `UUIDv7` for its id, no subject and no
    /// links. A producer that may retry its publish keeps this message, or its id, for the
    /// retry.
    pub fn new(kind: impl Into<String>, data: serde_json::Value) -> Self {
        Self {
            id: Uuid::now_v7(),
            kind: kind.into(),
            subject: None,
            time: Timestamp::now(),
            data,
            links: Vec::new(),
        }
    }

    /// The producer's own id. One whose own ids are not UUIDs derives them with
    /// [`Uuid::new_v5`].
    #[must_use]
    pub fn id(mut self, id: Uuid) -> Self {
        self.id = id;
        self
    }

    #[must_use]
    pub fn subject(mut self, subject: impl Into<String>) -> Self {
        self.subject = Some(subject.into());
        self
    }

    #[must_use]
    pub fn time(mut self, time: Timestamp) -> Self {
        self.time = time;
        self
    }

    #[must_use]
    pub fn link(mut self, label: impl Into<String>, url: impl Into<String>) -> Self {
        self.links.push(Link {
            label: label.into(),
            url: url.into(),
        });
        self
    }
}

impl Default for MessageQuery {
    fn default() -> Self {
        Self {
            output: None,
            connection: None,
            before: None,
            limit: 50,
        }
    }
}

impl Bus {
    /// Appends `message` to `output`, and returns its seq: the seq it already has when the
    /// output holds its id already, which then stays as it was.
    pub fn publish(&self, output: &str, message: &Message) -> Result<Seq> {
        self.write(|tx| {
            require(tx, output)?;
            match seq_of(tx, output, message.id)? {
                Some(seq) => Ok(seq),
                None => append(tx, output, message),
            }
        })
    }

    pub fn message(&self, seq: Seq) -> Result<Stored> {
        self.read(|tx| {
            tx.query_row(&format!("{SELECT} WHERE m.seq = ?1"), [seq.0], stored)
                .optional()?
                .ok_or(crate::Error::UnknownMessage(seq))
        })
    }

    /// The messages `query` names, newest first.
    pub fn messages(&self, query: &MessageQuery) -> Result<Vec<Stored>> {
        self.read(|tx| {
            let mut conditions = vec!["TRUE".to_owned()];
            let mut values = Vec::new();
            let mut on = |condition: &str, value: Value| {
                values.push(value);
                conditions.push(format!("{condition} ?{}", values.len()));
            };
            if let Some(output) = &query.output {
                on("m.output =", Value::Text(output.clone()));
            }
            if let Some(connection) = query.connection {
                let output = output_of(tx, connection)?;
                if output != TAP {
                    on("m.output =", Value::Text(output));
                }
            }
            if let Some(before) = query.before {
                on("m.seq <", Value::Integer(before.0));
            }
            values.push(Value::Integer(limit(query.limit)));
            let sql = format!(
                "{SELECT} WHERE {} ORDER BY m.seq DESC LIMIT ?{}",
                conditions.join(" AND "),
                values.len()
            );
            Ok(query_all(tx, &sql, values)?)
        })
    }
}

pub(crate) fn seq_of(tx: &Transaction, output: &str, id: Uuid) -> Result<Option<Seq>> {
    Ok(tx
        .query_row(
            "SELECT seq FROM messages WHERE output = ?1 AND id = ?2",
            params![output, id.to_string()],
            |row| row.get(0).map(Seq),
        )
        .optional()?)
}

/// Appends `message` to `output`, which holds no message of its id: an insert that adds
/// nothing would still use up a seq.
pub(crate) fn append(tx: &Transaction, output: &str, message: &Message) -> Result<Seq> {
    tx.execute(
        "INSERT INTO messages (output, id, type, subject, time, published, data, links)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            output,
            message.id.to_string(),
            message.kind,
            message.subject,
            message.time.to_string(),
            Timestamp::now().to_string(),
            to_json(&message.data),
            to_json(&message.links),
        ],
    )?;
    Ok(Seq(tx.last_insert_rowid()))
}

pub(crate) const SELECT: &str = "
    SELECT m.seq, m.output, m.published, m.id, m.type, m.subject, m.time, m.data, m.links,
      r.deadline, r.taken, r.reply, r.replied
    FROM messages m LEFT JOIN requests r ON r.seq = m.seq";

pub(crate) fn stored(row: &Row<'_>) -> rusqlite::Result<Stored> {
    Ok(Stored {
        seq: Seq(row.get(0)?),
        output: row.get(1)?,
        published: parsed(row, 2)?,
        message: Message {
            id: parsed(row, 3)?,
            kind: row.get(4)?,
            subject: row.get(5)?,
            time: parsed(row, 6)?,
            data: json(row, 7)?,
            links: json(row, 8)?,
        },
        request: request_columns(row, 9)?,
    })
}

fn query_all(tx: &Transaction, sql: &str, values: Vec<Value>) -> rusqlite::Result<Vec<Stored>> {
    tx.prepare(sql)?
        .query_map(params_from_iter(values), stored)?
        .collect()
}

pub(crate) fn limit(limit: usize) -> i64 {
    i64::try_from(limit).unwrap_or(i64::MAX)
}
