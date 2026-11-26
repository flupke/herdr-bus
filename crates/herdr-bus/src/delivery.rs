use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};

use crate::connections::output_of;
use crate::messages::{SELECT, limit as sql_limit, stored};
use crate::{Bus, ConnectionId, Error, Message, Request, RequestState, Result, Seq, TAP};

/// A message a pull returned, and the connection it came through: acknowledge it with both.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Delivery {
    pub connection: ConnectionId,
    pub seq: Seq,
    /// The output it was published on: a tap reads several.
    pub output: String,
    pub message: Message,
    /// Where it stands as a request, when it is one: take it before handling it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request: Option<RequestState>,
}

impl Bus {
    /// At most `limit` of the messages past the cursor of each enabled connection of
    /// `consumer`, a bare or full name: connections by id, then messages by seq. It moves no
    /// cursor, so a message comes back on each pull until it is acknowledged. A consumer with
    /// no connection has none.
    pub fn pull(&self, consumer: &str, limit: usize) -> Result<Vec<Delivery>> {
        let consumer = self.consumer(consumer)?;
        self.read(|tx| {
            let connections = tx
                .prepare(
                    "SELECT id, output, cursor FROM connections
                     WHERE consumer = ?1 AND enabled ORDER BY id",
                )?
                .query_map([&consumer], |row| {
                    Ok((
                        ConnectionId(row.get(0)?),
                        row.get::<_, String>(1)?,
                        row.get(2)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<(ConnectionId, String, i64)>>>()?;
            let mut deliveries = Vec::new();
            let mut tap = tx.prepare(&format!(
                "{SELECT} WHERE m.seq > ?1 ORDER BY m.seq LIMIT ?2"
            ))?;
            let mut one = tx.prepare(&format!(
                "{SELECT} WHERE m.output = ?3 AND m.seq > ?1 ORDER BY m.seq LIMIT ?2"
            ))?;
            for (connection, output, cursor) in connections {
                let left = sql_limit(limit - deliveries.len());
                if left == 0 {
                    break;
                }
                let rows = if output == TAP {
                    tap.query_map(params![cursor, left], stored)?
                        .collect::<rusqlite::Result<Vec<_>>>()?
                } else {
                    one.query_map(params![cursor, left, output], stored)?
                        .collect::<rusqlite::Result<Vec<_>>>()?
                };
                deliveries.extend(rows.into_iter().map(|stored| Delivery {
                    connection,
                    seq: stored.seq,
                    output: stored.output,
                    message: stored.message,
                    request: stored.request.as_ref().map(Request::state),
                }));
            }
            Ok(deliveries)
        })
    }

    /// Acknowledges the message at `seq` on `connection`, and every one before it there: its
    /// cursor moves to `seq` unless it is past it already. Acknowledging twice changes nothing.
    pub fn ack(&self, connection: ConnectionId, seq: Seq) -> Result<()> {
        self.write(|tx| {
            let reads = output_of(tx, connection)?;
            let output: Option<String> = tx
                .query_row(
                    "SELECT output FROM messages WHERE seq = ?1",
                    [seq.0],
                    |row| row.get(0),
                )
                .optional()?;
            match output {
                Some(output) if reads == TAP || output == reads => {}
                _ => return Err(Error::NotOnConnection { connection, seq }),
            }
            tx.execute(
                "UPDATE connections SET cursor = max(cursor, ?1) WHERE id = ?2",
                params![seq.0, connection.0],
            )?;
            Ok(())
        })
    }

    /// Where each enabled connection of `consumer`, a bare or full name, stands, by id: its
    /// cursor, and the newest message of its output.
    pub fn positions(&self, consumer: &str) -> Result<Vec<Position>> {
        let consumer = self.consumer(consumer)?;
        self.read(|tx| {
            Ok(tx
                .prepare(
                    "SELECT c.id, c.cursor,
                       CASE WHEN c.output = '*' THEN (SELECT max(seq) FROM messages)
                       ELSE (SELECT max(seq) FROM messages WHERE output = c.output) END
                     FROM connections c WHERE c.consumer = ?1 AND c.enabled ORDER BY c.id",
                )?
                .query_map([&consumer], |row| {
                    Ok(Position {
                        connection: ConnectionId(row.get(0)?),
                        cursor: Seq(row.get(1)?),
                        newest: row.get::<_, Option<i64>>(2)?.map(Seq),
                    })
                })?
                .collect::<rusqlite::Result<_>>()?)
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Position {
    pub connection: ConnectionId,
    /// The last message its consumer acknowledged.
    pub cursor: Seq,
    pub newest: Option<Seq>,
}
