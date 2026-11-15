//! Connections: which consumer reads which output, and how far it got.

use jiff::Timestamp;
use rusqlite::{OptionalExtension, Row, Transaction, params};
use serde::Serialize;

use crate::codec::parsed;
use crate::outputs::require;
use crate::{Bus, ConnectionId, Error, Result, Seq};

/// The output of a tap: every output, present and future.
pub const TAP: &str = "*";

/// Where a new connection's consumer starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Start {
    /// With the messages published from now on.
    Now,
    /// With every message its output holds, or the log for a tap.
    Beginning,
}

/// A connection: one consumer's cursor on one output, or on every output for a tap.
#[derive(Clone, Debug, Serialize)]
pub struct Connection {
    pub id: ConnectionId,
    /// The consumer's full name, `<name>@<machine id>`.
    pub consumer: String,
    /// The output's name, or [`TAP`].
    pub output: String,
    /// A disabled connection delivers nothing, and keeps its cursor.
    pub enabled: bool,
    /// The last message its consumer acknowledged, or one before the first it reads.
    pub cursor: Seq,
    /// How many messages of its output are past its cursor.
    pub lag: u64,
    pub created: Timestamp,
}

impl Bus {
    /// Connects `consumer`, a bare or full name, to `output`, an output's name or [`TAP`], and
    /// returns the connection's id. A connection that exists already stays as it was, cursor
    /// and `enabled` included, whatever `start` says.
    pub fn connect(&self, consumer: &str, output: &str, start: Start) -> Result<ConnectionId> {
        let consumer = self.consumer(consumer)?;
        self.write(|tx| {
            if output != TAP {
                require(tx, output)?;
            }
            let existing = tx
                .query_row(
                    "SELECT id FROM connections WHERE consumer = ?1 AND output = ?2",
                    [&consumer, output],
                    |row| row.get(0).map(ConnectionId),
                )
                .optional()?;
            if let Some(id) = existing {
                return Ok(id);
            }
            let cursor: i64 = match start {
                Start::Beginning => 0,
                // Seqs grow across every output, so the log's last one is past each output's.
                Start::Now => {
                    tx.query_row("SELECT coalesce(max(seq), 0) FROM messages", [], |row| {
                        row.get(0)
                    })?
                }
            };
            tx.execute(
                "INSERT INTO connections (consumer, output, cursor, created) VALUES (?1, ?2, ?3, ?4)",
                params![consumer, output, cursor, Timestamp::now().to_string()],
            )?;
            Ok(ConnectionId(tx.last_insert_rowid()))
        })
    }

    /// Removes a connection. The messages it read stay in the log.
    pub fn disconnect(&self, connection: ConnectionId) -> Result<()> {
        self.write(|tx| {
            let removed = tx.execute("DELETE FROM connections WHERE id = ?1", [connection.0])?;
            found(removed, connection)
        })
    }

    /// Enables or disables a connection: a disabled one delivers nothing, and keeps its cursor.
    pub fn set_enabled(&self, connection: ConnectionId, enabled: bool) -> Result<()> {
        self.write(|tx| {
            let changed = tx.execute(
                "UPDATE connections SET enabled = ?1 WHERE id = ?2",
                params![enabled, connection.0],
            )?;
            found(changed, connection)
        })
    }

    /// Every connection, by id.
    pub fn connections(&self) -> Result<Vec<Connection>> {
        self.read(Self::all_connections)
    }

    /// The connection `id`.
    pub fn connection(&self, id: ConnectionId) -> Result<Connection> {
        self.read(|tx| {
            tx.query_row(&format!("{SELECT} WHERE c.id = ?1"), [id.0], connection)
                .optional()?
                .ok_or(Error::UnknownConnection(id))
        })
    }

    pub(crate) fn all_connections(tx: &Transaction) -> Result<Vec<Connection>> {
        Ok(tx
            .prepare(&format!("{SELECT} ORDER BY c.id"))?
            .query_map([], connection)?
            .collect::<rusqlite::Result<_>>()?)
    }

    /// The connections on the output `name`, by id: taps are not.
    pub(crate) fn connections_on(tx: &Transaction, name: &str) -> Result<Vec<Connection>> {
        Ok(tx
            .prepare(&format!("{SELECT} WHERE c.output = ?1 ORDER BY c.id"))?
            .query_map([name], connection)?
            .collect::<rusqlite::Result<_>>()?)
    }
}

/// The output `connection` reads: an output's name, or [`TAP`].
pub(crate) fn output_of(tx: &Transaction, connection: ConnectionId) -> Result<String> {
    tx.query_row(
        "SELECT output FROM connections WHERE id = ?1",
        [connection.0],
        |row| row.get(0),
    )
    .optional()?
    .ok_or(Error::UnknownConnection(connection))
}

fn found(rows: usize, connection: ConnectionId) -> Result<()> {
    if rows == 0 {
        Err(Error::UnknownConnection(connection))
    } else {
        Ok(())
    }
}

// A branch for each kind, as in `Bus::positions`: an `OR` of the two would read the whole log
// past the cursor, where an output's count reads its index.
const SELECT: &str = "
    SELECT c.id, c.consumer, c.output, c.enabled, c.cursor, c.created,
      CASE WHEN c.output = '*' THEN (SELECT count(*) FROM messages WHERE seq > c.cursor)
      ELSE (SELECT count(*) FROM messages WHERE output = c.output AND seq > c.cursor) END
    FROM connections c";

fn connection(row: &Row<'_>) -> rusqlite::Result<Connection> {
    Ok(Connection {
        id: ConnectionId(row.get(0)?),
        consumer: row.get(1)?,
        output: row.get(2)?,
        enabled: row.get(3)?,
        cursor: Seq(row.get(4)?),
        // A count is never negative.
        lag: u64::try_from(row.get::<_, i64>(6)?).unwrap_or_default(),
        created: parsed(row, 5)?,
    })
}
