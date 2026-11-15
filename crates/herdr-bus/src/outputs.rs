//! The outputs programs declare and publish on.

use jiff::Timestamp;
use rusqlite::{OptionalExtension, Row, Transaction, params};
use serde::Serialize;

use crate::codec::{json, parsed, to_json};
use crate::{Bus, Connection, ConnectionId, Error, Result, TAP};

/// An output as its program declares it.
#[derive(Clone, Debug)]
pub struct Output {
    name: String,
    description: String,
    types: Vec<String>,
}

/// An output as the bus holds it.
#[derive(Clone, Debug, Serialize)]
pub struct Declared {
    pub name: String,
    pub description: String,
    /// The message types it carries, as its program documents them.
    pub types: Vec<String>,
    pub declared: Timestamp,
    pub last_seen: Timestamp,
    /// The connections on it by name: a tap shows in [`Bus::connections`] only.
    pub connections: Vec<Connection>,
}

impl Output {
    /// The output named `name`, global to the bus: not empty, and not `*`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: String::new(),
            types: Vec::new(),
        }
    }

    /// What it carries, in a line for people.
    #[must_use]
    pub fn describe(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    /// The message types it carries.
    #[must_use]
    pub fn types<T: Into<String>>(mut self, types: impl IntoIterator<Item = T>) -> Self {
        self.types = types.into_iter().map(Into::into).collect();
        self
    }
}

impl Bus {
    /// Declares `output`, or updates its description, types and `last_seen` when it is
    /// declared already. A program declares its outputs each time it starts. A deleted output
    /// comes back, with its messages and without its connections.
    pub fn declare(&self, output: &Output) -> Result<Declared> {
        if output.name.is_empty() || output.name == TAP {
            return Err(Error::BadOutput(output.name.clone()));
        }
        self.write(|tx| {
            tx.execute(
                "INSERT INTO outputs (name, description, types, declared, last_seen)
                 VALUES (?1, ?2, ?3, ?4, ?4)
                 ON CONFLICT (name) DO UPDATE SET
                   description = excluded.description,
                   types = excluded.types,
                   last_seen = excluded.last_seen,
                   deleted = NULL",
                params![
                    output.name,
                    output.description,
                    to_json(&output.types),
                    Timestamp::now().to_string(),
                ],
            )?;
            // In the same transaction: a delete since would refuse an output just declared.
            declared_output(tx, &output.name)
        })
    }

    /// Deletes the output `name`, and returns the connections it removed: those on it by name.
    /// Taps stay. Its row stays, marked deleted, and so do its messages, which a tap that has
    /// yet to read them still gets.
    pub fn delete(&self, name: &str) -> Result<Vec<ConnectionId>> {
        self.write(|tx| {
            require(tx, name)?;
            tx.execute(
                "UPDATE outputs SET deleted = ?2 WHERE name = ?1",
                params![name, Timestamp::now().to_string()],
            )?;
            Ok(tx
                .prepare("DELETE FROM connections WHERE output = ?1 RETURNING id")?
                .query_map([name], |row| row.get(0).map(ConnectionId))?
                .collect::<rusqlite::Result<_>>()?)
        })
    }

    /// Every declared output but the deleted ones, by name, with its connections.
    pub fn outputs(&self) -> Result<Vec<Declared>> {
        self.read(|tx| {
            let connections = Self::all_connections(tx)?;
            let mut statement =
                tx.prepare(&format!("{SELECT} WHERE deleted IS NULL ORDER BY name"))?;
            let outputs = statement
                .query_map([], declared)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(outputs
                .into_iter()
                .map(|output| with_connections(output, &connections))
                .collect())
        })
    }

    /// The output `name`, with its connections. A deleted one is refused.
    pub fn output(&self, name: &str) -> Result<Declared> {
        self.read(|tx| declared_output(tx, name))
    }
}

const SELECT: &str = "SELECT name, description, types, declared, last_seen FROM outputs";

fn declared(row: &Row<'_>) -> rusqlite::Result<Declared> {
    Ok(Declared {
        name: row.get(0)?,
        description: row.get(1)?,
        types: json(row, 2)?,
        declared: parsed(row, 3)?,
        last_seen: parsed(row, 4)?,
        connections: Vec::new(),
    })
}

/// The output `name`, with its connections, unless it is deleted.
fn declared_output(tx: &Transaction, name: &str) -> Result<Declared> {
    require(tx, name)?;
    let mut output = tx.query_row(&format!("{SELECT} WHERE name = ?1"), [name], declared)?;
    output.connections = Bus::connections_on(tx, name)?;
    Ok(output)
}

fn with_connections(mut output: Declared, connections: &[Connection]) -> Declared {
    output.connections = connections
        .iter()
        .filter(|connection| connection.output == output.name)
        .cloned()
        .collect();
    output
}

/// Fails unless the output `name` is declared and not deleted.
pub(crate) fn require(tx: &Transaction, name: &str) -> Result<()> {
    let deleted: Option<bool> = tx
        .query_row(
            "SELECT deleted IS NOT NULL FROM outputs WHERE name = ?1",
            [name],
            |row| row.get(0),
        )
        .optional()?;
    match deleted {
        None => Err(Error::UnknownOutput(name.to_owned())),
        Some(true) => Err(Error::DeletedOutput(name.to_owned())),
        Some(false) => Ok(()),
    }
}
