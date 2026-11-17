//! A message bus in one `SQLite` file, which every program opens itself: no daemon.
//!
//! A program declares its outputs and publishes messages on them. A consumer is named
//! `<name>@<machine id>`, the id of its machine's bus, or by its bare `<name>` on this machine.
//! A connection is one consumer's cursor on one output, or on every output for a tap: the last
//! message the consumer acknowledged there. A consumer pulls the messages of its connections
//! and acknowledges each one it handled; a message it does not acknowledge comes back on its
//! next pull, after a crash too.
//!
//! A consumer that waits for its messages, on this machine or another, reads them through a
//! session with `herdr-bus serve` instead, with the client of the `herdr-bus-client` crate.
//!
//! ```no_run
//! use herdr_bus::{Bus, Message, Output, Start};
//!
//! let bus = Bus::open_default()?;
//! bus.declare(&Output::new("events").types(["producer.thing.happened"]))?;
//! bus.connect("inbox", "events", Start::Now)?;
//!
//! let data = serde_json::json!({"any": "JSON the type defines"});
//! bus.publish("events", &Message::new("producer.thing.happened", data))?;
//!
//! for delivery in bus.pull("inbox", 50)? {
//!     // Handle delivery.message, then:
//!     bus.ack(delivery.connection, delivery.seq)?;
//! }
//! # Ok::<(), herdr_bus::Error>(())
//! ```

mod codec;
mod connections;
mod consumers;
mod delivery;
mod error;
mod messages;
mod outputs;
mod requests;
mod schema;

use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use rusqlite::{Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};

pub use connections::{Connection, Start, TAP};
pub use delivery::{Delivery, Position};
pub use error::{Error, Result};
/// The type of a message's `time`, so that an app need not take `jiff` itself.
pub use jiff::Timestamp;
pub use messages::{Link, Message, MessageQuery, Stored};
pub use outputs::{Declared, Output};
pub use requests::{Outcome, Reply, Request, RequestState, Take};
/// The type of a message's `id`, so that an app need not take `uuid` itself.
pub use uuid::Uuid;

/// The environment variable that names the bus file, over the default.
pub const DB_VARIABLE: &str = "HERDR_BUS_DB";

/// A message's place in the log: every message of every output gets the next one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Seq(pub i64);

/// A connection's id, which the bus never gives again once the connection is removed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ConnectionId(pub i64);

/// An open bus file.
pub struct Bus {
    sql: rusqlite::Connection,
    id: Uuid,
}

impl Bus {
    /// Opens the bus file at `path`, creating it and its folder when missing.
    pub fn open(path: &Path) -> Result<Self> {
        let (sql, id) = schema::open(path)?;
        Ok(Self { sql, id })
    }

    /// The bus's id, a random UUID made with its file: the machine id that ends the names of
    /// this machine's consumers.
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Opens the machine's bus file, at [`Bus::default_path`].
    pub fn open_default() -> Result<Self> {
        Self::open(&Self::default_path()?)
    }

    /// `$HERDR_BUS_DB` when set, else `$XDG_DATA_HOME/herdr-bus/bus.db`, else
    /// `~/.local/share/herdr-bus/bus.db`.
    pub fn default_path() -> Result<PathBuf> {
        let set = |name| std::env::var_os(name).filter(|value| !value.is_empty());
        if let Some(path) = set(DB_VARIABLE) {
            return Ok(PathBuf::from(path));
        }
        let data = set("XDG_DATA_HOME")
            .map(PathBuf::from)
            .filter(|path| path.is_absolute())
            .or_else(|| set("HOME").map(|home| PathBuf::from(home).join(".local/share")))
            .ok_or(Error::NoDefaultPath)?;
        Ok(data.join("herdr-bus/bus.db"))
    }

    /// Runs `change` in one short `BEGIN IMMEDIATE` transaction: an error rolls it back.
    fn write<T>(&self, change: impl FnOnce(&Transaction) -> Result<T>) -> Result<T> {
        self.transaction(TransactionBehavior::Immediate, change)
    }

    /// Runs `read` on one snapshot of the file.
    fn read<T>(&self, read: impl FnOnce(&Transaction) -> Result<T>) -> Result<T> {
        self.transaction(TransactionBehavior::Deferred, read)
    }

    fn transaction<T>(
        &self,
        behavior: TransactionBehavior,
        body: impl FnOnce(&Transaction) -> Result<T>,
    ) -> Result<T> {
        let tx = Transaction::new_unchecked(&self.sql, behavior)?;
        let value = body(&tx)?;
        tx.commit()?;
        Ok(value)
    }
}

impl fmt::Display for Seq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for Seq {
    type Err = std::num::ParseIntError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        text.parse().map(Self)
    }
}

impl FromStr for ConnectionId {
    type Err = std::num::ParseIntError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        text.parse().map(Self)
    }
}

#[cfg(test)]
#[path = "lib.tests.rs"]
mod tests;
