use std::path::PathBuf;

use crate::{ConnectionId, Seq};

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Why a bus operation failed.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("no bus file: set HERDR_BUS_DB, XDG_DATA_HOME or HOME")]
    NoDefaultPath,
    /// The file was written by a newer herdr-bus, whose schema this one does not know: it is
    /// refused before anything is written to it.
    #[error(
        "{path} holds bus schema {found}, newer than this herdr-bus knows ({known}): update herdr-bus"
    )]
    NewerSchema {
        path: PathBuf,
        found: i64,
        known: i64,
    },
    #[error("bad output name {0:?}: a name is not empty, and not '*'")]
    BadOutput(String),
    #[error("no output {0}: declare it first")]
    UnknownOutput(String),
    #[error("{0} was deleted: declare it again to use it")]
    DeletedOutput(String),
    #[error("bad consumer {0:?}: a consumer is <name>, or <name>@<machine id>, a UUID")]
    BadConsumer(String),
    #[error("no connection {0}")]
    UnknownConnection(ConnectionId),
    #[error("no message {seq} on connection {connection}")]
    NotOnConnection { connection: ConnectionId, seq: Seq },
    #[error("no message {0}")]
    UnknownMessage(Seq),
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
}
