use std::path::Path;
use std::time::{Duration, Instant};

use rusqlite::{Connection, ErrorCode, TransactionBehavior};

use uuid::Uuid;

use crate::codec::parsed;
use crate::{Error, Result};

const MIGRATIONS: &[&str] = &[
    "
    -- One row: the bus's id, a random UUID made with the file. A consumer's name ends with
    -- the id of its machine's bus.
    CREATE TABLE bus (
      id TEXT NOT NULL
    );

    CREATE TABLE outputs (
      name        TEXT PRIMARY KEY,
      description TEXT NOT NULL DEFAULT '',
      types       TEXT NOT NULL DEFAULT '[]',
      declared    TEXT NOT NULL,
      last_seen   TEXT NOT NULL,
      -- When it was deleted, or NULL: a deleted output keeps its row, so that its messages
      -- keep theirs, until it is declared again.
      deleted     TEXT
    );

    CREATE TABLE messages (
      seq       INTEGER PRIMARY KEY AUTOINCREMENT,
      output    TEXT NOT NULL REFERENCES outputs(name),
      id        TEXT NOT NULL,
      type      TEXT NOT NULL,
      subject   TEXT,
      time      TEXT NOT NULL,
      published TEXT NOT NULL,
      data      TEXT NOT NULL,
      links     TEXT NOT NULL DEFAULT '[]',
      UNIQUE (output, id)
    );
    CREATE INDEX messages_by_output ON messages (output, seq);

    -- `consumer` is always `<name>@<machine id>`, never a bare name. `output` is an output's
    -- name, or '*' for a tap, which no output row holds.
    -- AUTOINCREMENT: a consumer may still hold the id of a connection since removed, and its
    -- ack must not reach a new one.
    CREATE TABLE connections (
      id       INTEGER PRIMARY KEY AUTOINCREMENT,
      consumer TEXT NOT NULL,
      output   TEXT NOT NULL,
      enabled  INTEGER NOT NULL DEFAULT 1,
      cursor   INTEGER NOT NULL,
      created  TEXT NOT NULL,
      UNIQUE (consumer, output)
    );
    ",
    "
    -- A request is a message of the log with a deadline, which takes one reply. Times are
    -- RFC 3339, by the clock of this bus's machine.
    CREATE TABLE requests (
      seq      INTEGER PRIMARY KEY REFERENCES messages(seq),
      deadline TEXT NOT NULL,
      -- When a consumer took it, or NULL.
      taken    TEXT,
      -- The reply's data, JSON, and when it was given; both NULL until it is.
      reply    TEXT,
      replied  TEXT
    );
    ",
];

const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

fn known() -> i64 {
    i64::try_from(MIGRATIONS.len()).expect("a handful of migrations")
}

pub(crate) fn open(path: &Path) -> Result<(Connection, Uuid)> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        create_private_dir(parent).map_err(|source| Error::Io {
            path: parent.to_owned(),
            source,
        })?;
    }
    create_private_file(path).map_err(|source| Error::Io {
        path: path.to_owned(),
        source,
    })?;
    let mut connection = Connection::open(path)?;
    connection.busy_timeout(BUSY_TIMEOUT)?;
    let applied = refuse_newer(&connection, path)?;
    use_wal(&connection)?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    if applied < known() {
        migrate(&mut connection, path)?;
    }
    let id = connection.query_row("SELECT id FROM bus", [], |row| parsed(row, 0))?;
    Ok((connection, id))
}

fn create_private_dir(path: &Path) -> std::io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder.create(path)
}

/// Creates the bus file, empty, when it is missing, readable by its user alone: `SQLite` gives
/// its `-wal` and `-shm` files the same mode.
fn create_private_file(path: &Path) -> std::io::Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    options.open(path).map(drop)
}

/// Switches a new file to WAL. The switch takes an exclusive lock that the busy handler of `SQLite`
/// does not wait for, so a program that opens a new file while another switches it retries,
/// within the busy timeout.
fn use_wal(connection: &Connection) -> Result<()> {
    let deadline = Instant::now() + BUSY_TIMEOUT;
    loop {
        let switched = connection
            .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
            .and_then(|mode| {
                if mode.eq_ignore_ascii_case("wal") {
                    Ok(())
                } else {
                    connection.pragma_update(None, "journal_mode", "WAL")
                }
            });
        match switched {
            Err(error) if is_busy(&error) && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            switched => return Ok(switched?),
        }
    }
}

fn is_busy(error: &rusqlite::Error) -> bool {
    matches!(
        error.sqlite_error_code(),
        Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
    )
}

fn refuse_newer(connection: &Connection, path: &Path) -> Result<i64> {
    let found: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if found > known() {
        return Err(Error::NewerSchema {
            path: path.to_owned(),
            found,
            known: known(),
        });
    }
    Ok(found)
}

fn migrate(connection: &mut Connection, path: &Path) -> Result<()> {
    let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let applied = refuse_newer(&tx, path)?;
    if applied == known() {
        return Ok(());
    }
    for migration in &MIGRATIONS[usize::try_from(applied).expect("a version is positive")..] {
        tx.execute_batch(migration)?;
    }
    if applied == 0 {
        tx.execute(
            "INSERT INTO bus (id) VALUES (?1)",
            [Uuid::new_v4().to_string()],
        )?;
    }
    tx.pragma_update(None, "user_version", known())?;
    tx.commit()?;
    Ok(())
}
