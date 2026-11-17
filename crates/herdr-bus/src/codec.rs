//! How the columns that are not plain SQL values read and write.

use std::str::FromStr;

use rusqlite::Row;
use rusqlite::types::Type;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// A TEXT column that parses: a timestamp in RFC 3339, or a hyphenated UUID.
pub(crate) fn parsed<T>(row: &Row<'_>, column: usize) -> rusqlite::Result<T>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let text: String = row.get(column)?;
    text.parse().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(column, Type::Text, Box::new(error))
    })
}

/// A JSON column.
pub(crate) fn json<T: DeserializeOwned>(row: &Row<'_>, column: usize) -> rusqlite::Result<T> {
    let text: String = row.get(column)?;
    serde_json::from_str(&text).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(column, Type::Text, Box::new(error))
    })
}

/// A value as a JSON column holds it.
pub(crate) fn to_json(value: &impl Serialize) -> String {
    serde_json::to_string(value).expect("a JSON value, a string list or links serialize")
}

/// A nullable TEXT column that parses, as [`parsed`] does.
pub(crate) fn parsed_or_null<T>(row: &Row<'_>, column: usize) -> rusqlite::Result<Option<T>>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match row.get_ref(column)? {
        rusqlite::types::ValueRef::Null => Ok(None),
        _ => parsed(row, column).map(Some),
    }
}
