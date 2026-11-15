//! Consumers' names: `<name>@<machine id>`, or a bare `<name>` on this machine.

use uuid::Uuid;

use crate::{Bus, Error, Result};

impl Bus {
    /// The full name of `consumer`, `<name>@<machine id>`, the form the bus stores and
    /// compares: a bare name gets this bus's id, and a machine id its hyphenated form. Split at
    /// the last `@`, the name must not be empty, and the machine id must be a UUID.
    pub(crate) fn consumer(&self, consumer: &str) -> Result<String> {
        let bad = || Error::BadConsumer(consumer.to_owned());
        match consumer.rsplit_once('@') {
            None if !consumer.is_empty() => Ok(format!("{consumer}@{}", self.id)),
            Some((name, machine)) if !name.is_empty() => {
                let machine: Uuid = machine.parse().map_err(|_| bad())?;
                Ok(format!("{name}@{machine}"))
            }
            _ => Err(bad()),
        }
    }
}
