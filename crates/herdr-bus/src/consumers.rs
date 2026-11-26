use uuid::Uuid;

use crate::{Bus, Error, Result};

impl Bus {
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
