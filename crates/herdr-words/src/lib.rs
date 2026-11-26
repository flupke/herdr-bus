//! The `herdr.words` message type: words someone wrote to an actor, such as a prompt the user
//! typed to an agent. The programs that publish it and those that read it share this one
//! definition; the bus itself knows no type.
//!
//! ```
//! use herdr_words::{USER, Words, pane};
//!
//! let words = Words { author: USER.into(), to: Some(pane("w1-3")), text: "Ship it".into() };
//! let data = serde_json::to_value(&words)?;
//! # Ok::<(), serde_json::Error>(())
//! ```

use serde::{Deserialize, Serialize};

/// The type of a message whose `data` is [`Words`].
pub const TYPE: &str = "herdr.words";

/// The author of words the person at the keyboard wrote.
pub const USER: &str = "user";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Words {
    /// Who wrote them: [`USER`] for the person at the keyboard.
    pub author: String,
    /// The actor they were written to, such as [`pane`] for an agent in a pane, or `None`.
    pub to: Option<String>,
    pub text: String,
}

/// The actor that is the agent in a pane: `pane:<the pane's ID as Herdr gives it>`.
pub fn pane(id: &str) -> String {
    format!("pane:{id}")
}

#[cfg(test)]
#[path = "lib.tests.rs"]
mod tests;
