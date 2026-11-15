use super::*;

/// The format the other apps' tickets are written against: a rename here breaks them.
#[test]
fn words_have_the_shared_format() {
    let to_a_pane = Words {
        author: USER.into(),
        to: Some(pane("w1-3")),
        text: "Ship it".into(),
    };
    assert_eq!(
        serde_json::to_string(&to_a_pane).unwrap(),
        r#"{"author":"user","to":"pane:w1-3","text":"Ship it"}"#
    );
    let to_no_one = Words {
        to: None,
        ..to_a_pane
    };
    assert_eq!(
        serde_json::to_string(&to_no_one).unwrap(),
        r#"{"author":"user","to":null,"text":"Ship it"}"#
    );
    assert_eq!(TYPE, "herdr.words");
}
