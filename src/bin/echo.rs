use std::collections::HashSet;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Message<T> {
    src: String,
    dest: String,
    body: MessageBody<T>,
}

#[derive(Debug, Serialize, Deserialize)]
struct MessageBody<T> {
    #[serde(rename = "msg_id")]
    id: u32,
    #[serde(flatten)]
    type_: T,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde[tag = "type", rename_all = "snake_case"]]
enum Request {
    Init {
        node_id: String,
        node_ids: HashSet<String>,
    },
    Echo {
        echo: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct Response {
    in_reply_to: u32,
    #[serde(flatten)]
    kind: ResponseKind,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponseKind {
    InitOk,
    EchoOk { echo: String },
}

fn main() -> Result<()> {
    let stdin = std::io::stdin().lock();

    for message in serde_json::Deserializer::from_reader(stdin).into_iter::<Message<Request>>() {
        let message = message.context("reading message from stdin")?;

        let response_kind = match message.body.type_ {
            Request::Init { .. } => ResponseKind::InitOk,
            Request::Echo { echo } => ResponseKind::EchoOk { echo },
        };
        let response_message = Message {
            src: message.dest,
            dest: message.src,
            body: MessageBody {
                id: message.body.id,
                type_: Response {
                    in_reply_to: message.body.id,
                    kind: response_kind,
                },
            },
        };

        println!(
            "{}",
            serde_json::to_string(&response_message)
                .context("converting message response to json")?
        );
    }

    Ok(())
}
