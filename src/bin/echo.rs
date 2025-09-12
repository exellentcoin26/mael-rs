use std::{
    collections::HashSet,
    io::{Read, Write},
};

use anyhow::Result;
use mael::{Node, Sender};
use serde::{Deserialize, Serialize};

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
#[serde(tag = "type", rename_all = "snake_case")]
enum Response {
    InitOk,
    EchoOk { echo: String },
}

struct EchoNode;

impl Node for EchoNode {
    type Request = Request;

    type Response = Response;

    fn handle(
        &mut self,
        request: Self::Request,
        _: Sender<impl Read, impl Write>,
    ) -> Result<Self::Response> {
        Ok(match request {
            Request::Init { .. } => Response::InitOk,
            Request::Echo { echo } => Response::EchoOk { echo },
        })
    }
}

fn main() -> Result<()> {
    let stdin = std::io::stdin().lock();
    let stdout = std::io::stdout().lock();

    EchoNode.run(stdin, stdout)
}
