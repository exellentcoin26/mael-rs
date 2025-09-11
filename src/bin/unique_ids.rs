use std::{collections::HashSet, io::Write};

use anyhow::Result;
use mael::{Node, Sender};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

#[derive(Debug, Serialize, Deserialize)]
#[serde[tag = "type", rename_all = "snake_case"]]
enum Request {
    Init {
        node_id: String,
        node_ids: HashSet<String>,
    },
    Generate,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Response {
    InitOk,
    GenerateOk { id: Ulid },
}

struct UniqueIdNode;

impl Node for UniqueIdNode {
    type Request = Request;

    type Response = Response;

    fn handle(&mut self, request: Self::Request, _: Sender<impl Write>) -> Result<Self::Response> {
        Ok(match request {
            Request::Init { .. } => Response::InitOk,
            Request::Generate => Response::GenerateOk { id: Ulid::new() },
        })
    }
}

fn main() -> Result<()> {
    let stdin = std::io::stdin().lock();
    let stdout = std::io::stdout().lock();

    UniqueIdNode.run(stdin, stdout)
}
