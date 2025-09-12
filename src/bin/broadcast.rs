use std::{
    collections::{BTreeSet, HashMap, HashSet},
    io::{Read, Write},
};

use anyhow::{Context, Result};
use mael::{Message, Node, Sender};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde[tag = "type", rename_all = "snake_case"]]
enum Request {
    Init {
        node_id: String,
        node_ids: HashSet<String>,
    },
    Broadcast {
        message: u32,
    },
    Read,
    Topology {
        topology: HashMap<String, HashSet<String>>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(clippy::enum_variant_names)]
enum Response {
    InitOk,
    BroadcastOk,
    ReadOk { messages: BTreeSet<u32> },
    TopologyOk,
}

#[derive(Default)]
struct BroadcastNode {
    id: String,
    messages: BTreeSet<u32>,
    neighbours: HashSet<String>,
}

impl Node for BroadcastNode {
    type Request = Request;

    type Response = Response;

    fn handle(
        &mut self,
        request: Self::Request,
        mut sender: Sender<impl Read, impl Write>,
    ) -> Result<Self::Response> {
        Ok(match request {
            Request::Init { node_id, .. } => {
                self.id = node_id;
                Response::InitOk
            }
            Request::Broadcast { message } => {
                self.messages.insert(message);
                for neighbour in self.neighbours.iter() {
                    sender
                        .send(Message::new(
                            self.id.clone(),
                            neighbour.clone(),
                            Request::Broadcast { message },
                        ))
                        .context("broadcasting message to neighbour")?;
                }
                Response::BroadcastOk
            }
            Request::Read => Response::ReadOk {
                messages: self.messages.clone(),
            },
            Request::Topology { mut topology } => {
                self.neighbours = topology.remove(&self.id).unwrap_or_default();
                Response::TopologyOk
            }
        })
    }
}

fn main() -> Result<()> {
    let stdin = std::io::stdin().lock();
    let stdout = std::io::stdout().lock();

    BroadcastNode::default().run(stdin, stdout)
}
