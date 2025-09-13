use std::{
    collections::{BTreeSet, HashMap, HashSet},
    io::{Read, Write},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use mael::{Message, Node, RequestInfo, Socket};
use serde::{Deserialize, Serialize};

const BROADCAST_RETRY_TIMEOUT: Duration = Duration::from_secs(4);

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
struct IdGen(u32);

impl IdGen {
    fn next_id(&mut self) -> u32 {
        let id = self.0;
        self.0 += 1;
        id
    }
}

#[derive(Default)]
struct BroadcastNode {
    node_id: String,
    id_gen: IdGen,
    messages: BTreeSet<u32>,
    neighbours: HashSet<String>,
    awaiting_broadcast_ok: HashMap<u32, (Instant, String, u32)>,
}

impl Node for BroadcastNode {
    type Request = Request;

    type Response = Response;

    fn handle_request(
        &mut self,
        request: Self::Request,
        info: RequestInfo,
        socket: &mut Socket<impl Read, impl Write>,
    ) -> Result<Self::Response> {
        // TODO: Move this to a seperate thread.
        let mut handled = HashSet::new();
        let mut new = HashMap::new();
        // This is a closure purely to update state even after an error occurs as this loop does
        // not mutate.
        let mut rebroadcast = || -> Result<()> {
            for (acknowledge_id, (sent_at, destination, message)) in
                self.awaiting_broadcast_ok.iter()
            {
                if sent_at.elapsed() < BROADCAST_RETRY_TIMEOUT {
                    continue;
                }

                // Resend the request.
                let message_id = self.id_gen.next_id();
                socket
                    .send(
                        Message::new(
                            self.node_id.clone(),
                            destination.clone(),
                            Request::Broadcast { message: *message },
                        )
                        .with_id(message_id),
                    )
                    .context("re-broadcasting message to neighbour")?;
                new.insert(message_id, (Instant::now(), destination.clone(), *message));
                handled.insert(*acknowledge_id);
            }
            Ok(())
        };
        let result = rebroadcast();
        // Remove the handled requests from the set regardless of the result. If the the error is
        // recoverable, the set still makes sense. No messages will be re-broadcast a second
        // time.
        handled.into_iter().for_each(|h| {
            self.awaiting_broadcast_ok.remove(&h);
        });
        self.awaiting_broadcast_ok.extend(new);
        result?;

        Ok(match request {
            Request::Init { node_id, .. } => {
                self.node_id = node_id;
                Response::InitOk
            }
            Request::Broadcast { message } => {
                self.messages.insert(message);
                for neighbour in self.neighbours.clone().into_iter() {
                    if neighbour == info.src {
                        // Do not broadcast the message back to where it came from.
                        continue;
                    }
                    let message_id = self.id_gen.next_id();
                    socket
                        .send(
                            Message::new(
                                self.node_id.clone(),
                                neighbour.clone(),
                                Request::Broadcast { message },
                            )
                            .with_id(message_id),
                        )
                        .context("broadcasting message to neighbour")?;
                    self.awaiting_broadcast_ok
                        .insert(message_id, (Instant::now(), neighbour, message));
                }
                Response::BroadcastOk
            }
            Request::Read => Response::ReadOk {
                messages: self.messages.clone(),
            },
            Request::Topology { mut topology } => {
                self.neighbours = topology.remove(&self.node_id).unwrap_or_default();
                // Ensure this node is not found in the neighbours.
                self.neighbours.remove(&self.node_id);
                Response::TopologyOk
            }
        })
    }

    fn handle_response(
        &mut self,
        response: Self::Response,
        info: mael::ResponseInfo,
        _socket: &mut Socket<impl Read, impl Write>,
    ) -> Result<()> {
        if let Response::BroadcastOk = response {
            let Some(in_reply_to) = info.in_reply_to else {
                // Should not happen.
                return Ok(());
            };
            self.awaiting_broadcast_ok.remove(&in_reply_to);
        }
        Ok(())
    }
}

fn main() -> Result<()> {
    let stdin = std::io::stdin().lock();
    let stdout = std::io::stdout().lock();
    let socket = Socket::new(stdin, stdout);

    BroadcastNode::default().run(socket)
}
