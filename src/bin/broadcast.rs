use std::{
    collections::{BTreeSet, HashMap, HashSet},
    io::{Read, Write},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use mael::{Message, Node, RequestInfo, Socket};
use serde::{Deserialize, Serialize};

const BROADCAST_RETRY_TIMEOUT: Duration = Duration::from_secs(1);

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
struct RetrySet {
    messages: HashMap<u32, HashSet<u32>>,
}

impl RetrySet {
    fn register(&mut self, original_message: u32, retry: u32) {
        self.messages
            .entry(original_message)
            .or_default()
            .insert(retry);
    }

    /// Removes the retry chain of messages and returns the original message id of the chain.
    fn remove_chain_for_message(&mut self, message: u32) -> Option<u32> {
        let original = self
            .messages
            .iter()
            .find_map(|(o, rc)| rc.contains(&message).then_some(*o));
        original.inspect(|o| {
            self.messages.remove(o);
        })
    }
}

struct BroadcastOkWaiter {
    message_id: u32,
    last_sent: Instant,
    /// Use an exponential back-off.
    retries: u32,
    dest: String,
    message: u32,
}

impl BroadcastOkWaiter {
    fn retry(&mut self, new_message_id: u32) {
        self.message_id = new_message_id;
        self.last_sent = Instant::now();
        self.retries += 1;
    }
}

#[derive(Default)]
struct BroadcastNode {
    node_id: String,
    id_gen: IdGen,
    messages: BTreeSet<u32>,
    neighbours: HashSet<String>,
    retry_set: RetrySet,
    awaiting_broadcast_ok: HashMap<u32, BroadcastOkWaiter>,
}

impl BroadcastNode {
    fn rebroadcast_on_timeout(&mut self, socket: &mut Socket<impl Read, impl Write>) -> Result<()> {
        // TODO: Move this to a seperate thread.
        for (
            original_message_id,
            entry @ &mut BroadcastOkWaiter {
                message_id,
                last_sent,
                retries,
                message,
                ..
            },
        ) in self.awaiting_broadcast_ok.iter_mut()
        {
            if last_sent.elapsed() < BROADCAST_RETRY_TIMEOUT * 2u32.pow(retries) {
                continue;
            }

            // Resend the request.
            let new_message_id = self.id_gen.next_id();
            socket
                .send(
                    Message::new(
                        self.node_id.clone(),
                        entry.dest.clone(),
                        Request::Broadcast { message },
                    )
                    .with_id(message_id),
                )
                .context("re-broadcasting message to neighbour")?;
            entry.retry(new_message_id);
            self.retry_set.register(*original_message_id, message_id);
        }
        Ok(())
    }
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
        self.rebroadcast_on_timeout(&mut *socket)?;

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
                    self.awaiting_broadcast_ok.insert(
                        message_id,
                        BroadcastOkWaiter {
                            message_id,
                            last_sent: Instant::now(),
                            retries: 0,
                            dest: neighbour,
                            message,
                        },
                    );
                    self.retry_set.register(message_id, message_id);
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
            self.retry_set
                .remove_chain_for_message(in_reply_to)
                .inspect(|o| {
                    self.awaiting_broadcast_ok.remove(o);
                });
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
