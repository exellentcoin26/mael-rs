use std::{
    collections::{BTreeSet, HashMap, HashSet},
    io::{Read, Write},
    sync::mpsc,
    thread::JoinHandle,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use mael::{ID_GENERATOR, Message, Node, RequestInfo, Socket};
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
    node_id: String,
    dest: String,
    message: u32,
}

struct BroadcastRequest {
    node_id: String,
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

enum BroadcastEvent {
    NewRequest(BroadcastRequest),
    OkReceived(u32),
}

struct BroadcastThread {
    _jh: JoinHandle<Result<()>>,
    channel: mpsc::Sender<BroadcastEvent>,
}

impl BroadcastThread {
    fn new<I, O>(mut socket: Socket<I, O>) -> Self
    where
        I: Read + Send + 'static,
        O: Write + Send + 'static,
    {
        let (tx, rx) = mpsc::channel::<BroadcastEvent>();
        let jh = std::thread::spawn(move || {
            let mut waiters = HashMap::new();
            let mut retry_set = RetrySet::default();
            loop {
                while let Ok(event) = rx.recv() {
                    match event {
                        BroadcastEvent::NewRequest(BroadcastRequest {
                            node_id,
                            dest,
                            message,
                        }) => {
                            let message_id = ID_GENERATOR.next_id();
                            socket
                                .send(
                                    Message::new(
                                        node_id.clone(),
                                        dest.clone(),
                                        Request::Broadcast { message },
                                    )
                                    .with_id(message_id),
                                )
                                .context("sending broadcast request")?;
                            waiters.insert(
                                message_id,
                                BroadcastOkWaiter {
                                    message_id,
                                    last_sent: Instant::now(),
                                    retries: 0,
                                    node_id,
                                    dest,
                                    message,
                                },
                            );
                        }
                        BroadcastEvent::OkReceived(message) => {
                            retry_set
                                .remove_chain_for_message(message)
                                .and_then(|o| waiters.remove(&o));
                        }
                    };
                }

                for (
                    original_message_id,
                    entry @ &mut BroadcastOkWaiter {
                        message_id,
                        last_sent,
                        retries,
                        message,
                        ..
                    },
                ) in waiters.iter_mut()
                {
                    if last_sent.elapsed() < BROADCAST_RETRY_TIMEOUT * 2u32.pow(retries) {
                        continue;
                    }

                    // Resend the request.
                    let new_message_id = ID_GENERATOR.next_id();
                    socket
                        .send(
                            Message::new(
                                entry.node_id.clone(),
                                entry.dest.clone(),
                                Request::Broadcast { message },
                            )
                            .with_id(message_id),
                        )
                        .context("re-broadcasting message to neighbour")?;
                    entry.retry(new_message_id);
                    retry_set.register(*original_message_id, message_id);
                }
            }
        });

        Self {
            _jh: jh,
            channel: tx,
        }
    }
}

struct BroadcastNode {
    node_id: String,
    messages: BTreeSet<u32>,
    neighbours: HashSet<String>,
    broadcast_thread: BroadcastThread,
}

impl BroadcastNode {
    fn new<I, O>(socket: Socket<I, O>) -> Self
    where
        I: Read + Send + 'static,
        O: Write + Send + 'static,
    {
        Self {
            node_id: String::new(),
            messages: BTreeSet::new(),
            neighbours: HashSet::new(),
            broadcast_thread: BroadcastThread::new(socket),
        }
    }
}

impl Node for BroadcastNode {
    type Request = Request;

    type Response = Response;

    fn handle_request(
        &mut self,
        request: Self::Request,
        info: RequestInfo,
        _socket: &mut Socket<impl Read, impl Write>,
    ) -> Result<Self::Response> {
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

                    self.broadcast_thread
                        .channel
                        .send(BroadcastEvent::NewRequest(BroadcastRequest {
                            node_id: self.node_id.clone(),
                            dest: neighbour,
                            message,
                        }))
                        .context("registering new broadcast event")?;
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
            self.broadcast_thread
                .channel
                .send(BroadcastEvent::OkReceived(in_reply_to))
                .context("registering broadcast ok received event")?;
        }
        Ok(())
    }
}

fn main() -> Result<()> {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let socket = Socket::new(stdin, stdout);

    BroadcastNode::new(socket.clone()).run(socket)
}
