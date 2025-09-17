use std::{
    collections::{BTreeSet, HashMap, HashSet},
    io::{Read, Write},
    thread::JoinHandle,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use mael::{EventIncjector, ID_GENERATOR, Message, Node, RequestInfo, ResponseInfo, Socket};
use serde::{Deserialize, Serialize};

const GOSSIP_INTERVAL: Duration = Duration::from_millis(50);
const GOSSIP_NEIGHBOUR_COUNT: usize = 2;

#[derive(Debug, Serialize, Deserialize)]
#[serde[tag = "type", rename_all = "snake_case"]]
enum Request {
    Broadcast {
        message: u32,
    },
    Read,
    Topology {
        topology: HashMap<String, HashSet<String>>,
    },
    Gossip {
        messages: BTreeSet<u32>,
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
    GossipOk,
}

enum Event {
    StartGossip,
}

struct BroadcastNode {
    node_id: String,
    messages: BTreeSet<u32>,
    neighbours: HashSet<String>,
    neighbour_known: HashMap<String, BTreeSet<u32>>,
    sent_to_neighbour: HashMap<u32, (String, BTreeSet<u32>)>,
    _gossip_thread: GossipThread,
}

impl Node for BroadcastNode {
    type Request = Request;
    type Response = Response;
    type Event = Event;

    type InitState = ();

    fn from_init(
        init: mael::Init,
        _init_state: Self::InitState,
        event_injector: EventIncjector<Self::Request, Self::Response, Self::Event>,
    ) -> Self {
        Self {
            node_id: init.node_id,
            messages: BTreeSet::new(),
            neighbours: init.node_ids,
            neighbour_known: HashMap::new(),
            sent_to_neighbour: HashMap::new(),
            _gossip_thread: GossipThread::new(event_injector),
        }
    }

    fn handle_request(
        &mut self,
        request: Self::Request,
        _info: RequestInfo,
        _socket: &mut Socket<impl Read, impl Write>,
    ) -> Result<Self::Response> {
        Ok(match request {
            Request::Broadcast { message } => {
                self.messages.insert(message);
                Response::BroadcastOk
            }
            Request::Read => Response::ReadOk {
                messages: self.messages.clone(),
            },
            Request::Topology { .. } => {
                // self.neighbours = topology.remove(&self.node_id).unwrap_or_default();
                Response::TopologyOk
            }
            Request::Gossip { messages } => {
                self.messages.extend(messages);
                Response::GossipOk
            }
        })
    }

    fn handle_response(
        &mut self,
        response: Self::Response,
        info: ResponseInfo,
        _socket: &mut Socket<impl Read, impl Write>,
    ) -> Result<()> {
        if let Response::GossipOk = response {
            let Some(ref in_reply_to) = info.in_reply_to else {
                bail!("gossip ok received without in-reply-to field");
            };
            let Some((neighbour, messages)) = self.sent_to_neighbour.remove(in_reply_to) else {
                bail!("gossip ok received to a message that is not known to be sent");
            };
            self.neighbour_known
                .entry(neighbour)
                .or_default()
                .extend(messages);
        }
        Ok(())
    }

    fn handle_event(
        &mut self,
        event: Self::Event,
        socket: &mut Socket<impl Read, impl Write>,
    ) -> Result<()> {
        match event {
            Event::StartGossip => {
                // 1. Decide the neighbours to send to.
                //    - Everyone?
                //    - Random?
                //    - Topology?
                // 2. What data to send.
                //    - Keep list of neighbour-known values?
                //    - Random?

                use rand::seq::IteratorRandom;

                for neighbour in self
                    .neighbours
                    .iter()
                    .choose_multiple(&mut rand::rng(), GOSSIP_NEIGHBOUR_COUNT)
                {
                    let messages: BTreeSet<u32> = self
                        .messages
                        .difference(self.neighbour_known.entry(neighbour.clone()).or_default())
                        .copied()
                        .collect();

                    if messages.is_empty() {
                        continue;
                    }

                    let message_id = ID_GENERATOR.next_id();
                    socket
                        .send(
                            Message::new(
                                self.node_id.clone(),
                                neighbour.clone(),
                                Request::Gossip {
                                    messages: messages.clone(),
                                },
                            )
                            .with_id(message_id),
                        )
                        .context("gossiping messages to neightbour")?;
                    self.sent_to_neighbour
                        .entry(message_id)
                        .or_insert_with(|| (neighbour.clone(), messages));
                }
            }
        }
        Ok(())
    }
}

struct GossipThread {
    _jh: JoinHandle<Result<()>>,
}

impl GossipThread {
    fn new<Req, Res>(mut event_injector: EventIncjector<Req, Res, Event>) -> Self
    where
        EventIncjector<Req, Res, Event>: Send + 'static,
    {
        let _jh = std::thread::spawn(move || {
            loop {
                event_injector.send(Event::StartGossip);
                std::thread::sleep(GOSSIP_INTERVAL);
            }
        });

        Self { _jh }
    }
}

fn main() -> Result<()> {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let socket = Socket::new(stdin, stdout);

    BroadcastNode::run((), socket)
}
