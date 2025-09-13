use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    io::{Read, Write},
};

use anyhow::Result;
use mael::{Node, RequestInfo, Socket};
use serde::{Deserialize, Serialize};

#[derive(Default)]
struct Log {
    messages: Vec<u32>,
    commit_offset: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde[tag = "type", rename_all = "snake_case"]]
enum Request {
    Init {
        node_id: String,
        node_ids: HashSet<String>,
    },
    Send {
        #[serde(rename = "key")]
        log: String,
        #[serde(rename = "msg")]
        message: u32,
    },
    Poll {
        offsets: BTreeMap<String, usize>,
    },
    CommitOffsets {
        offsets: BTreeMap<String, usize>,
    },
    ListCommittedOffsets {
        #[serde(rename = "keys")]
        logs: BTreeSet<String>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(clippy::enum_variant_names)]
enum Response {
    InitOk,
    SendOk {
        offset: usize,
    },
    PollOk {
        #[serde(rename = "msgs")]
        messages: BTreeMap<String, Vec<(usize, u32)>>,
    },
    CommitOffsetsOk,
    ListCommittedOffsetsOk {
        offsets: BTreeMap<String, usize>,
    },
}

#[derive(Default)]
struct KafkaNode {
    logs: HashMap<String, Log>,
}

impl Node for KafkaNode {
    type Request = Request;

    type Response = Response;

    fn handle_request(
        &mut self,
        request: Self::Request,
        _: RequestInfo,
        _: &mut Socket<impl Read, impl Write>,
    ) -> Result<Self::Response> {
        Ok(match request {
            Request::Init { .. } => Response::InitOk,
            Request::Send { log, message } => {
                let log = self.logs.entry(log).or_default();
                log.messages.push(message);
                Response::SendOk {
                    offset: log.messages.len() - 1,
                }
            }
            Request::Poll { offsets } => Response::PollOk {
                messages: offsets
                    .into_iter()
                    .map(|(log, offset)| {
                        let messages = self
                            .logs
                            .entry(log.clone())
                            .or_default()
                            .messages
                            .iter()
                            .copied()
                            .enumerate()
                            .skip(offset)
                            .collect::<Vec<_>>();
                        (log, messages)
                    })
                    .collect(),
            },
            Request::CommitOffsets { offsets } => {
                offsets.into_iter().for_each(|(log, offset)| {
                    self.logs.entry(log).or_default().commit_offset = offset
                });
                Response::CommitOffsetsOk
            }
            Request::ListCommittedOffsets { logs } => Response::ListCommittedOffsetsOk {
                offsets: logs
                    .into_iter()
                    .map(|log| (log.clone(), self.logs.entry(log).or_default().commit_offset))
                    .collect(),
            },
        })
    }
}

fn main() -> Result<()> {
    let stdin = std::io::stdin().lock();
    let stdout = std::io::stdout().lock();
    let socket = Socket::new(stdin, stdout);

    KafkaNode::default().run(socket)
}
