use std::{
    collections::HashSet,
    io::{Read, Write},
};

use anyhow::{Context, Result};
use mael::{Node, Sender, SeqKv};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde[tag = "type", rename_all = "snake_case"]]
enum Request {
    Init {
        node_id: String,
        node_ids: HashSet<String>,
    },
    Add {
        delta: u32,
    },
    Read,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(clippy::enum_variant_names)]
enum Response {
    InitOk,
    AddOk,
    ReadOk { value: u32 },
}

#[derive(Default)]
struct CountingNode {
    id: String,
}

impl Node for CountingNode {
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
            Request::Read => {
                let value = SeqKv
                    .read(self.id.clone(), "counter".to_string(), &mut sender)
                    .context("reading counter from key-value store")?
                    .unwrap_or_else(|| "0".to_string())
                    .parse()
                    .context("parsing value into u32")?;
                Response::ReadOk { value }
            }
            Request::Add { delta } => {
                loop {
                    use mael::seq_kv::CasResponse;

                    let value = SeqKv
                        .read(self.id.clone(), "counter".to_string(), &mut sender)
                        .context("reading counter from key-value store")?
                        .unwrap_or_else(|| "0".to_string());
                    let result = SeqKv
                        .compare_and_set(
                            self.id.clone(),
                            "counter".to_string(),
                            value.clone(),
                            format!(
                                "{}",
                                value.parse::<u32>().context("parsing value as u32")? + delta
                            ),
                            &mut sender,
                        )
                        .context("setting a new counter in the key-value store")?;
                    match result {
                        CasResponse::Ok => break,
                        CasResponse::Retry => {
                            // The cas operation failed as the value was not the same anymore as
                            // the expected value. Retry the operation.
                            continue;
                        }
                    }
                }
                Response::AddOk
            }
        })
    }
}

fn main() -> Result<()> {
    let stdin = std::io::stdin().lock();
    let stdout = std::io::stdout().lock();

    CountingNode::default().run(stdin, stdout)
}
