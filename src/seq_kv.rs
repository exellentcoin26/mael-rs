use std::io::{Read, Write};

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use crate::{Message, Socket};

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Request {
    Read {
        key: String,
    },
    Write {
        key: String,
        value: String,
    },
    Cas {
        key: String,
        from: String,
        to: String,
        create_if_not_exists: bool,
    },
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(clippy::enum_variant_names)]
enum Response {
    ReadOk { value: String },
    WriteOk,
    CasOk,
    Error { code: u32 },
}

#[derive(Deserialize)]
#[serde(try_from = "Response")]
struct ReadResponse {
    value: Option<String>,
}

impl TryFrom<Response> for ReadResponse {
    type Error = anyhow::Error;

    fn try_from(value: Response) -> Result<Self> {
        Ok(match value {
            Response::ReadOk { value } => Self { value: Some(value) },
            Response::Error { code: 20 } => Self { value: None },
            _ => bail!("incorrect response received"),
        })
    }
}

#[derive(Deserialize)]
#[serde(try_from = "Response")]
struct WriteResponse;

impl TryFrom<Response> for WriteResponse {
    type Error = anyhow::Error;

    fn try_from(value: Response) -> Result<Self> {
        Ok(match value {
            Response::WriteOk => Self,
            _ => bail!("incorrect response received"),
        })
    }
}

#[derive(Deserialize)]
#[serde(try_from = "Response")]
pub enum CasResponse {
    Ok,
    Retry,
}

impl TryFrom<Response> for CasResponse {
    type Error = anyhow::Error;

    fn try_from(value: Response) -> Result<Self> {
        Ok(match value {
            Response::CasOk => Self::Ok,
            Response::Error { code: 22 } => Self::Retry,
            _ => bail!("incorrect response received"),
        })
    }
}

pub struct SeqKv;

impl SeqKv {
    pub fn read<I, O>(
        self,
        src: String,
        key: String,
        sender: &mut Socket<I, O>,
    ) -> Result<Option<String>>
    where
        I: Read,
        O: Write,
    {
        sender
            .send_and_receive::<_, ReadResponse>(Message::new(
                src,
                "seq-kv".to_string(),
                Request::Read { key },
            ))
            .map(|r| r.value)
    }

    pub fn write<I, O>(
        self,
        src: String,
        key: String,
        value: String,
        sender: &mut Socket<I, O>,
    ) -> Result<()>
    where
        I: Read,
        O: Write,
    {
        sender.send_and_receive::<_, WriteResponse>(Message::new(
            src,
            "seq-kv".to_string(),
            Request::Write { key, value },
        ))?;
        Ok(())
    }

    pub fn compare_and_set<I, O>(
        self,
        src: String,
        key: String,
        from: String,
        to: String,
        sender: &mut Socket<I, O>,
    ) -> Result<CasResponse>
    where
        I: Read,
        O: Write,
    {
        sender.send_and_receive::<_, CasResponse>(Message::new(
            src,
            "seq-kv".to_string(),
            Request::Cas {
                key,
                from,
                to,
                create_if_not_exists: true,
            },
        ))
    }
}
