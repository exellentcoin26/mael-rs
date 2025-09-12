use std::io::{Read, Write};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

pub use self::seq_kv::SeqKv;

pub mod seq_kv;

#[derive(Debug, Serialize, Deserialize)]
pub struct Message<T> {
    src: String,
    dest: String,
    body: MessageBody<T>,
}

impl<T> Message<T> {
    pub fn new(src: String, dest: String, body: T) -> Self {
        Self {
            src,
            dest,
            body: MessageBody {
                id: Some(1),
                type_: body,
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct MessageBody<T> {
    #[serde(rename = "msg_id")]
    id: Option<u32>,
    #[serde(flatten)]
    type_: T,
}

#[derive(Debug, Serialize, Deserialize)]
struct Response<R> {
    in_reply_to: Option<u32>,
    #[serde(flatten)]
    kind: R,
}

pub trait Node: Sized {
    type Request: for<'de> serde::Deserialize<'de>;
    type Response: serde::Serialize;

    fn handle(
        &mut self,
        request: Self::Request,
        sender: Sender<impl Read, impl Write>,
    ) -> Result<Self::Response>;

    fn run(mut self, mut stdin: impl Read, mut stdout: impl Write) -> Result<()> {
        while let Some(message) = serde_json::Deserializer::from_reader(&mut stdin)
            .into_iter::<Message<Self::Request>>()
            .next()
        {
            let message = message.context("reading message from stdin")?;

            let response = self
                .handle(message.body.type_, Sender::new(&mut stdin, &mut stdout))
                .context("handling a request")?;

            let response_message = Message {
                src: message.dest,
                dest: message.src,
                body: MessageBody {
                    id: message.body.id,
                    type_: Response {
                        in_reply_to: message.body.id,
                        kind: response,
                    },
                },
            };

            serde_json::to_writer(&mut stdout, &response_message)
                .context("converting message response to json")?;
            stdout.write_all(b"\n").context("writing newline")?;
            stdout.flush().context("flushing")?;
        }
        Ok(())
    }
}

pub struct Sender<I, O> {
    stdin: I,
    stdout: O,
}

impl<I, O> Sender<I, O> {
    fn new(stdin: I, stdout: O) -> Self {
        Self { stdin, stdout }
    }
}

impl<I, O> Sender<I, O>
where
    O: Write,
{
    pub fn send<R>(&mut self, message: Message<R>) -> Result<()>
    where
        R: serde::Serialize,
    {
        serde_json::to_writer(&mut self.stdout, &message).context("writing message to stdout")?;
        self.stdout.write_all(b"\n").context("writing newline")?;
        self.stdout.flush().context("flushing stdout")?;
        Ok(())
    }
}

impl<I, O> Sender<I, O>
where
    I: Read,
{
    pub fn receive<R>(&mut self) -> Result<R>
    where
        R: for<'de> Deserialize<'de>,
    {
        Ok(serde_json::Deserializer::from_reader(&mut self.stdin)
            .into_iter::<Message<Response<R>>>()
            .next()
            .context("waiting for message from stdin")?
            .context("reading message from stdin")?
            .body
            .type_
            .kind)
    }
}

impl<I, O> Sender<I, O>
where
    I: Read,
    O: Write,
{
    pub fn send_and_receive<Req, Res>(&mut self, message: Message<Req>) -> Result<Res>
    where
        Req: serde::Serialize,
        Res: for<'de> serde::Deserialize<'de>,
    {
        self.send(message).context("sending message")?;
        self.receive()
    }
}
