use std::io::{Read, Write};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

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
            body: MessageBody { id: 1, type_: body },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct MessageBody<T> {
    #[serde(rename = "msg_id")]
    id: u32,
    #[serde(flatten)]
    type_: T,
}

#[derive(Debug, Serialize, Deserialize)]
struct Response<R> {
    in_reply_to: u32,
    #[serde(flatten)]
    kind: R,
}

pub trait Node: Sized {
    type Request: for<'de> serde::Deserialize<'de>;
    type Response: serde::Serialize;

    fn handle(
        &mut self,
        request: Self::Request,
        sender: Sender<impl Write>,
    ) -> Result<Self::Response>;

    fn run(mut self, stdin: impl Read, mut stdout: impl Write) -> Result<()> {
        for message in
            serde_json::Deserializer::from_reader(stdin).into_iter::<Message<Self::Request>>()
        {
            let message = message.context("reading message from stdin")?;

            let response = self
                .handle(message.body.type_, Sender::new(&mut stdout))
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

pub struct Sender<O> {
    stdout: O,
}

impl<O> Sender<O> {
    fn new(stdout: O) -> Self {
        Self { stdout }
    }
}

impl<O> Sender<O>
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
