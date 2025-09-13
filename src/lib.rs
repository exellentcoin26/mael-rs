use std::io::{Read, Write};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

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

#[derive(Deserialize)]
#[serde(untagged)]
enum RequestResponse<Req, Res> {
    Request(Req),
    Response(Response<Res>),
}

pub struct RequestInfo<'a> {
    pub src: &'a str,
}

pub trait Node: Sized {
    type Request: DeserializeOwned;
    type Response: serde::Serialize + DeserializeOwned;

    fn handle(
        &mut self,
        request: Self::Request,
        info: RequestInfo,
        socket: &mut Socket<impl Read, impl Write>,
    ) -> Result<Self::Response>;

    fn run(mut self, mut socket: Socket<impl Read, impl Write>) -> Result<()> {
        loop {
            let message = socket
                .receive::<RequestResponse<Self::Request, Self::Response>>()
                .context("receiving message from socket")?;

            match message.body.type_ {
                RequestResponse::Request(req) => {
                    let response = self
                        .handle(req, RequestInfo { src: &message.src }, &mut socket)
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

                    socket.send(response_message).context("sending response")?;
                }
                RequestResponse::Response(_res) => {
                    // Ignore for now.
                }
            }
        }
    }
}

pub struct Socket<I, O> {
    stdin: I,
    stdout: O,
}

impl<I, O> Socket<I, O> {
    pub fn new(stdin: I, stdout: O) -> Self {
        Self { stdin, stdout }
    }
}

impl<I, O> Socket<I, O>
where
    I: Read,
{
    pub fn receive<R>(&mut self) -> Result<Message<R>>
    where
        R: DeserializeOwned,
    {
        serde_json::Deserializer::from_reader(&mut self.stdin)
            .into_iter::<Message<R>>()
            .next()
            .context("waiting for message from stdin")?
            .context("reading message from stdin")
    }
}

impl<I, O> Socket<I, O>
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

impl<I, O> Socket<I, O>
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
        Ok(self.receive::<Response<Res>>()?.body.type_.kind)
    }
}
