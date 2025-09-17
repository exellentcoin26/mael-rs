use std::collections::HashSet;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex, mpsc};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub use self::id_gen::ID_GENERATOR;
pub use self::seq_kv::SeqKv;

pub mod id_gen;
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
                id: None,
                kind: body,
            },
        }
    }

    pub fn with_id(mut self, id: u32) -> Self {
        self.body.id = Some(id);
        self
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct MessageBody<T> {
    #[serde(rename = "msg_id")]
    id: Option<u32>,
    #[serde(flatten)]
    kind: T,
}

#[derive(Debug, Serialize, Deserialize)]
struct Response<R> {
    in_reply_to: Option<u32>,
    #[serde(flatten)]
    inner: R,
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

pub struct ResponseInfo {
    pub in_reply_to: Option<u32>,
}

enum Incoming<Req, Res, E> {
    Message(Message<RequestResponse<Req, Res>>),
    Event(E),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename = "init")]
pub struct Init {
    pub node_id: String,
    pub node_ids: HashSet<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename = "init_ok")]
struct InitOk {}

pub struct EventIncjector<Req, Res, E> {
    sender: mpsc::Sender<Incoming<Req, Res, E>>,
}

impl<Req, Res, E> Clone for EventIncjector<Req, Res, E> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<Req, Res, E> EventIncjector<Req, Res, E> {
    pub fn send(&mut self, event: E) {
        self.sender
            .send(Incoming::Event(event))
            .expect("failed to send event over channel")
    }
}

pub trait Node: Sized {
    type Request: DeserializeOwned + Send + 'static;
    type Response: serde::Serialize + DeserializeOwned + Send + 'static;
    type Event: Send + 'static;

    type InitState;

    fn from_init(
        init: Init,
        init_state: Self::InitState,
        event_injector: EventIncjector<Self::Request, Self::Response, Self::Event>,
    ) -> Self;

    fn handle_request(
        &mut self,
        request: Self::Request,
        info: RequestInfo,
        socket: &mut Socket<impl Read, impl Write>,
    ) -> Result<Self::Response>;

    fn handle_response(
        &mut self,
        response: Self::Response,
        info: ResponseInfo,
        socket: &mut Socket<impl Read, impl Write>,
    ) -> Result<()> {
        // By default receiving a response of any kind, does not matter.
        let _ = (response, info, socket);
        Ok(())
    }

    fn handle_event(
        &mut self,
        event: Self::Event,
        socket: &mut Socket<impl Read, impl Write>,
    ) -> Result<()> {
        // By default no event handling is enabled.
        let _ = (event, socket);
        Ok(())
    }

    fn run<I, O>(init_state: Self::InitState, mut socket: Socket<I, O>) -> Result<()>
    where
        I: Read,
        O: Write,
        Socket<I, O>: Send + 'static,
    {
        let (tx, rx) = mpsc::channel();

        let init = socket
            .receive::<Init>()
            .expect("first message to node should be init");
        socket
            .send(Message {
                src: init.dest,
                dest: init.src,
                body: MessageBody {
                    id: init.body.id,
                    kind: Response {
                        in_reply_to: init.body.id,
                        inner: InitOk {},
                    },
                },
            })
            .context("sending init ok")?;
        let mut this = Self::from_init(
            init.body.kind,
            init_state,
            EventIncjector { sender: tx.clone() },
        );

        {
            let socket_tx = tx.clone();
            let mut socket = socket.clone();
            std::thread::spawn(move || -> Result<()> {
                loop {
                    let message = socket
                        .receive::<RequestResponse<Self::Request, Self::Response>>()
                        .context("receiving message from socket")?;
                    socket_tx
                        .send(Incoming::Message(message))
                        .expect("failed to send incoming message from socket over channel");
                }
            })
        };

        loop {
            let incoming = rx.recv().expect("failed to receive message over channel");
            match incoming {
                Incoming::Message(message) => match message.body.kind {
                    RequestResponse::Request(req) => {
                        let response = this
                            .handle_request(req, RequestInfo { src: &message.src }, &mut socket)
                            .context("handling a request")?;

                        let response_message = Message {
                            src: message.dest,
                            dest: message.src,
                            body: MessageBody {
                                id: message.body.id,
                                kind: Response {
                                    in_reply_to: message.body.id,
                                    inner: response,
                                },
                            },
                        };

                        socket.send(response_message).context("sending response")?;
                    }
                    RequestResponse::Response(res) => {
                        this.handle_response(
                            res.inner,
                            ResponseInfo {
                                in_reply_to: res.in_reply_to,
                            },
                            &mut socket,
                        )
                        .context("handling a response")?;
                    }
                },
                Incoming::Event(event) => this
                    .handle_event(event, &mut socket)
                    .context("handling event")?,
            }
        }
    }
}

pub struct Socket<I, O> {
    stdin: Arc<Mutex<I>>,
    stdout: Arc<Mutex<O>>,
}

impl<I, O> Clone for Socket<I, O> {
    fn clone(&self) -> Self {
        Self {
            stdin: self.stdin.clone(),
            stdout: self.stdout.clone(),
        }
    }
}

impl<I, O> Socket<I, O> {
    pub fn new(stdin: I, stdout: O) -> Self {
        Self {
            stdin: Arc::new(Mutex::new(stdin)),
            stdout: Arc::new(Mutex::new(stdout)),
        }
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
        let mut stdin = self.stdin.lock().expect("failed to lock stdin");
        serde_json::Deserializer::from_reader(&mut *stdin)
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
        let mut stdout = self.stdout.lock().expect("failed to lock stdout");
        serde_json::to_writer(&mut *stdout, &message).context("writing message to stdout")?;
        stdout.write_all(b"\n").context("writing newline")?;
        stdout.flush().context("flushing stdout")?;
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
        Ok(self.receive::<Response<Res>>()?.body.kind.inner)
    }
}
