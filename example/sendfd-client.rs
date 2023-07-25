// Copyright 2022 Alibaba Cloud. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

mod protocols;
mod utils;

#[cfg(unix)]
use protocols::r#async::{streaming, streaming_ttrpc};
use std::os::{
    fd::{AsRawFd, FromRawFd},
    unix::net::UnixStream as StdUnixStream,
};
use tokio::{io::AsyncReadExt, net::UnixStream};
use ttrpc::context::{self, Context};
#[cfg(unix)]
use ttrpc::r#async::Client;

#[cfg(windows)]
fn main() {
    println!("This example only works on Unix-like OSes");
}

#[cfg(unix)]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    simple_logging::log_to_stderr(log::LevelFilter::Info);

    let c = Client::connect(utils::SOCK_ADDR).unwrap();
    let sc = streaming_ttrpc::StreamingClient::new(c);

    let t2 = tokio::spawn(echo_stream(sc));

    t2.await.ok();
}

fn default_ctx() -> Context {
    let mut ctx = context::with_timeout(0);
    ctx.add("key-1".to_string(), "value-1-1".to_string());
    ctx.add("key-1".to_string(), "value-1-2".to_string());
    ctx.set("key-2".to_string(), vec!["value-2".to_string()]);

    ctx
}

const EXPECTED_NUM_SERVER_SEND: u8 = 233;
const EXPECTED_NUM_SERVER_SEND2: u8 = 66;

#[cfg(unix)]
async fn echo_stream(cli: streaming_ttrpc::StreamingClient) {
    let (mut l, r) = UnixStream::pair().expect("create UnixDatagram pair");
    let mut stream = cli.echo_stream(default_ctx()).await.unwrap();
    let i = 0;

    let echo = streaming::EchoPayload {
        seq: i as u32,
        msg: format!("{}: Echo in a stream", i),
        ..Default::default()
    };
    stream
        .send_with_fd(&echo, vec![r.as_raw_fd()])
        .await
        .unwrap();
    let (resp, server_fds) = stream.recv_with_fd().await.unwrap();

    // client side fd read
    let server_sent_num = l.read_u8().await.unwrap();
    assert_eq!(server_sent_num, EXPECTED_NUM_SERVER_SEND);

    // server side fd read
    let tmp = unsafe { StdUnixStream::from_raw_fd(server_fds[0]) };
    let mut server_sent_stream = UnixStream::from_std(tmp).unwrap();
    let server_sent_num2 = server_sent_stream.read_u8().await.unwrap();
    assert_eq!(server_sent_num2, EXPECTED_NUM_SERVER_SEND2);

    assert_eq!(resp.msg, echo.msg);
    assert_eq!(resp.seq, echo.seq + 1);

    stream.close_send().await.unwrap();
    let ret = stream.recv().await;
    assert!(matches!(ret, Err(ttrpc::Error::Eof)));

    println!("============ all done ==============");
}
