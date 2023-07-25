// Copyright 2022 Alibaba Cloud. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

mod protocols;
mod utils;

use std::{
    os::fd::{AsRawFd, FromRawFd},
    os::unix::net::UnixStream as StdUnixStream,
    sync::Arc,
};

use log::{info, LevelFilter};
use tokio::{io::AsyncWriteExt, net::UnixStream};

#[cfg(unix)]
use protocols::r#async::{streaming, streaming_ttrpc};
#[cfg(unix)]
use ttrpc::asynchronous::Server;

#[cfg(unix)]
use async_trait::async_trait;
#[cfg(unix)]
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::sleep;

struct StreamingService;

const EXPECTED_NUM_SERVER_SEND: u8 = 233;
const EXPECTED_NUM_SERVER_SEND2: u8 = 66;

#[cfg(unix)]
#[async_trait]
impl streaming_ttrpc::Streaming for StreamingService {
    async fn echo_stream(
        &self,
        _ctx: &::ttrpc::r#async::TtrpcContext,
        mut s: ::ttrpc::r#async::ServerStream<streaming::EchoPayload, streaming::EchoPayload>,
    ) -> ::ttrpc::Result<()> {
        let (mut l, r) = UnixStream::pair().expect("create UnixDatagram pair");
        if let (Some(mut e), client_fds) = s.recv_with_fd().await? {
            e.seq += 1;
            s.send_with_fd(&e, vec![r.as_raw_fd()]).await?;

            // client side fd write
            let tmp = unsafe { StdUnixStream::from_raw_fd(client_fds[0]) };
            let mut client_sent_stream = UnixStream::from_std(tmp).unwrap();
            client_sent_stream
                .write_u8(EXPECTED_NUM_SERVER_SEND)
                .await
                .unwrap();

            // server side fd write
            l.write_u8(EXPECTED_NUM_SERVER_SEND2).await.unwrap();
        }

        // wait client close
        s.recv().await?;

        Ok(())
    }
}

#[cfg(windows)]
fn main() {
    println!("This example only works on Unix-like OSes");
}

#[cfg(unix)]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    simple_logging::log_to_stderr(LevelFilter::Info);

    let s = Box::new(StreamingService {}) as Box<dyn streaming_ttrpc::Streaming + Send + Sync>;
    let s = Arc::new(s);
    let service = streaming_ttrpc::create_streaming(s);

    utils::remove_if_sock_exist(utils::SOCK_ADDR).unwrap();

    let mut server = Server::new()
        .bind(utils::SOCK_ADDR)
        .unwrap()
        .register_service(service);

    let mut hangup = signal(SignalKind::hangup()).unwrap();
    let mut interrupt = signal(SignalKind::interrupt()).unwrap();
    server.start().await.unwrap();

    tokio::select! {
        _ = hangup.recv() => {
            // test stop_listen -> start
            info!("stop listen");
            server.stop_listen().await;
            info!("start listen");
            server.start().await.unwrap();

            // hold some time for the new test connection.
            sleep(std::time::Duration::from_secs(100)).await;
        }
        _ = interrupt.recv() => {
            // test graceful shutdown
            info!("graceful shutdown");
            server.shutdown().await.unwrap();
        }
    };
}
