use std::{os::unix::io::RawFd, sync::atomic::Ordering};

use super::*;
use crate::{
    error::{get_rpc_status, Error, Result},
    proto::{Code, GenMessage, MessageHeader},
};

impl StreamSender {
    pub async fn send_with_fd(&self, buf: Vec<u8>, fds: Vec<RawFd>) -> Result<()> {
        debug_assert!(self.sendable);
        if self.local_closed.load(Ordering::Relaxed) {
            debug_assert_eq!(self.kind, Kind::Client);
            return Err(Error::LocalClosed);
        }

        if fds.len() > 255 {
            return Err(get_rpc_status(
                Code::INVALID_ARGUMENT,
                "The length of fds can't be greater than 255",
            ));
        }

        let mut header = MessageHeader::new_data(self.stream_id, buf.len() as u32);
        header.length = header.length | (fds.len() << 24) as u32;

        let msg = GenMessage {
            header,
            payload: buf,
            fds,
        };

        _send(&self.tx, msg).await?;

        Ok(())
    }
}

impl StreamReceiver {
    pub async fn recv_with_fd(&mut self) -> Result<(Vec<u8>, Vec<RawFd>)> {
        if self.remote_closed {
            return Err(Error::RemoteClosed);
        }
        let msg = _recv(&mut self.rx).await?;

        let payload = match msg.header.type_ {
            MESSAGE_TYPE_RESPONSE => {
                debug_assert_eq!(self.kind, Kind::Client);
                self.remote_closed = true;
                let resp = Response::decode(&msg.payload)
                    .map_err(err_to_others_err!(e, "Decode message failed."))?;
                if let Some(status) = resp.status.as_ref() {
                    if status.code() != Code::OK {
                        return Err(Error::RpcStatus((*status).clone()));
                    }
                }
                resp.payload
            }
            MESSAGE_TYPE_DATA => {
                if !self.recveivable {
                    self.remote_closed = true;
                    return Err(Error::Others(
                        "received data from non-streaming server.".to_string(),
                    ));
                }
                if (msg.header.flags & FLAG_REMOTE_CLOSED) == FLAG_REMOTE_CLOSED {
                    self.remote_closed = true;
                    if (msg.header.flags & FLAG_NO_DATA) == FLAG_NO_DATA {
                        return Err(Error::Eof);
                    }
                }
                msg.payload
            }
            _ => {
                return Err(Error::Others("not support".to_string()));
            }
        };
        Ok((payload, msg.fds))
    }
}

impl<Q, P> ClientStreamSender<Q, P>
where
    Q: Codec,
    P: Codec,
    <Q as Codec>::E: std::fmt::Display,
    <P as Codec>::E: std::fmt::Display,
{
    pub async fn send_with_fd(&self, req: &Q, fds: Vec<RawFd>) -> Result<()> {
        let msg_buf = req
            .encode()
            .map_err(err_to_others_err!(e, "Encode message failed."))?;
        self.inner.send_with_fd(msg_buf, fds).await
    }
}

impl StreamInner {
    pub async fn send_with_fd(&self, buf: Vec<u8>, fds: Vec<RawFd>) -> Result<()> {
        self.sender.send_with_fd(buf, fds).await
    }

    pub async fn recv_with_fd(&mut self) -> Result<(Vec<u8>, Vec<RawFd>)> {
        self.receiver.recv_with_fd().await
    }
}

impl<P> SSSender<P>
where
    P: Codec,
    <P as Codec>::E: std::fmt::Display,
{
    pub async fn send_with_fd(&self, resp: &P, fds: Vec<RawFd>) -> Result<()> {
        let msg_buf = resp
            .encode()
            .map_err(err_to_others_err!(e, "Encode message failed."))?;
        self.tx.send_with_fd(msg_buf, fds).await
    }
}

impl<Q> SSReceiver<Q>
where
    Q: Codec,
    <Q as Codec>::E: std::fmt::Display,
{
    pub async fn recv_with_fd(&mut self) -> Result<(Option<Q>, Vec<RawFd>)> {
        let res = self.rx.recv_with_fd().await;

        if matches!(res, Err(Error::Eof)) {
            return Ok((None, Default::default()));
        }
        let (msg_buf, fds) = res?;
        let buf = Q::decode(msg_buf)
            .map_err(err_to_others_err!(e, "Decode message failed."))
            .map(Some)?;

        Ok((buf, fds))
    }
}

impl<P, Q> ServerStream<P, Q>
where
    P: Codec,
    Q: Codec,
    <P as Codec>::E: std::fmt::Display,
    <Q as Codec>::E: std::fmt::Display,
{
    pub async fn send_with_fd(&self, resp: &P, fds: Vec<RawFd>) -> Result<()> {
        self.tx.send_with_fd(resp, fds).await
    }

    pub async fn recv_with_fd(&mut self) -> Result<(Option<Q>, Vec<RawFd>)> {
        self.rx.recv_with_fd().await
    }
}

impl<Q> CSSender<Q>
where
    Q: Codec,
    <Q as Codec>::E: std::fmt::Display,
{
    pub async fn send_with_fd(&self, req: &Q, fds: Vec<RawFd>) -> Result<()> {
        let msg_buf = req
            .encode()
            .map_err(err_to_others_err!(e, "Encode message failed."))?;
        self.tx.send_with_fd(msg_buf, fds).await
    }
}

impl<P> CSReceiver<P>
where
    P: Codec,
    <P as Codec>::E: std::fmt::Display,
{
    pub async fn recv_with_fd(&mut self) -> Result<(P, Vec<RawFd>)> {
        let (msg_buf, fds) = self.rx.recv_with_fd().await?;
        let buf = P::decode(msg_buf).map_err(err_to_others_err!(e, "Decode message failed."))?;

        Ok((buf, fds))
    }
}

impl<Q, P> ClientStream<Q, P>
where
    Q: Codec,
    P: Codec,
    <Q as Codec>::E: std::fmt::Display,
    <P as Codec>::E: std::fmt::Display,
{
    pub async fn send_with_fd(&self, req: &Q, fds: Vec<RawFd>) -> Result<()> {
        self.tx.send_with_fd(req, fds).await
    }

    pub async fn recv_with_fd(&mut self) -> Result<(P, Vec<RawFd>)> {
        self.rx.recv_with_fd().await
    }
}
