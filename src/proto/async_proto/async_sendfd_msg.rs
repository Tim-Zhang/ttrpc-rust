use std::os::unix::io::RawFd;

use super::*;
use crate::error::{Error, Result as TtResult};
use sendfd::{RecvWithFd, SendWithFd};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::unix::{OwnedReadHalf, OwnedWriteHalf},
};

impl GenMessage {
    /// Encodes a MessageHeader to writer.
    pub async fn write_to(&self, mut writer: &mut OwnedWriteHalf) -> TtResult<()> {
        self.header
            .write_to(&mut writer)
            .await
            .map_err(|e| Error::Socket(e.to_string()))?;

        let fds_len = (self.header.length >> 24) as usize;

        if fds_len == 0 {
            writer
                .write_all(&self.payload)
                .await
                .map_err(|e| Error::Socket(e.to_string()))?;
        } else {
            write_to_with_fd(writer, &self.payload, &self.fds)?;
        }

        Ok(())
    }

    /// Decodes a MessageHeader from reader.
    pub async fn read_from(
        reader: &mut OwnedReadHalf,
    ) -> std::result::Result<Self, GenMessageError> {
        let mut header = MessageHeader::read_from(&mut *reader)
            .await
            .map_err(|e| Error::Socket(e.to_string()))?;

        let fds_len = (header.length >> 24) as usize;
        header.length = header.length & 0x00ffffff;

        if let Err(e) = check_oversize(header.length as usize, true) {
            discard_message_body(reader, &header).await?;
            return Err(GenMessageError::ReturnError(header, e));
        }

        let (payload, fds) = if fds_len == 0 {
            let mut content = vec![0; header.length as usize];
            reader
                .read_exact(&mut content)
                .await
                .map_err(|e| Error::Socket(e.to_string()))?;
            (content, Vec::new())
        } else {
            read_from_with_fd(reader, header.length as usize, fds_len).await?
        };

        Ok(Self {
            header,
            payload,
            fds,
        })
    }
}

async fn read_from_with_fd(
    reader: &mut OwnedReadHalf,
    cont_len: usize,
    fds_len: usize,
) -> std::result::Result<(Vec<u8>, Vec<RawFd>), GenMessageError> {
    let mut fds = vec![-1; fds_len];
    let mut content = vec![0; cont_len];

    let mut r_len = 0;
    let mut r_fds_len = 0;

    loop {
        if cont_len == 0 && fds_len == 0 {
            break;
        }

        let (l, fl) = reader
            .as_ref()
            .recv_with_fd(&mut content[r_len..], &mut fds[r_fds_len..])
            .map_err(|e| Error::Socket(e.to_string()))?;

        r_len += l;
        r_fds_len += fl;

        if r_len == 0 && r_fds_len == 0 {
            return Err(Error::Socket("socket peer closed".to_string()).into());
        }

        if r_len == cont_len as usize && r_fds_len == fds_len {
            break;
        }
    }

    return Ok((content, fds));
}

fn write_to_with_fd(
    writer: &mut OwnedWriteHalf,
    payload: &Vec<u8>,
    fds: &Vec<RawFd>,
) -> TtResult<()> {
    writer
        .as_ref()
        .send_with_fd(payload, fds)
        .map_err(|e| Error::Socket(e.to_string()))?;

    return Ok(());
}
