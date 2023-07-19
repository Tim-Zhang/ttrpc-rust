use sendfd::{RecvWithFd, SendWithFd};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};

use super::*;
use crate::error::{Error, Result as TtResult};

impl GenMessage {
    /// Encodes a MessageHeader to writer.
    pub async fn write_to(&self, mut writer: &mut OwnedWriteHalf) -> TtResult<()> {
        self.header
            .write_to(&mut writer)
            .await
            .map_err(|e| Error::Socket(e.to_string()))?;

        writer
            .as_ref()
            .send_with_fd(&self.payload, &self.fds)
            .map_err(|e| Error::Socket(e.to_string()))?;

        Ok(())
    }

    /// Decodes a MessageHeader from reader.
    pub async fn read_from(
        reader: &mut OwnedReadHalf,
    ) -> std::result::Result<Self, GenMessageError> {
        let header = MessageHeader::read_from(&mut *reader)
            .await
            .map_err(|e| Error::Socket(e.to_string()))?;

        let fds_len = (header.length >> 24) as usize;
        let length = (header.length & 0x00ffffff) as usize;

        if let Err(e) = check_oversize(length, true) {
            discard_message_body(reader, &header).await?;
            return Err(GenMessageError::ReturnError(header, e));
        }

        let mut fds = vec![-1; fds_len];
        let mut content = vec![0; length as usize];

        let mut r_len = 0;
        let mut r_fds_len = 0;

        loop {
            if length == 0 && fds_len == 0 {
                break;
            }

            let (l, fl) = reader
                .as_ref()
                .recv_with_fd(&mut content[r_len..], &mut fds[r_fds_len..])
                .map_err(|e| Error::Socket(e.to_string()))?;

            r_len += l;
            r_fds_len += fl;

            if r_len == 0 && r_fds_len == 0 {
                return Err(GenMessageError::InternalError(Error::Socket(
                    "socket peer closed".to_string(),
                )));
            }

            if r_len == length && r_fds_len == fds_len {
                break;
            }
        }

        Ok(Self {
            header,
            payload: content,
            fds,
        })
    }
}
