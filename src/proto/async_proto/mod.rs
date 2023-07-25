cfg_send_fd! {
    mod async_sendfd_msg;
    #[cfg(test)]
    mod async_normal_msg;
}

cfg_not_send_fd! {
    mod async_normal_msg;
    #[cfg(test)]
    mod async_sendfd_msg;
}

use super::common_proto::*;
use crate::error::{Error, Result as TtResult};

impl GenMessage {
    pub fn check(&self) -> TtResult<()> {
        check_oversize(self.header.length as usize, true)
    }
}

// Discard the unwanted message body
pub async fn discard_message_body(
    mut reader: impl tokio::io::AsyncReadExt + Unpin,
    header: &MessageHeader,
) -> TtResult<()> {
    let mut need_discard = header.length as usize;

    while need_discard > 0 {
        let once_discard = std::cmp::min(DEFAULT_PAGE_SIZE, need_discard);
        let mut content = vec![0; once_discard];
        reader
            .read_exact(&mut content)
            .await
            .map_err(|e| Error::Socket(e.to_string()))?;
        need_discard -= once_discard;
    }

    Ok(())
}

impl MessageHeader {
    /// Encodes a MessageHeader to writer.
    pub async fn write_to(
        &self,
        mut writer: impl tokio::io::AsyncWriteExt + Unpin,
    ) -> std::io::Result<()> {
        writer.write_u32(self.length).await?;
        writer.write_u32(self.stream_id).await?;
        writer.write_u8(self.type_).await?;
        writer.write_u8(self.flags).await?;
        writer.flush().await
    }

    /// Decodes a MessageHeader from reader.
    pub async fn read_from(
        mut reader: impl tokio::io::AsyncReadExt + Unpin,
    ) -> std::io::Result<MessageHeader> {
        let mut content = vec![0; MESSAGE_HEADER_LENGTH];
        reader.read_exact(&mut content).await?;
        Ok(MessageHeader::from(&content))
    }
}

impl<C> Message<C>
where
    C: Codec,
    C::E: std::fmt::Display,
{
    /// Encodes a MessageHeader to writer.
    pub async fn write_to(
        &self,
        mut writer: impl tokio::io::AsyncWriteExt + Unpin,
    ) -> TtResult<()> {
        self.header
            .write_to(&mut writer)
            .await
            .map_err(|e| Error::Socket(e.to_string()))?;
        let content = self
            .payload
            .encode()
            .map_err(err_to_others_err!(e, "Encode payload failed."))?;
        writer
            .write_all(&content)
            .await
            .map_err(|e| Error::Socket(e.to_string()))?;
        Ok(())
    }

    /// Decodes a MessageHeader from reader.
    pub async fn read_from(mut reader: impl tokio::io::AsyncReadExt + Unpin) -> TtResult<Self> {
        let header = MessageHeader::read_from(&mut reader)
            .await
            .map_err(|e| Error::Socket(e.to_string()))?;

        if check_oversize(header.length as usize, true).is_err() {
            discard_message_body(reader, &header).await?;
            return Ok(Self {
                header,
                payload: C::decode("").map_err(err_to_others_err!(e, "Decode payload failed."))?,
            });
        }

        let mut content = vec![0; header.length as usize];
        reader
            .read_exact(&mut content)
            .await
            .map_err(|e| Error::Socket(e.to_string()))?;
        let payload =
            C::decode(content).map_err(err_to_others_err!(e, "Decode payload failed."))?;
        Ok(Self { header, payload })
    }
}
