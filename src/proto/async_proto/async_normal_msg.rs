use super::*;
use crate::error::{Error, Result as TtResult};

impl GenMessage {
    /// Encodes a MessageHeader to writer.
    pub async fn write_to(
        &self,
        mut writer: impl tokio::io::AsyncWriteExt + Unpin,
    ) -> TtResult<()> {
        self.header
            .write_to(&mut writer)
            .await
            .map_err(|e| Error::Socket(e.to_string()))?;
        writer
            .write_all(&self.payload)
            .await
            .map_err(|e| Error::Socket(e.to_string()))?;
        Ok(())
    }

    /// Decodes a MessageHeader from reader.
    pub async fn read_from(
        mut reader: impl tokio::io::AsyncReadExt + Unpin,
    ) -> std::result::Result<Self, GenMessageError> {
        let header = MessageHeader::read_from(&mut reader)
            .await
            .map_err(|e| Error::Socket(e.to_string()))?;

        if let Err(e) = check_oversize(header.length as usize, true) {
            discard_message_body(reader, &header).await?;
            return Err(GenMessageError::ReturnError(header, e));
        }

        let mut content = vec![0; header.length as usize];
        reader
            .read_exact(&mut content)
            .await
            .map_err(|e| Error::Socket(e.to_string()))?;

        Ok(Self {
            header,
            payload: content,
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::{common_proto::*, test_utils::*};
    use super::*;

    #[tokio::test]
    async fn async_message_header() {
        use std::io::Cursor;
        let mut buf = vec![];
        let mut io = Cursor::new(&mut buf);
        let mh = MessageHeader::from(&MESSAGE_HEADER);
        mh.write_to(&mut io).await.unwrap();
        assert_eq!(buf, &MESSAGE_HEADER);

        let dmh = MessageHeader::read_from(&buf[..]).await.unwrap();
        assert_eq!(mh, dmh);
    }

    #[tokio::test]
    async fn async_gen_message() {
        // Test packet which exceeds maximum message size
        let mut buf = Vec::from(MESSAGE_HEADER);
        let header = MessageHeader::read_from(&*buf).await.expect("read header");
        buf.append(&mut vec![0x0; header.length as usize]);

        match GenMessage::read_from(&*buf).await {
            Err(GenMessageError::ReturnError(h, Error::RpcStatus(s))) => {
                if h != header || s.code() != crate::proto::Code::INVALID_ARGUMENT {
                    panic!("got invalid error when the size exceeds limit");
                }
            }
            _ => {
                panic!("got invalid error when the size exceeds limit");
            }
        }

        let mut buf = Vec::from(PROTOBUF_MESSAGE_HEADER);
        buf.extend_from_slice(&PROTOBUF_REQUEST);
        buf.extend_from_slice(&[0x0, 0x0]);
        let gen = GenMessage::read_from(&*buf).await.unwrap();
        assert_eq!(gen.header.length as usize, TEST_PAYLOAD_LEN);
        assert_eq!(gen.header.length, gen.payload.len() as u32);
        assert_eq!(gen.header.stream_id, 0x123456);
        assert_eq!(gen.header.type_, MESSAGE_TYPE_REQUEST);
        assert_eq!(gen.header.flags, 0xef);
        assert_eq!(&gen.payload, &PROTOBUF_REQUEST);
        assert_eq!(
            &buf[MESSAGE_HEADER_LENGTH + TEST_PAYLOAD_LEN..],
            &[0x0, 0x0]
        );

        let mut dbuf = vec![];
        let mut io = std::io::Cursor::new(&mut dbuf);
        gen.write_to(&mut io).await.unwrap();
        assert_eq!(&*dbuf, &buf[..MESSAGE_HEADER_LENGTH + TEST_PAYLOAD_LEN]);
    }

    #[tokio::test]
    async fn async_message() {
        // Test packet which exceeds maximum message size
        let mut buf = Vec::from(MESSAGE_HEADER);
        let header = MessageHeader::read_from(&*buf).await.expect("read header");
        buf.append(&mut vec![0x0; header.length as usize]);

        let gen = Message::<Request>::read_from(&*buf)
            .await
            .expect("read message");

        assert_eq!(gen.header, header);
        assert_eq!(protobuf::Message::compute_size(&gen.payload), 0);

        let mut buf = Vec::from(PROTOBUF_MESSAGE_HEADER);
        buf.extend_from_slice(&PROTOBUF_REQUEST);
        buf.extend_from_slice(&[0x0, 0x0]);
        let msg = Message::<Request>::read_from(&*buf).await.unwrap();
        assert_eq!(msg.header.length, 67);
        assert_eq!(msg.header.length, msg.payload.size());
        assert_eq!(msg.header.stream_id, 0x123456);
        assert_eq!(msg.header.type_, MESSAGE_TYPE_REQUEST);
        assert_eq!(msg.header.flags, 0xef);
        assert_eq!(&msg.payload.service, "grpc.TestServices");
        assert_eq!(&msg.payload.method, "Test");
        assert_eq!(
            msg.payload.payload,
            vec![0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9]
        );
        assert_eq!(msg.payload.timeout_nano, 20 * 1000 * 1000);
        assert_eq!(msg.payload.metadata.len(), 1);
        assert_eq!(&msg.payload.metadata[0].key, "test_key1");
        assert_eq!(&msg.payload.metadata[0].value, "test_value1");

        let req = new_protobuf_request();
        let mut dmsg = Message::new_request(u32::MAX, req).unwrap();
        dmsg.header.set_stream_id(0x123456);
        dmsg.header.set_flags(0xe0);
        dmsg.header.add_flags(0x0f);
        let mut dbuf = vec![];
        let mut io = std::io::Cursor::new(&mut dbuf);
        dmsg.write_to(&mut io).await.unwrap();
        assert_eq!(&dbuf, &buf[..MESSAGE_HEADER_LENGTH + TEST_PAYLOAD_LEN]);
    }
}
