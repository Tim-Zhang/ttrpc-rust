// Copyright 2022 Alibaba Cloud. All rights reserved.
// Copyright (c) 2020 Ant Financial
//
// SPDX-License-Identifier: Apache-2.0
//

#[allow(soft_unstable, clippy::type_complexity, clippy::too_many_arguments)]
mod compiled {
    include!(concat!(env!("OUT_DIR"), "/mod.rs"));
}
pub use compiled::ttrpc::*;

use byteorder::{BigEndian, ByteOrder};
use protobuf::{CodedInputStream, CodedOutputStream};

use crate::error::{get_rpc_status, Error, Result as TtResult};

pub const MESSAGE_HEADER_LENGTH: usize = 10;
pub const MESSAGE_LENGTH_MAX: usize = 4 << 20;
pub const DEFAULT_PAGE_SIZE: usize = 4 << 10;

pub const MESSAGE_TYPE_REQUEST: u8 = 0x1;
pub const MESSAGE_TYPE_RESPONSE: u8 = 0x2;
pub const MESSAGE_TYPE_DATA: u8 = 0x3;

pub const FLAG_REMOTE_CLOSED: u8 = 0x1;
pub const FLAG_REMOTE_OPEN: u8 = 0x2;
pub const FLAG_NO_DATA: u8 = 0x4;

pub(crate) fn check_oversize(len: usize, return_rpc_error: bool) -> TtResult<()> {
    if len > MESSAGE_LENGTH_MAX {
        let msg = format!(
            "message length {} exceed maximum message size of {}",
            len, MESSAGE_LENGTH_MAX
        );
        let e = if return_rpc_error {
            get_rpc_status(Code::INVALID_ARGUMENT, msg)
        } else {
            Error::Others(msg)
        };

        return Err(e);
    }

    Ok(())
}

/// Message header of ttrpc.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageHeader {
    pub length: u32,
    pub stream_id: u32,
    pub type_: u8,
    pub flags: u8,
}

impl<T> From<T> for MessageHeader
where
    T: AsRef<[u8]>,
{
    fn from(buf: T) -> Self {
        let buf = buf.as_ref();
        debug_assert!(buf.len() >= MESSAGE_HEADER_LENGTH);
        Self {
            length: BigEndian::read_u32(&buf[..4]),
            stream_id: BigEndian::read_u32(&buf[4..8]),
            type_: buf[8],
            flags: buf[9],
        }
    }
}

impl From<MessageHeader> for Vec<u8> {
    fn from(mh: MessageHeader) -> Self {
        let mut buf = vec![0u8; MESSAGE_HEADER_LENGTH];
        mh.into_buf(&mut buf);
        buf
    }
}

impl MessageHeader {
    /// Creates a request MessageHeader from stream_id and len.
    ///
    /// Use the default message type MESSAGE_TYPE_REQUEST, and default flags 0.
    pub fn new_request(stream_id: u32, len: u32) -> Self {
        Self {
            length: len,
            stream_id,
            type_: MESSAGE_TYPE_REQUEST,
            flags: 0,
        }
    }

    /// Creates a response MessageHeader from stream_id and len.
    ///
    /// Use the MESSAGE_TYPE_RESPONSE message type, and default flags 0.
    pub fn new_response(stream_id: u32, len: u32) -> Self {
        Self {
            length: len,
            stream_id,
            type_: MESSAGE_TYPE_RESPONSE,
            flags: 0,
        }
    }

    /// Creates a data MessageHeader from stream_id and len.
    ///
    /// Use the MESSAGE_TYPE_DATA message type, and default flags 0.
    pub fn new_data(stream_id: u32, len: u32) -> Self {
        Self {
            length: len,
            stream_id,
            type_: MESSAGE_TYPE_DATA,
            flags: 0,
        }
    }

    /// Set the stream_id of message using the given value.
    pub fn set_stream_id(&mut self, stream_id: u32) {
        self.stream_id = stream_id;
    }

    /// Set the flags of message using the given flags.
    pub fn set_flags(&mut self, flags: u8) {
        self.flags = flags;
    }

    /// Add a new flags to the message.
    pub fn add_flags(&mut self, flags: u8) {
        self.flags |= flags;
    }

    pub(crate) fn into_buf(self, mut buf: impl AsMut<[u8]>) {
        let buf = buf.as_mut();
        debug_assert!(buf.len() >= MESSAGE_HEADER_LENGTH);

        let covbuf: &mut [u8] = &mut buf[..4];
        BigEndian::write_u32(covbuf, self.length);
        let covbuf: &mut [u8] = &mut buf[4..8];
        BigEndian::write_u32(covbuf, self.stream_id);
        buf[8] = self.type_;
        buf[9] = self.flags;
    }
}

/// Generic message of ttrpc.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct GenMessage {
    pub header: MessageHeader,
    pub payload: Vec<u8>,
    #[cfg(all(feature = "send-fd", target_family = "unix"))]
    pub fds: Vec<std::os::unix::io::RawFd>,
}

#[derive(Debug, PartialEq)]
pub enum GenMessageError {
    InternalError(Error),
    ReturnError(MessageHeader, Error),
}

impl From<Error> for GenMessageError {
    fn from(e: Error) -> Self {
        Self::InternalError(e)
    }
}

/// TTRPC codec, only protobuf is supported.
pub trait Codec {
    type E;

    fn size(&self) -> u32;
    fn encode(&self) -> Result<Vec<u8>, Self::E>;
    fn decode(buf: impl AsRef<[u8]>) -> Result<Self, Self::E>
    where
        Self: Sized;
}

impl<M: protobuf::Message> Codec for M {
    type E = protobuf::Error;

    fn size(&self) -> u32 {
        self.compute_size() as u32
    }

    fn encode(&self) -> Result<Vec<u8>, Self::E> {
        let mut buf = vec![0; self.compute_size() as usize];
        let mut s = CodedOutputStream::bytes(&mut buf);
        self.write_to(&mut s)?;
        s.flush()?;
        drop(s);
        Ok(buf)
    }

    fn decode(buf: impl AsRef<[u8]>) -> Result<Self, Self::E> {
        let mut s = CodedInputStream::from_bytes(buf.as_ref());
        M::parse_from(&mut s)
    }
}

/// Message of ttrpc.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct Message<C> {
    pub header: MessageHeader,
    pub payload: C,
}

impl<C> std::convert::TryFrom<GenMessage> for Message<C>
where
    C: Codec,
{
    type Error = C::E;
    fn try_from(gen: GenMessage) -> Result<Self, Self::Error> {
        Ok(Self {
            header: gen.header,
            payload: C::decode(&gen.payload)?,
        })
    }
}

impl<C> std::convert::TryFrom<Message<C>> for GenMessage
where
    C: Codec,
{
    type Error = C::E;
    fn try_from(msg: Message<C>) -> Result<Self, Self::Error> {
        Ok(Self {
            header: msg.header,
            payload: msg.payload.encode()?,
            ..Default::default()
        })
    }
}

impl<C: Codec> Message<C> {
    pub fn new_request(stream_id: u32, message: C) -> TtResult<Self> {
        check_oversize(message.size() as usize, false)?;

        Ok(Self {
            header: MessageHeader::new_request(stream_id, message.size()),
            payload: message,
        })
    }
}

#[cfg(test)]
pub mod tests {
    use super::super::test_utils::*;
    use std::convert::{TryFrom, TryInto};

    use super::*;

    #[test]
    fn message_header() {
        let mh = MessageHeader::from(&MESSAGE_HEADER);
        assert_eq!(mh.length, 0x1000_0000);
        assert_eq!(mh.stream_id, 0x3);
        assert_eq!(mh.type_, MESSAGE_TYPE_RESPONSE);
        assert_eq!(mh.flags, 0xef);

        let mut buf2 = vec![0; MESSAGE_HEADER_LENGTH];
        mh.into_buf(&mut buf2);
        assert_eq!(&MESSAGE_HEADER, &buf2[..]);

        let mh = MessageHeader::from(&PROTOBUF_MESSAGE_HEADER);
        assert_eq!(mh.length as usize, TEST_PAYLOAD_LEN);
    }

    #[test]
    fn protobuf_codec() {
        let creq = new_protobuf_request();
        let buf = creq.encode().unwrap();
        assert_eq!(&buf, &PROTOBUF_REQUEST);
        let dreq = Request::decode(&buf).unwrap();
        assert_eq!(creq, dreq);
        let dreq2 = Request::decode(PROTOBUF_REQUEST).unwrap();
        assert_eq!(creq, dreq2);
    }

    #[test]
    fn gen_message_to_message() {
        let req = new_protobuf_request();
        let msg = Message::new_request(3, req).unwrap();
        let msg_clone = msg.clone();
        let gen: GenMessage = msg.try_into().unwrap();
        let dmsg = Message::<Request>::try_from(gen).unwrap();
        assert_eq!(msg_clone, dmsg);
    }
}
