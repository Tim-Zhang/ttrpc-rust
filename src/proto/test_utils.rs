use super::{KeyValue, Request, MESSAGE_HEADER_LENGTH};

pub static MESSAGE_HEADER: [u8; MESSAGE_HEADER_LENGTH] = [
    0x10, 0x0, 0x0, 0x0, // length
    0x0, 0x0, 0x0, 0x03, // stream_id
    0x2,  // type_
    0xef, // flags
];

#[rustfmt::skip]
pub static PROTOBUF_MESSAGE_HEADER: [u8; MESSAGE_HEADER_LENGTH] = [
	0x00, 0x0, 0x0, TEST_PAYLOAD_LEN as u8, // length
	0x0, 0x12, 0x34, 0x56, // stream_id
	0x1,  // type_
	0xef, // flags
];

pub const TEST_PAYLOAD_LEN: usize = 67;
pub static PROTOBUF_REQUEST: [u8; TEST_PAYLOAD_LEN] = [
    10, 17, 103, 114, 112, 99, 46, 84, 101, 115, 116, 83, 101, 114, 118, 105, 99, 101, 115, 18, 4,
    84, 101, 115, 116, 26, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 32, 128, 218, 196, 9, 42, 24, 10, 9, 116,
    101, 115, 116, 95, 107, 101, 121, 49, 18, 11, 116, 101, 115, 116, 95, 118, 97, 108, 117, 101,
    49,
];

pub fn new_protobuf_request() -> Request {
    let mut creq = Request::new();
    creq.set_service("grpc.TestServices".to_string());
    creq.set_method("Test".to_string());
    creq.set_timeout_nano(20 * 1000 * 1000);
    let meta = vec![KeyValue {
        key: "test_key1".to_string(),
        value: "test_value1".to_string(),
        ..Default::default()
    }];
    creq.set_metadata(meta);
    creq.payload = vec![0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9];
    creq
}
