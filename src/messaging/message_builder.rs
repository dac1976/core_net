use crate::protocol::{ArchiveType, MessageHeader, MAGIC_STRING_LEN, RESPONSE_ADDRESS_LEN};

#[derive(Debug, Clone)]
pub struct OutboundMessageHeader {
    pub magic_string: [u8; MAGIC_STRING_LEN],
    pub response_address: [u8; RESPONSE_ADDRESS_LEN],
    pub response_port: u16,
    pub message_id: i32,
    pub archive_type: ArchiveType,
}

impl OutboundMessageHeader {
    pub fn new(
        magic_string: [u8; MAGIC_STRING_LEN],
        message_id: i32,
        archive_type: ArchiveType,
    ) -> Self {
        Self {
            magic_string,
            response_address: [0u8; RESPONSE_ADDRESS_LEN],
            response_port: 0,
            message_id,
            archive_type,
        }
    }
}

pub fn build_message(header: &OutboundMessageHeader, payload: &[u8]) -> Vec<u8> {
    let total_length = (MessageHeader::WIRE_SIZE + payload.len()) as u32;
    let mut out = Vec::with_capacity(total_length as usize);

    out.extend_from_slice(&header.magic_string);
    out.extend_from_slice(&header.response_address);
    out.extend_from_slice(&header.response_port.to_le_bytes());
    out.extend_from_slice(&header.message_id.to_le_bytes());
    out.push(header.archive_type as u8);
    out.extend_from_slice(&total_length.to_le_bytes());
    out.extend_from_slice(payload);

    out
}

pub fn build_raw_message(
    magic_string: [u8; MAGIC_STRING_LEN],
    message_id: i32,
    payload: &[u8],
) -> Vec<u8> {
    let header = OutboundMessageHeader::new(magic_string, message_id, ArchiveType::Raw);
    build_message(&header, payload)
}

pub fn build_protobuf_message(
    magic_string: [u8; MAGIC_STRING_LEN],
    message_id: i32,
    payload: &[u8],
) -> Vec<u8> {
    let header = OutboundMessageHeader::new(magic_string, message_id, ArchiveType::Protobuf);
    build_message(&header, payload)
}

pub fn build_flatbuffer_message(
    magic_string: [u8; MAGIC_STRING_LEN],
    message_id: i32,
    payload: &[u8],
) -> Vec<u8> {
    let header = OutboundMessageHeader::new(magic_string, message_id, ArchiveType::FlatBuffer);
    build_message(&header, payload)
}

pub fn build_msgpack_message(
    magic_string: [u8; MAGIC_STRING_LEN],
    message_id: i32,
    payload: &[u8],
) -> Vec<u8> {
    let header = OutboundMessageHeader::new(magic_string, message_id, ArchiveType::MessagePack);
    build_message(&header, payload)
}
