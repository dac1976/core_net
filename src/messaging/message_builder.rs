// This file is part of core-net Rust crate containing useful reusable
// networking utilities.
//
// Copyright (C) 2026 to present, Duncan Crutchley
// Contact <15799155+dac1976@users.noreply.github.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License and GNU Lesser General Public License
// for more details.
//
// You should have received a copy of the GNU General Public License
// and GNU Lesser General Public License along with this program. If
// not, see <http://www.gnu.org/licenses/>.

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
        response_address: Option<[u8; RESPONSE_ADDRESS_LEN]>,
        response_port: Option<u16>,
    ) -> Self {
        Self {
            magic_string,
            response_address: response_address.unwrap_or([0u8; RESPONSE_ADDRESS_LEN]),
            response_port: response_port.unwrap_or(0),
            message_id,
            archive_type,
        }
    }
}

fn write_message(out: &mut Vec<u8>, header: &OutboundMessageHeader, payload: &[u8]) {
    let total_length = (MessageHeader::WIRE_SIZE + payload.len()) as u32;

    out.clear();
    out.reserve(MessageHeader::WIRE_SIZE + payload.len());

    out.extend_from_slice(&header.magic_string);
    out.extend_from_slice(&header.response_address);
    out.extend_from_slice(&header.response_port.to_le_bytes());
    out.extend_from_slice(&header.message_id.to_le_bytes());
    out.push(header.archive_type as u8);
    out.extend_from_slice(&total_length.to_le_bytes());

    // 👇 payload written directly into same buffer
    out.extend_from_slice(payload);
}

pub fn build_raw_message_into(
    out: &mut Vec<u8>,
    magic_string: [u8; MAGIC_STRING_LEN],
    message_id: i32,
    payload: &[u8],
    response_address: Option<[u8; RESPONSE_ADDRESS_LEN]>,
    response_port: Option<u16>,
) {
    let header = OutboundMessageHeader::new(
        magic_string,
        message_id,
        ArchiveType::Raw,
        response_address,
        response_port,
    );

    write_message(out, &header, payload);
}

pub fn build_raw_message(
    magic_string: [u8; MAGIC_STRING_LEN],
    message_id: i32,
    payload: &[u8],
    response_address: Option<[u8; RESPONSE_ADDRESS_LEN]>,
    response_port: Option<u16>,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(MessageHeader::WIRE_SIZE + payload.len());

    build_raw_message_into(
        &mut out,
        magic_string,
        message_id,
        payload,
        response_address,
        response_port,
    );

    out
}

pub fn build_protobuf_message_into(
    out: &mut Vec<u8>,
    magic_string: [u8; MAGIC_STRING_LEN],
    message_id: i32,
    payload: &[u8],
    response_address: Option<[u8; RESPONSE_ADDRESS_LEN]>,
    response_port: Option<u16>,
) {
    let header = OutboundMessageHeader::new(
        magic_string,
        message_id,
        ArchiveType::Protobuf,
        response_address,
        response_port,
    );

    write_message(out, &header, payload);
}

pub fn build_protobuf_message(
    magic_string: [u8; MAGIC_STRING_LEN],
    message_id: i32,
    payload: &[u8],
    response_address: Option<[u8; RESPONSE_ADDRESS_LEN]>,
    response_port: Option<u16>,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(MessageHeader::WIRE_SIZE + payload.len());

    build_protobuf_message_into(
        &mut out,
        magic_string,
        message_id,
        payload,
        response_address,
        response_port,
    );

    out
}

pub fn build_flatbuffer_message_into(
    out: &mut Vec<u8>,
    magic_string: [u8; MAGIC_STRING_LEN],
    message_id: i32,
    payload: &[u8],
    response_address: Option<[u8; RESPONSE_ADDRESS_LEN]>,
    response_port: Option<u16>,
) {
    let header = OutboundMessageHeader::new(
        magic_string,
        message_id,
        ArchiveType::FlatBuffer,
        response_address,
        response_port,
    );

    write_message(out, &header, payload);
}

pub fn build_flatbuffer_message(
    magic_string: [u8; MAGIC_STRING_LEN],
    message_id: i32,
    payload: &[u8],
    response_address: Option<[u8; RESPONSE_ADDRESS_LEN]>,
    response_port: Option<u16>,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(MessageHeader::WIRE_SIZE + payload.len());

    build_flatbuffer_message_into(
        &mut out,
        magic_string,
        message_id,
        payload,
        response_address,
        response_port,
    );

    out
}

pub fn build_msgpack_message_into(
    out: &mut Vec<u8>,
    magic_string: [u8; MAGIC_STRING_LEN],
    message_id: i32,
    payload: &[u8],
    response_address: Option<[u8; RESPONSE_ADDRESS_LEN]>,
    response_port: Option<u16>,
) {
    let header = OutboundMessageHeader::new(
        magic_string,
        message_id,
        ArchiveType::MessagePack,
        response_address,
        response_port,
    );

    write_message(out, &header, payload);
}

pub fn build_msgpack_message(
    magic_string: [u8; MAGIC_STRING_LEN],
    message_id: i32,
    payload: &[u8],
    response_address: Option<[u8; RESPONSE_ADDRESS_LEN]>,
    response_port: Option<u16>,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(MessageHeader::WIRE_SIZE + payload.len());

    build_msgpack_message_into(
        &mut out,
        magic_string,
        message_id,
        payload,
        response_address,
        response_port,
    );

    out
}

fn write_message_with_payload<F>(
    out: &mut Vec<u8>,
    header: &OutboundMessageHeader,
    payload_len: usize,
    write_payload: F,
) where
    F: FnOnce(&mut Vec<u8>),
{
    let total_length = (MessageHeader::WIRE_SIZE + payload_len) as u32;

    out.clear();
    out.reserve(MessageHeader::WIRE_SIZE + payload_len);

    // header
    out.extend_from_slice(&header.magic_string);
    out.extend_from_slice(&header.response_address);
    out.extend_from_slice(&header.response_port.to_le_bytes());
    out.extend_from_slice(&header.message_id.to_le_bytes());
    out.push(header.archive_type as u8);
    out.extend_from_slice(&total_length.to_le_bytes());

    // 👇 payload written directly into same buffer
    write_payload(out);
}

pub fn build_raw_message_with_payload_into<F>(
    out: &mut Vec<u8>,
    magic_string: [u8; MAGIC_STRING_LEN],
    message_id: i32,
    payload_len: usize,
    response_address: Option<[u8; RESPONSE_ADDRESS_LEN]>,
    response_port: Option<u16>,
    write_payload: F,
) where
    F: FnOnce(&mut Vec<u8>),
{
    let header = OutboundMessageHeader::new(
        magic_string,
        message_id,
        ArchiveType::Raw,
        response_address,
        response_port,
    );

    write_message_with_payload(out, &header, payload_len, write_payload);
}
