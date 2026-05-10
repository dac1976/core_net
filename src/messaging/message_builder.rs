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

use crate::protocol::{ArchiveType, MAGIC_STRING_LEN, MessageHeader, RESPONSE_ADDRESS_LEN};

/// Lightweight outbound message header representation.
///
/// This structure contains the metadata required to build a complete
/// wire-format message.
///
/// The payload itself is not stored here.
///
/// Wire layout:
///
/// ```text
/// +-------------------+
/// | magic string      |
/// +-------------------+
/// | response address  |
/// +-------------------+
/// | response port     |
/// +-------------------+
/// | message id        |
/// +-------------------+
/// | archive type      |
/// +-------------------+
/// | total length      |
/// +-------------------+
/// | payload bytes     |
/// +-------------------+
/// ```
#[derive(Debug, Clone)]
pub struct OutboundMessageHeader {
    /// Protocol magic string used to validate framing.
    pub magic_string: [u8; MAGIC_STRING_LEN],

    /// Optional response address field.
    ///
    /// This is protocol-level metadata and is independent of the transport
    /// socket address.
    pub response_address: [u8; RESPONSE_ADDRESS_LEN],

    /// Optional response port field.
    pub response_port: u16,

    /// Application message identifier.
    ///
    /// Used by the dispatcher layer for routing.
    pub message_id: i32,

    /// Payload/archive encoding type.
    ///
    /// Examples:
    ///
    /// - Raw
    /// - Protobuf
    /// - FlatBuffer
    /// - MessagePack
    pub archive_type: ArchiveType,
}

impl OutboundMessageHeader {
    /// Creates a new outbound message header.
    ///
    /// Optional response fields default to zero-filled values when omitted.
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

/// Internal helper that serialises:
///
/// - protocol header
/// - payload
///
/// into a caller-provided output buffer.
///
/// Important optimisation:
///
/// The output buffer is reused rather than recreated, allowing callers to:
///
/// - avoid repeated allocations
/// - retain Vec capacity
/// - reduce heap churn under sustained traffic
fn write_message(out: &mut Vec<u8>, header: &OutboundMessageHeader, payload: &[u8]) {
    // Total wire-format message length including protocol header.
    let total_length = (MessageHeader::WIRE_SIZE + payload.len()) as u32;

    // Reuse caller-owned Vec allocation.
    out.clear();

    // Ensure enough capacity exists.
    //
    // reserve() only grows if required.
    out.reserve(MessageHeader::WIRE_SIZE + payload.len());

    // ---------------------------------------------------------------------
    // Serialise protocol header fields.
    // ---------------------------------------------------------------------

    out.extend_from_slice(&header.magic_string);

    out.extend_from_slice(&header.response_address);

    out.extend_from_slice(&header.response_port.to_le_bytes());

    out.extend_from_slice(&header.message_id.to_le_bytes());

    out.push(header.archive_type as u8);

    out.extend_from_slice(&total_length.to_le_bytes());

    // ---------------------------------------------------------------------
    // Append payload directly into same output buffer.
    // ---------------------------------------------------------------------

    out.extend_from_slice(payload);
}

/// Builds a RAW-format message directly into a caller-provided buffer.
///
/// This is the preferred high-performance API for hot paths because:
///
/// - Vec allocation can be reused
/// - capacity is retained across calls
/// - fewer temporary buffers are created
///
/// Typical usage:
///
/// ```rust
/// let mut msg = Vec::with_capacity(65536);
///
/// build_raw_message_into(...);
/// ```
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

/// Convenience API that allocates and returns a RAW-format message.
///
/// Simpler ergonomics but less efficient than *_into APIs because a new
/// Vec allocation is created per call.
pub fn build_raw_message(
    magic_string: [u8; MAGIC_STRING_LEN],
    message_id: i32,
    payload: &[u8],
    response_address: Option<[u8; RESPONSE_ADDRESS_LEN]>,
    response_port: Option<u16>,
) -> Vec<u8> {
    // Preallocate exact-ish required capacity.
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

/// Builds a Protobuf-format message into a caller-provided buffer.
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

/// Convenience API returning an owned Protobuf-format message buffer.
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

/// Builds a FlatBuffer-format message into a caller-provided buffer.
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

/// Convenience API returning an owned FlatBuffer-format message buffer.
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

/// Builds a MessagePack-format message into a caller-provided buffer.
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

/// Convenience API returning an owned MessagePack-format message buffer.
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

/// Internal helper allowing payload bytes to be written directly into the
/// destination message buffer.
///
/// This avoids:
///
/// - intermediate payload Vec allocations
/// - payload copy steps
///
/// and is useful for high-throughput data generation paths.
fn write_message_with_payload<F>(
    out: &mut Vec<u8>,
    header: &OutboundMessageHeader,
    payload_len: usize,
    write_payload: F,
) where
    F: FnOnce(&mut Vec<u8>),
{
    // Total wire-format message length including protocol header.
    let total_length = (MessageHeader::WIRE_SIZE + payload_len) as u32;

    // Reuse caller-owned Vec allocation.
    out.clear();

    // Ensure sufficient capacity exists.
    out.reserve(MessageHeader::WIRE_SIZE + payload_len);

    // ---------------------------------------------------------------------
    // Serialise protocol header.
    // ---------------------------------------------------------------------

    out.extend_from_slice(&header.magic_string);

    out.extend_from_slice(&header.response_address);

    out.extend_from_slice(&header.response_port.to_le_bytes());

    out.extend_from_slice(&header.message_id.to_le_bytes());

    out.push(header.archive_type as u8);

    out.extend_from_slice(&total_length.to_le_bytes());

    // ---------------------------------------------------------------------
    // Caller writes payload directly into final destination buffer.
    // ---------------------------------------------------------------------

    write_payload(out);
}

/// High-performance RAW message builder allowing direct payload generation
/// into the final output buffer.
///
/// This is the most allocation-efficient API in this module.
///
/// Example use cases:
///
/// - packet generators
/// - real-time acquisition systems
/// - zero-copy-ish serialisation paths
/// - high-rate telemetry
///
/// Example:
///
/// ```rust
/// build_raw_message_with_payload_into(
///     &mut msg,
///     magic,
///     id,
///     payload_len,
///     None,
///     None,
///     |out| {
///         out.extend_from_slice(samples);
///     },
/// );
/// ```
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
