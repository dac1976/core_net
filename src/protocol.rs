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

use thiserror::Error;

/// Size in bytes of the fixed protocol magic string.
///
/// The magic string is embedded into every message header and is used to:
///
/// - identify valid protocol packets
/// - reject unrelated traffic
/// - help avoid accidental cross-protocol communication
pub const MAGIC_STRING_LEN: usize = 16;

/// Size in bytes of the optional response address field.
///
/// This field exists primarily for historical compatibility with older
/// networking architectures and protocols.
///
/// In many modern request/reply workflows this field may remain zeroed.
pub const RESPONSE_ADDRESS_LEN: usize = 16;

/// Default protocol magic string.
///
/// Stored as a fixed-size 16-byte array.
///
/// Layout:
///
/// ```text
/// "_BEGIN_MESSAGE_\0"
/// ```
///
/// The terminating null byte is intentional to exactly fill 16 bytes.
pub const DEFAULT_MAGIC_STRING: [u8; MAGIC_STRING_LEN] = *b"_BEGIN_MESSAGE_\0";

/// Payload/archive encoding type.
///
/// This tells the receiver how the payload bytes should be interpreted.
///
/// Notes:
///
/// - Archive types 0-3 exist primarily for compatibility with legacy
///   C++ systems using Boost/Cereal.
/// - Rust-side support is currently focused on:
///     - Raw
///     - Protobuf
///     - FlatBuffer
///     - MessagePack
///
/// `repr(u8)` guarantees the enum occupies exactly one byte on the wire.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveType {
    /// Legacy Cereal/Boost portable binary format.
    ///
    /// Intended for C++ compatibility only.
    PortableBinary = 0,

    /// Legacy Cereal/Boost binary format.
    ///
    /// Intended for C++ compatibility only.
    Binary = 1,

    /// Legacy Cereal/Boost JSON archive format.
    ///
    /// Intended for C++ compatibility only.
    Json = 2,

    /// Legacy Cereal/Boost XML archive format.
    ///
    /// Intended for C++ compatibility only.
    Xml = 3,

    /// Raw uninterpreted byte payload.
    ///
    /// The application defines payload semantics.
    Raw = 4,

    /// Google Protocol Buffers payload.
    Protobuf = 5,

    /// FlatBuffers payload.
    FlatBuffer = 6,

    /// MessagePack payload.
    MessagePack = 7,
}

impl TryFrom<u8> for ArchiveType {
    type Error = ProtocolError;

    /// Converts a raw wire byte into an ArchiveType.
    ///
    /// Returns an error for unknown/unsupported values.
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::PortableBinary),
            1 => Ok(Self::Binary),
            2 => Ok(Self::Json),
            3 => Ok(Self::Xml),
            4 => Ok(Self::Raw),
            5 => Ok(Self::Protobuf),
            6 => Ok(Self::FlatBuffer),
            7 => Ok(Self::MessagePack),

            _ => Err(ProtocolError::InvalidArchiveType(value)),
        }
    }
}

/// Fixed-size protocol message header.
///
/// Wire layout:
///
/// ```text
/// +----------------------+
/// | magic_string   [16]  |
/// +----------------------+
/// | response_addr  [16]  |
/// +----------------------+
/// | response_port   u16  |
/// +----------------------+
/// | message_id      i32  |
/// +----------------------+
/// | archive_type    u8   |
/// +----------------------+
/// | total_length    u32  |
/// +----------------------+
/// ```
///
/// The payload bytes immediately follow the header.
///
/// `total_length` includes:
///
/// - header bytes
/// - payload bytes
#[derive(Debug, Clone)]
pub struct MessageHeader {
    /// Protocol validation marker.
    pub magic_string: [u8; MAGIC_STRING_LEN],

    /// Optional response address field.
    ///
    /// Often unused in modern request/reply flows.
    pub response_address: [u8; RESPONSE_ADDRESS_LEN],

    /// Optional response port field.
    pub response_port: u16,

    /// Application-defined message ID.
    ///
    /// Used for dispatch/routing at the application layer.
    pub message_id: i32,

    /// Payload encoding/archive type.
    pub archive_type: ArchiveType,

    /// Total message size in bytes.
    ///
    /// Includes:
    ///
    /// - header
    /// - payload
    pub total_length: u32,
}

impl MessageHeader {
    /// Exact wire size of the fixed header structure.
    ///
    /// Calculated as:
    ///
    /// ```text
    /// 16 + 16 + 2 + 4 + 1 + 4 = 43 bytes
    /// ```
    pub const WIRE_SIZE: usize = 16 + 16 + 2 + 4 + 1 + 4;

    /// Decodes a MessageHeader from raw wire bytes.
    ///
    /// This function:
    ///
    /// - validates minimum header size
    /// - extracts all fixed fields
    /// - converts archive type enum safely
    ///
    /// Note:
    ///
    /// This function does NOT validate:
    ///
    /// - magic string correctness
    /// - payload size consistency
    ///
    /// Those checks are handled separately.
    pub fn decode(bytes: &[u8]) -> Result<Self, ProtocolError> {
        if bytes.len() < Self::WIRE_SIZE {
            return Err(ProtocolError::HeaderTooShort(bytes.len()));
        }

        let mut magic_string = [0u8; MAGIC_STRING_LEN];
        magic_string.copy_from_slice(&bytes[0..16]);

        let mut response_address = [0u8; RESPONSE_ADDRESS_LEN];
        response_address.copy_from_slice(&bytes[16..32]);

        let response_port = u16::from_le_bytes([bytes[32], bytes[33]]);

        let message_id = i32::from_le_bytes([bytes[34], bytes[35], bytes[36], bytes[37]]);

        let archive_type = ArchiveType::try_from(bytes[38])?;

        let total_length = u32::from_le_bytes([bytes[39], bytes[40], bytes[41], bytes[42]]);

        Ok(Self {
            magic_string,
            response_address,
            response_port,
            message_id,
            archive_type,
            total_length,
        })
    }

    /// Validates the decoded header against protocol expectations.
    ///
    /// Checks:
    ///
    /// - magic string correctness
    /// - total length sanity
    ///
    /// This helps reject:
    ///
    /// - unrelated traffic
    /// - corrupted packets
    /// - malformed headers
    pub fn validate_against(
        &self,
        expected_magic_string: &[u8; MAGIC_STRING_LEN],
    ) -> Result<(), ProtocolError> {
        if &self.magic_string != expected_magic_string {
            return Err(ProtocolError::InvalidMagic {
                expected: *expected_magic_string,
                actual: self.magic_string,
            });
        }

        let total = self.total_length as usize;

        // Total length must at least contain the fixed header.
        if total < Self::WIRE_SIZE {
            return Err(ProtocolError::InvalidTotalLength(total));
        }

        Ok(())
    }

    /// Returns the payload length in bytes.
    ///
    /// Calculated as:
    ///
    /// ```text
    /// total_length - header_size
    /// ```
    pub fn payload_len(&self) -> Result<usize, ProtocolError> {
        let total = self.total_length as usize;

        if total < Self::WIRE_SIZE {
            return Err(ProtocolError::InvalidTotalLength(total));
        }

        Ok(total - Self::WIRE_SIZE)
    }
}

/// Protocol-level decode/validation errors.
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// Incoming data was smaller than the fixed header size.
    #[error("header too short: {0}")]
    HeaderTooShort(usize),

    /// Unknown archive type byte encountered.
    #[error("invalid archive type: {0}")]
    InvalidArchiveType(u8),

    /// Invalid total message length.
    ///
    /// Usually indicates:
    ///
    /// - corrupted data
    /// - malformed packet
    /// - protocol mismatch
    #[error("invalid total length: {0}")]
    InvalidTotalLength(usize),

    /// Magic string mismatch.
    ///
    /// Indicates the received message does not belong to the expected
    /// protocol/application domain.
    #[error("invalid magic string")]
    InvalidMagic {
        expected: [u8; MAGIC_STRING_LEN],
        actual: [u8; MAGIC_STRING_LEN],
    },
}
