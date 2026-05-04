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

pub const MAGIC_STRING_LEN: usize = 16;
pub const RESPONSE_ADDRESS_LEN: usize = 16;
pub const DEFAULT_MAGIC_STRING: [u8; MAGIC_STRING_LEN] = *b"_BEGIN_MESSAGE_\0";

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveType {
    PortableBinary = 0, // Cereal/Boost -only C++ compatible format, no Rust support
    Binary = 1,         // Cereal/Boost -only C++ compatible format, no Rust support
    Json = 2,           // Cereal/Boost -only C++ compatible format, no Rust support
    Xml = 3,            // Cereal/Boost -only C++ compatible format, no Rust support
    Raw = 4,
    Protobuf = 5,
    FlatBuffer = 6,
    MessagePack = 7,
}

impl TryFrom<u8> for ArchiveType {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::PortableBinary), // Cereal/Boost -only C++ compatible format, no Rust support
            1 => Ok(Self::Binary), // Cereal/Boost -only C++ compatible format, no Rust support
            2 => Ok(Self::Json),   // Cereal/Boost -only C++ compatible format, no Rust support
            3 => Ok(Self::Xml),    // Cereal/Boost -only C++ compatible format, no Rust support
            4 => Ok(Self::Raw),
            5 => Ok(Self::Protobuf),
            6 => Ok(Self::FlatBuffer),
            7 => Ok(Self::MessagePack),
            _ => Err(ProtocolError::InvalidArchiveType(value)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageHeader {
    pub magic_string: [u8; MAGIC_STRING_LEN],
    pub response_address: [u8; RESPONSE_ADDRESS_LEN],
    pub response_port: u16,
    pub message_id: i32,
    pub archive_type: ArchiveType,
    pub total_length: u32,
}

impl MessageHeader {
    pub const WIRE_SIZE: usize = 16 + 16 + 2 + 4 + 1 + 4;

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
        if total < Self::WIRE_SIZE {
            return Err(ProtocolError::InvalidTotalLength(total));
        }

        Ok(())
    }

    pub fn payload_len(&self) -> Result<usize, ProtocolError> {
        let total = self.total_length as usize;
        if total < Self::WIRE_SIZE {
            return Err(ProtocolError::InvalidTotalLength(total));
        }
        Ok(total - Self::WIRE_SIZE)
    }
}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("header too short: {0}")]
    HeaderTooShort(usize),

    #[error("invalid archive type: {0}")]
    InvalidArchiveType(u8),

    #[error("invalid total length: {0}")]
    InvalidTotalLength(usize),

    #[error("invalid magic string")]
    InvalidMagic {
        expected: [u8; MAGIC_STRING_LEN],
        actual: [u8; MAGIC_STRING_LEN],
    },
}
