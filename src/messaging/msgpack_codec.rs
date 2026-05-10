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

use serde::{Serialize, de::DeserializeOwned};

/// Serialises a Rust value into a MessagePack byte vector.
///
/// This is a thin convenience wrapper around `rmp-serde`.
///
/// MessagePack is useful for:
///
/// - compact binary messaging
/// - network transport
/// - cross-language interoperability
/// - lower overhead than JSON
///
/// Example:
///
/// ```rust
/// #[derive(Serialize)]
/// struct MyMessage {
///     value: u32,
/// }
///
/// let msg = MyMessage { value: 42 };
///
/// let bytes = to_msgpack_vec(&msg)?;
/// ```
///
/// Allocation behaviour:
///
/// - allocates a new Vec<u8>
/// - encoded bytes are owned by caller
pub fn to_msgpack_vec<T: Serialize>(value: &T) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    rmp_serde::to_vec(value)
}

/// Deserialises a MessagePack byte slice into a Rust type.
///
/// This is a thin convenience wrapper around `rmp-serde`.
///
/// The target type must implement `Deserialize`.
///
/// Example:
///
/// ```rust
/// #[derive(Deserialize)]
/// struct MyReply {
///     ok: bool,
/// }
///
/// let reply: MyReply = from_msgpack_slice(bytes)?;
/// ```
///
/// Performance notes:
///
/// - reads directly from provided byte slice
/// - no intermediate UTF-8 conversion
/// - efficient for binary network protocols
pub fn from_msgpack_slice<T: DeserializeOwned>(
    bytes: &[u8],
) -> Result<T, rmp_serde::decode::Error> {
    rmp_serde::from_slice(bytes)
}
