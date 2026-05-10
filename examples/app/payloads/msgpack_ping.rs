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

use serde::{Deserialize, Serialize};

/// Example typed request payload used by the MessagePack ping demo.
///
/// This struct is serialized/deserialized using Serde and MessagePack.
///
/// It demonstrates how higher-level structured messages can be transported
/// using core-net's raw framing layer:
///
/// ```text
/// core-net message header
///   -> archive_type = MessagePack
///   -> payload      = MessagePack(PingRequest)
/// ```
///
/// Example logical content:
///
/// ```text
/// name  = "Duncan"
/// count = 42
/// ```
#[derive(Debug, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct PingRequest {
    /// Name or label included in the ping request.
    pub name: String,

    /// Example numeric value included in the request.
    pub count: u32,
}

/// Example typed reply payload used by the MessagePack ping demo.
///
/// This is encoded as MessagePack and returned to the peer as the payload
/// of a core-net message with archive type `MessagePack`.
///
/// Example logical content:
///
/// ```text
/// ok      = true
/// message = "Hello Duncan, count=42"
/// ```
#[derive(Debug, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct PingReply {
    /// Indicates whether the request was handled successfully.
    pub ok: bool,

    /// Human-readable reply message.
    pub message: String,
}
