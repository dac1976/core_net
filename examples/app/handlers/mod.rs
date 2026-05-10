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

/// Example message handler demonstrating deferred/background work.
pub mod deferred;

/// Simple echo request/reply handler.
pub mod echo;

/// Example MessagePack-based ping handler.
pub mod msgpack_ping;

/// Simple raw ping request/reply handler.
pub mod ping;

use core_net::messaging::dispatcher::MessageDispatcherBuilder;

/// Registers all example/demo message handlers with the dispatcher.
///
/// This module demonstrates the intended usage pattern for the
/// `MessageDispatcherBuilder`:
///
/// ```text
/// create dispatcher builder
///   -> register handlers by message id
///   -> optionally set default handler
///   -> build dispatcher
/// ```
///
/// Current message id mappings:
///
/// ```text
/// 1 -> ping
/// 2 -> echo
/// 3 -> deferred
/// 4 -> msgpack_ping
/// ```
///
/// A default handler is also installed to catch unregistered message ids.
///
/// This is useful for:
///
/// - debugging
/// - protocol diagnostics
/// - unknown message logging
/// - development/testing
pub fn register_handlers(builder: &mut MessageDispatcherBuilder) {
    // Register simple raw ping handler.
    ping::register(builder);

    // Register echo request/reply handler.
    echo::register(builder);

    // Register deferred/background work handler.
    deferred::register(builder);

    // Register MessagePack-based ping handler.
    msgpack_ping::register(builder);

    // Install default handler for unregistered message ids.
    //
    // This prevents unknown messages from silently disappearing and is
    // especially useful during protocol development and debugging.
    builder.set_default(
        |ctx: core_net::messaging::dispatcher::MessageContext,
         message: core_net::messaging::message::Message| async move {
            tracing::warn!(
                message_id = message.header.message_id,
                source_addr = ?ctx.source_addr,
                "default handler invoked for unregistered message id"
            );
        },
    );
}
