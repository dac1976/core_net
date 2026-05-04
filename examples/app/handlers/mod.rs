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

pub mod deferred;
pub mod echo;
pub mod msgpack_ping;
pub mod ping;

use core_net::messaging::dispatcher::MessageDispatcherBuilder;

pub fn register_handlers(builder: &mut MessageDispatcherBuilder) {
    ping::register(builder);
    echo::register(builder);
    deferred::register(builder);
    msgpack_ping::register(builder);

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
