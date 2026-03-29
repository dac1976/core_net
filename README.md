# core_net
Core network library code implemented in Rust compatible with my C++ CoreLibrary.

This repo is a bit experimental and is being used by me to implement idiomatic Rust versions (hopefully) of code I have in my C++-based CoreLibrary repo.

So far I have Rust versions of my TCP, UDP (Multicast, Broadcast, Unicast) networking code from my C++ repo. The messaging for the networking code should be compatible with what I have done in the C++ repo.

This Rust code as dependencies on Tokio, rpm_serde and Miette, as well as a few others.
