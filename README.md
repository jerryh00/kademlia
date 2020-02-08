# kademlia
A kademlia implementation in Rust.

# How to run
In console for server:
cargo run --bin server -- -t

In console for client node:
cargo run --bin node -- -t

# For debug output
In console for server:
RUST_LOG=debug cargo run --bin server -- -t

In console for client node:
RUST_LOG=debug cargo run --bin node -- -t

# For debug node with many peers running
In console for server and peers:
./test/gen_network_for_debug.py

In console for client node:
RUST_LOG=debug cargo run --bin node -- -t
