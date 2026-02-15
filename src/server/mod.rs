//! TCP server for the streaming protocol.

mod handler;
mod kafka_handler;

pub use handler::run_server;
pub use kafka_handler::{run_kafka_server, run_kafka_server_on_listener};
