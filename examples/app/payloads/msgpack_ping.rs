use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct PingRequest {
    pub name: String,
    pub count: u32,
}

#[derive(Debug, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct PingReply {
    pub ok: bool,
    pub message: String,
}
