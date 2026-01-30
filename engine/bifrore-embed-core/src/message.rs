use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
    pub dup: bool,
    pub timestamp_millis: u64,
    pub client_id: Option<String>,
    pub username: Option<String>,
    pub properties: HashMap<String, String>,
}

impl Message {
    pub fn new(topic: impl Into<String>, payload: Vec<u8>) -> Self {
        Self {
            topic: topic.into(),
            payload,
            qos: 0,
            retain: false,
            dup: false,
            timestamp_millis: 0,
            client_id: None,
            username: None,
            properties: HashMap::new(),
        }
    }
}
