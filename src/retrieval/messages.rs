use std::sync::Arc;
use serde_json::Value;

/// Define message types for our channels
#[derive(Clone)]
pub enum RetrievalMessage {
    Batch(Arc<Value>),
    Done,
} 
