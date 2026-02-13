use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct TableStats {
    pub rows: Option<u64>,
    pub bytes: Option<u64>,
}
