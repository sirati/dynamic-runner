use serde::{Deserialize, Serialize};

/// The concrete identifier type for the tokenizer task.
///
/// This is the task-specific struct that was previously hardcoded as
/// `BinaryIdentifier` in `db_comm_api_base`. Different task definitions
/// can define their own identifier types implementing the `Identifier`
/// trait (Clone + Debug + Hash + Eq + Serialize + Deserialize + Send + 'static).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TokenizerIdentifier {
    pub binary_name: String,
    pub platform: String,
    pub compiler: String,
    pub version: String,
    pub opt_level: String,
}
