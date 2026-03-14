pub mod types;
pub mod traits;

pub use traits::{MessageReceiver, MessageSender};
pub use types::{BinaryInfo, ErrorType, FailedTask, Identifier, MemoryBytes, TaskResult, WorkerId};
