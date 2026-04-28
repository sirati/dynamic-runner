/// Aggregate processing statistics.
#[derive(Debug, Clone, Default)]
pub struct ProcessingStats {
    pub completed: u32,
    pub total: u32,
    pub errored: u32,
    pub skipped: u32,
}
