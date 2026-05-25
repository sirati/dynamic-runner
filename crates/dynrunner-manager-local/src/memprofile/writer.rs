//! Per-task JSONL + zstd writer.
//!
//! Each `write_sample_as_frame` call emits ONE self-contained zstd
//! frame containing exactly one JSONL line (`{...}\n`). The file is
//! a concatenation of independent frames — `zstd -dc <file>` reads
//! all complete frames and stops cleanly at the last one. If the
//! manager dies mid-flight the file may end with partial bytes from
//! one half-written frame; the decoder truncates at the last
//! complete frame and the consumer loses at most one sample.
//!
//! Per-frame overhead is ~20 bytes (header + footer); at 1Hz × ~20
//! workers the absolute overhead is well under 1 KB/s/run.

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use super::error::MemProfileError;
use super::sample::Sample;

const ZSTD_LEVEL: i32 = 3;

pub struct JsonlZstdWriter {
    path: PathBuf,
    file: File,
}

impl JsonlZstdWriter {
    /// Open (or create, truncating any existing content) the file at
    /// `path`. The parent directory is created with `create_dir_all`
    /// — `task_id` may contain slashes for asm-tokenizer, so the
    /// final file lives at arbitrary depth under
    /// `{output_dir}/memprofile/`.
    pub fn open(path: &Path) -> Result<Self, MemProfileError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| MemProfileError::io(parent.to_path_buf(), e))?;
        }
        let file = File::create(path).map_err(|e| MemProfileError::io(path.to_path_buf(), e))?;
        Ok(Self {
            path: path.to_path_buf(),
            file,
        })
    }

    /// Append one self-contained zstd frame containing one JSONL
    /// line for `sample`. Each call is one open(Encoder) →
    /// write(json + '\n') → finish() sequence so the frame is
    /// fully flushed to disk before returning.
    pub fn write_sample_as_frame(&mut self, sample: &Sample) -> Result<(), MemProfileError> {
        let json = serde_json::to_vec(sample)
            .map_err(|e| MemProfileError::serialize(self.path.clone(), e.to_string()))?;

        let mut encoder = zstd::stream::write::Encoder::new(&mut self.file, ZSTD_LEVEL)
            .map_err(|e| MemProfileError::io(self.path.clone(), e))?;
        encoder
            .write_all(&json)
            .map_err(|e| MemProfileError::io(self.path.clone(), e))?;
        encoder
            .write_all(b"\n")
            .map_err(|e| MemProfileError::io(self.path.clone(), e))?;
        encoder
            .finish()
            .map_err(|e| MemProfileError::io(self.path.clone(), e))?;
        Ok(())
    }

    /// Best-effort flush; closing the writer (drop) is sufficient
    /// for correctness because every frame is finalised in
    /// `write_sample_as_frame`. Provided for callers that want an
    /// explicit fsync-point.
    pub fn close(mut self) -> Result<(), MemProfileError> {
        self.file
            .flush()
            .map_err(|e| MemProfileError::io(self.path, e))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs::File;
    use std::io::Read;
    use std::process::Command;

    use super::*;

    fn make_sample(t_ns: u64) -> Sample {
        let mut memory_stat = BTreeMap::new();
        memory_stat.insert("anon".to_string(), 100 * t_ns);
        memory_stat.insert("file".to_string(), 10 * t_ns);
        Sample {
            t_ns,
            t_rel_ns: t_ns * 1_000,
            worker_id: 7,
            memory_current: 1024 * t_ns,
            swap_current: 0,
            memory_stat,
        }
    }

    /// `task_id` for asm-tokenizer takes the form
    /// `nping/x86/clang/9/Os`; the writer must create every parent
    /// directory of the final file before opening it.
    #[test]
    fn writes_nested_subdirs_for_slash_task_id() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let nested = tmp
            .path()
            .join("memprofile")
            .join("nping")
            .join("x86")
            .join("clang")
            .join("9")
            .join("Os.worker-3.memprofile.jsonl.zst");

        let writer = JsonlZstdWriter::open(&nested).expect("open nested path");
        drop(writer);

        assert!(nested.exists(), "file should exist after open");
        assert!(
            nested.parent().unwrap().is_dir(),
            "parent dirs should be created"
        );
    }

    /// Three independent frames in one file decode back to three
    /// JSONL lines in order. Documents the wire format.
    #[test]
    fn round_trip_three_samples() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("round-trip.jsonl.zst");

        let mut writer = JsonlZstdWriter::open(&path).expect("open");
        writer
            .write_sample_as_frame(&make_sample(1))
            .expect("write 1");
        writer
            .write_sample_as_frame(&make_sample(2))
            .expect("write 2");
        writer
            .write_sample_as_frame(&make_sample(3))
            .expect("write 3");
        writer.close().expect("close");

        let mut decoded = Vec::new();
        let file = File::open(&path).expect("open for read");
        let mut decoder = zstd::stream::read::Decoder::new(file).expect("decoder");
        decoder
            .read_to_end(&mut decoded)
            .expect("decode all frames");

        let lines: Vec<&str> = std::str::from_utf8(&decoded)
            .expect("utf8")
            .split_terminator('\n')
            .collect();
        assert_eq!(lines.len(), 3, "three samples should yield three lines");

        for (idx, line) in lines.iter().enumerate() {
            let expected_t_ns = (idx as u64) + 1;
            let parsed: serde_json::Value =
                serde_json::from_str(line).expect("each line is valid JSON");
            assert_eq!(
                parsed["t_ns"].as_u64().expect("t_ns is u64"),
                expected_t_ns,
                "line {idx} t_ns mismatch"
            );
        }
    }

    /// A half-written final frame must not destroy the preceding
    /// complete frames. Documents the per-frame-recoverability
    /// invariant that motivates writing one frame per sample.
    #[test]
    fn recovers_after_truncation_at_partial_frame() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("partial.jsonl.zst");

        let mut writer = JsonlZstdWriter::open(&path).expect("open");
        writer
            .write_sample_as_frame(&make_sample(1))
            .expect("write 1");
        writer
            .write_sample_as_frame(&make_sample(2))
            .expect("write 2");
        writer.close().expect("close");

        // Simulate a manager crash mid-frame: append junk bytes that
        // look like the start of a zstd frame but are truncated.
        {
            use std::io::Write as _;
            let mut tail = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .expect("reopen for append");
            tail.write_all(&[0x28, 0xB5, 0x2F, 0xFD, 0x00, 0x00, 0x00, 0x00])
                .expect("append junk");
        }

        // Decode in a loop: bytes returned before any error are still
        // valid; we count complete `\n`-terminated JSON lines.
        let file = File::open(&path).expect("open corrupted");
        let mut decoder = zstd::stream::read::Decoder::new(file).expect("decoder");
        let mut decoded = Vec::new();
        // read_to_end may return Err at the partial tail; bytes
        // written into `decoded` BEFORE the error are still valid.
        let _ = decoder.read_to_end(&mut decoded);

        let text = std::str::from_utf8(&decoded).expect("utf8 prefix");
        let complete_lines: Vec<&str> = text.split_terminator('\n').collect();
        assert!(
            complete_lines.len() >= 2,
            "expected >= 2 complete samples to survive corruption, got {}: {:?}",
            complete_lines.len(),
            complete_lines
        );

        for (idx, line) in complete_lines.iter().take(2).enumerate() {
            let expected_t_ns = (idx as u64) + 1;
            let parsed: serde_json::Value =
                serde_json::from_str(line).expect("survived line parses");
            assert_eq!(
                parsed["t_ns"].as_u64().unwrap(),
                expected_t_ns,
                "line {idx} t_ns mismatch"
            );
        }
    }

    /// External `zstd -dc` must decode the multi-frame file the same
    /// way our Rust decoder does. Documents that consumers using the
    /// CLI (e.g. analysis scripts) don't need any special handling
    /// for the per-frame layout.
    ///
    /// Skipped if the `zstd` CLI is not installed on the host.
    #[test]
    fn per_frame_independence_via_zstd_cli() {
        if Command::new("zstd").arg("-V").output().is_err() {
            eprintln!("skipping per_frame_independence_via_zstd_cli: `zstd` CLI not found");
            return;
        }

        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("cli.jsonl.zst");

        let mut writer = JsonlZstdWriter::open(&path).expect("open");
        for t_ns in 1..=3 {
            writer
                .write_sample_as_frame(&make_sample(t_ns))
                .expect("write");
        }
        writer.close().expect("close");

        let output = Command::new("zstd")
            .args(["-dc"])
            .arg(&path)
            .output()
            .expect("invoke zstd -dc");
        assert!(
            output.status.success(),
            "zstd -dc failed: status={:?} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );

        let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
        let lines: Vec<&str> = stdout.split_terminator('\n').collect();
        assert_eq!(
            lines.len(),
            3,
            "zstd -dc should yield three lines, got {}: {stdout}",
            lines.len()
        );
        for (idx, line) in lines.iter().enumerate() {
            let expected_t_ns = (idx as u64) + 1;
            let parsed: serde_json::Value =
                serde_json::from_str(line).expect("CLI-decoded line parses");
            assert_eq!(parsed["t_ns"].as_u64().unwrap(), expected_t_ns);
        }
    }
}
