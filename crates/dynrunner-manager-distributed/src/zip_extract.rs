//! ZIP extraction and file hash utilities for the secondary coordinator.
//!
//! Supports two modes:
//! - **File-ready mode**: binaries referenced by path (no extraction needed)
//! - **ZIP mode**: binaries extracted from ZIP archives and cached by hash

use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// Cache of extracted binaries: file_hash -> extracted path.
///
/// Avoids re-extracting the same binary from ZIP files multiple times.
pub struct ExtractionCache {
    /// hash -> extracted file path
    extracted: HashMap<String, PathBuf>,
    /// Temporary directory for extracted files
    tmp_dir: PathBuf,
    /// Network source directory containing ZIP files
    src_network: Option<PathBuf>,
}

impl ExtractionCache {
    pub fn new(tmp_dir: PathBuf, src_network: Option<PathBuf>) -> Self {
        if let Err(e) = std::fs::create_dir_all(&tmp_dir) {
            tracing::warn!(dir = %tmp_dir.display(), error = %e, "failed to create tmp dir");
        }
        Self {
            extracted: HashMap::new(),
            tmp_dir,
            src_network,
        }
    }

    /// Get a file by hash, checking cache first then trying direct path.
    ///
    /// `expected_content_hash` is optional:
    ///   - `Some(h)` — verify SHA256 of the candidate matches `h`;
    ///     reject candidates whose content doesn't. Used in normal
    ///     mode where the primary computed `h` while staging.
    ///   - `None` — skip content verification; accept the first
    ///     candidate that exists. Used in pre-staged-source mode
    ///     (the bind-mount IS the contract; no transfer happened so
    ///     there's nothing to dedup).
    ///
    /// `lookup_key` is always the cache key (a `compute_task_hash`
    /// 16-char hex). Even with no content verification, the cache
    /// hit/miss + register path uses it.
    pub fn get_file_by_hash(
        &mut self,
        lookup_key: &str,
        file_path: Option<&str>,
        expected_content_hash: Option<&str>,
    ) -> Option<PathBuf> {
        // Check cache
        if let Some(path) = self.extracted.get(lookup_key) {
            if path.exists() {
                return Some(path.clone());
            }
            // Cached path no longer exists, remove stale entry
            self.extracted.remove(lookup_key);
        }

        // Returns true iff the candidate's content matches the
        // expected hash (or no expectation was given).
        let verify = |path: &Path| -> bool {
            match expected_content_hash {
                None => true,
                Some(expected) => compute_file_hash(path)
                    .map(|computed| computed == expected)
                    .unwrap_or(false),
            }
        };

        if let Some(path_str) = file_path {
            let direct_path = PathBuf::from(path_str);

            // Try absolute path (local/file-ready / pre-staged mode).
            if direct_path.is_absolute() && direct_path.exists() && verify(&direct_path) {
                self.extracted
                    .insert(lookup_key.to_string(), direct_path.clone());
                return Some(direct_path);
            }

            // Try relative to tmp_dir
            let file_name = direct_path.file_name().unwrap_or(direct_path.as_os_str());
            let relative_path = self.tmp_dir.join(file_name);
            if relative_path.exists() && verify(&relative_path) {
                self.extracted
                    .insert(lookup_key.to_string(), relative_path.clone());
                return Some(relative_path);
            }

            // Try src_network fallbacks (when configured): the primary
            // may have placed the file on the shared drive without
            // sending an explicit StageFile, OR the wrapper bind-mount
            // exposes the pre-staged data here. Try
            // `src_network/<basename>` and `src_network/<full local_path>`.
            if let Some(net) = self.src_network.as_ref() {
                let base_in_net = net.join(file_name);
                if base_in_net.exists() && verify(&base_in_net) {
                    self.extracted
                        .insert(lookup_key.to_string(), base_in_net.clone());
                    return Some(base_in_net);
                }
                let full_in_net = net.join(path_str);
                if full_in_net != base_in_net && full_in_net.exists() && verify(&full_in_net) {
                    self.extracted
                        .insert(lookup_key.to_string(), full_in_net.clone());
                    return Some(full_in_net);
                }
            }
        }

        None
    }

    /// Extract a binary from a ZIP file.
    ///
    /// Returns the path to the extracted file, or `None` on failure.
    /// Results are cached by `file_hash`.
    pub fn extract_from_zip(
        &mut self,
        zip_name: &str,
        local_path: &str,
        file_hash: &str,
    ) -> Option<PathBuf> {
        // Check cache
        if let Some(path) = self.extracted.get(file_hash)
            && path.exists()
        {
            return Some(path.clone());
        }

        let src_network = self.src_network.as_ref()?;
        let zip_path = src_network.join(zip_name);
        if !zip_path.exists() {
            tracing::error!(zip = %zip_path.display(), "ZIP file not found");
            return None;
        }

        // Open ZIP and extract the specific file
        let zip_file = match std::fs::File::open(&zip_path) {
            Ok(f) => f,
            Err(e) => {
                tracing::error!(zip = %zip_path.display(), error = %e, "failed to open ZIP");
                return None;
            }
        };

        let mut archive = match zip::ZipArchive::new(zip_file) {
            Ok(a) => a,
            Err(e) => {
                tracing::error!(zip = %zip_path.display(), error = %e, "failed to read ZIP");
                return None;
            }
        };

        let mut zip_entry = match archive.by_name(local_path) {
            Ok(e) => e,
            Err(e) => {
                tracing::error!(
                    zip = %zip_path.display(),
                    entry = local_path,
                    error = %e,
                    "entry not found in ZIP"
                );
                return None;
            }
        };

        // Extract to tmp_dir using the file name
        let file_name = Path::new(local_path)
            .file_name()
            .unwrap_or(std::ffi::OsStr::new(local_path));
        let extracted_path = self.tmp_dir.join(file_name);

        let mut output = match std::fs::File::create(&extracted_path) {
            Ok(f) => f,
            Err(e) => {
                tracing::error!(path = %extracted_path.display(), error = %e, "failed to create output file");
                return None;
            }
        };

        if let Err(e) = std::io::copy(&mut zip_entry, &mut output) {
            tracing::error!(
                zip = %zip_path.display(),
                entry = local_path,
                error = %e,
                "failed to extract from ZIP"
            );
            let _ = std::fs::remove_file(&extracted_path);
            return None;
        }

        // Verify hash
        if let Some(computed) = compute_file_hash(&extracted_path) {
            if computed != file_hash {
                tracing::error!(
                    expected = file_hash,
                    computed = %computed,
                    entry = local_path,
                    "hash mismatch after extraction"
                );
                let _ = std::fs::remove_file(&extracted_path);
                return None;
            }
        } else {
            tracing::error!(path = %extracted_path.display(), "failed to compute hash");
            let _ = std::fs::remove_file(&extracted_path);
            return None;
        }

        self.extracted
            .insert(file_hash.to_string(), extracted_path.clone());
        tracing::debug!(
            zip = zip_name,
            entry = local_path,
            hash = file_hash,
            "extracted binary from ZIP"
        );
        Some(extracted_path)
    }

    /// Register a direct file path (file-ready mode, no extraction).
    pub fn register_path(&mut self, file_hash: &str, path: PathBuf) {
        self.extracted.insert(file_hash.to_string(), path);
    }

    /// Local scratch directory backing this cache.
    pub fn tmp_dir(&self) -> &Path {
        &self.tmp_dir
    }

    /// Resolve a binary: try file-ready mode first, then ZIP extraction.
    ///
    /// If `zip_name` is empty or `None`, uses file-ready mode (direct
    /// path). Otherwise extracts from ZIP.
    ///
    /// `file_hash` (the cache lookup key, a `compute_task_hash`
    /// value) is always required. `expected_content_hash` is the
    /// SHA256 the primary computed at staging time; pass `None` when
    /// no transfer happened (pre-staged-source mode) so the resolver
    /// accepts the bind-mounted file by existence alone. The hash
    /// machinery is a network-transfer-dedup optimisation; with no
    /// transfer there's nothing to dedup, and demanding a content
    /// hash that the primary never computed is the same fail-mode
    /// bf1ce02 / a344b0e were both about.
    pub fn resolve_binary(
        &mut self,
        zip_name: Option<&str>,
        local_path: &str,
        file_hash: &str,
        expected_content_hash: Option<&str>,
    ) -> Option<PathBuf> {
        // File-ready mode: zip_name is empty or None
        if zip_name.is_none_or(|z| z.is_empty()) {
            return self.get_file_by_hash(file_hash, Some(local_path), expected_content_hash);
        }

        // ZIP mode (always content-verified — extraction touches
        // the file, hash on extracted content is the integrity
        // check the cluster relies on).
        self.extract_from_zip(zip_name.unwrap(), local_path, file_hash)
    }
}

/// Compute SHA256 hash of a file's contents.
pub fn compute_file_hash(path: &Path) -> Option<String> {
    let mut file = std::fs::File::open(path).ok()?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 65536]; // 64KB chunks, matching Python
    loop {
        let n = file.read(&mut buffer).ok()?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }
    Some(format!("{:x}", hasher.finalize()))
}

/// Compute task hash from binary metadata (matches Python's _compute_task_hash).
pub fn compute_task_hash_from_parts(path: &str, fields: &[&str]) -> String {
    let hash_input = std::iter::once(path)
        .chain(fields.iter().copied())
        .collect::<Vec<_>>()
        .join(":");
    let hash = Sha256::digest(hash_input.as_bytes());
    format!("{:x}", hash)[..16].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn compute_file_hash_works() {
        let dir = std::env::temp_dir().join(format!("zip_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("test.bin");
        std::fs::write(&file_path, b"hello world").unwrap();

        let hash = compute_file_hash(&file_path).unwrap();
        // SHA256 of "hello world"
        assert_eq!(
            hash,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn task_hash_from_parts() {
        let hash =
            compute_task_hash_from_parts("path/to/bin", &["binname", "x64", "gcc", "5", "O0"]);
        assert_eq!(hash.len(), 16);
    }

    #[test]
    fn extraction_cache_file_ready_mode() {
        let dir = std::env::temp_dir().join(format!("cache_test_{}", std::process::id()));
        let tmp = dir.join("tmp");
        std::fs::create_dir_all(&tmp).unwrap();

        let test_file = dir.join("test.bin");
        std::fs::write(&test_file, b"test data").unwrap();
        let hash = compute_file_hash(&test_file).unwrap();

        let mut cache = ExtractionCache::new(tmp, None);

        // Should find by direct path (with content verification)
        let result = cache.get_file_by_hash(&hash, Some(test_file.to_str().unwrap()), Some(&hash));
        assert!(result.is_some());

        // Should be cached now (no path needed)
        let result2 = cache.get_file_by_hash(&hash, None, None);
        assert!(result2.is_some());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn extraction_cache_zip_mode() {
        let dir = std::env::temp_dir().join(format!("zip_cache_test_{}", std::process::id()));
        let tmp = dir.join("tmp");
        let src = dir.join("src_network");
        std::fs::create_dir_all(&tmp).unwrap();
        std::fs::create_dir_all(&src).unwrap();

        // Create a small ZIP file
        let zip_path = src.join("test.zip");
        let zip_file = std::fs::File::create(&zip_path).unwrap();
        let mut zip_writer = zip::ZipWriter::new(zip_file);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        zip_writer.start_file("inner/binary.bin", options).unwrap();
        zip_writer.write_all(b"binary content").unwrap();
        zip_writer.finish().unwrap();

        // Compute expected hash of "binary content"
        let expected_hash = {
            let h = Sha256::digest(b"binary content");
            format!("{:x}", h)
        };

        let mut cache = ExtractionCache::new(tmp, Some(src));

        let result = cache.extract_from_zip("test.zip", "inner/binary.bin", &expected_hash);
        assert!(result.is_some());
        let extracted = result.unwrap();
        assert_eq!(std::fs::read(&extracted).unwrap(), b"binary content");

        // Should be cached
        let result2 = cache.extract_from_zip("test.zip", "inner/binary.bin", &expected_hash);
        assert!(result2.is_some());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_via_src_network_basename() {
        let dir = std::env::temp_dir().join(format!("net_basename_test_{}", std::process::id()));
        let tmp = dir.join("tmp");
        let net = dir.join("src_network");
        std::fs::create_dir_all(&tmp).unwrap();
        std::fs::create_dir_all(&net).unwrap();

        let payload = b"shared-drive payload A";
        let basename = "binary.bin";
        let staged = net.join(basename);
        std::fs::write(&staged, payload).unwrap();
        let hash = compute_file_hash(&staged).unwrap();

        let mut cache = ExtractionCache::new(tmp, Some(net));
        // local_path the primary thinks the file is at — does not exist
        // here; resolution must fall through to src_network/<basename>.
        let primary_view = format!("/nonexistent/dir/{}", basename);
        let result = cache.resolve_binary(None, &primary_view, &hash, Some(&hash));
        assert!(
            result.is_some(),
            "expected resolution via src_network/<basename>"
        );
        assert_eq!(result.unwrap(), staged);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_via_src_network_local_path() {
        let dir = std::env::temp_dir().join(format!("net_localpath_test_{}", std::process::id()));
        let tmp = dir.join("tmp");
        let net = dir.join("src_network");
        std::fs::create_dir_all(&tmp).unwrap();
        std::fs::create_dir_all(&net).unwrap();

        let payload = b"shared-drive payload B";
        // The relative local_path the primary sends; place the file at
        // src_network/<full local_path>, NOT at src_network/<basename>.
        let rel = "subdir/inner/binary2.bin";
        let staged = net.join(rel);
        std::fs::create_dir_all(staged.parent().unwrap()).unwrap();
        std::fs::write(&staged, payload).unwrap();
        let hash = compute_file_hash(&staged).unwrap();

        let mut cache = ExtractionCache::new(tmp, Some(net));
        let result = cache.resolve_binary(None, rel, &hash, Some(&hash));
        assert!(
            result.is_some(),
            "expected resolution via src_network/<full local_path>"
        );
        assert_eq!(result.unwrap(), staged);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
