import hashlib
import logging
import secrets
import zipfile
from pathlib import Path
from typing import Any

from ..binary_info import BinaryInfo

logger = logging.getLogger(__name__)


class FileDistributor:
    """Handles intelligent file distribution with deduplication"""

    def __init__(self, srcbins_dir: Path):
        self.srcbins_dir = Path(srcbins_dir)
        self.discovered_hashes: dict[str, tuple[str, str]] = {}  # hash -> (zip_name, local_path)
        self.sent_hashes: set[str] = set()

    def register_discovered_binary(self, hash_str: str, zip_name: str, local_path: str) -> None:
        """Register a binary discovered by first secondary

        Args:
            hash_str: Hash of the binary file
            zip_name: Name of ZIP file containing the binary
            local_path: Path within ZIP to the binary
        """
        self.discovered_hashes[hash_str] = (zip_name, local_path)
        logger.debug(f"Registered discovered binary: {hash_str[:8]} in {zip_name}")

    def compute_binary_hash(self, binary_path: Path) -> str:
        """Compute SHA256 hash of binary file

        Args:
            binary_path: Path to binary file

        Returns:
            Hex string of hash
        """
        hasher = hashlib.sha256()

        with open(binary_path, "rb") as f:
            while chunk := f.read(65536):  # 64KB chunks
                hasher.update(chunk)

        return hasher.hexdigest()

    def is_already_sent(self, binary_path: Path) -> tuple[bool, str | None]:
        """Check if binary was already sent (discovered by first secondary)

        Args:
            binary_path: Path to binary file

        Returns:
            (is_sent, hash) tuple
        """
        try:
            hash_str = self.compute_binary_hash(binary_path)

            if hash_str in self.discovered_hashes:
                logger.debug(f"Binary {binary_path.name} already sent (hash: {hash_str[:8]})")
                return True, hash_str

            return False, hash_str

        except Exception as e:
            logger.warning(f"Failed to hash {binary_path}: {e}")
            return False, None

    def create_distribution_zip(
        self,
        binaries: list[BinaryInfo],
        secondary_id: str,
        base_dir: Path,
    ) -> tuple[Path, dict[str, tuple[str, bool]]]:
        """Create ZIP file for distribution to secondary

        This creates an uncompressed ZIP of binaries not already sent to the
        first secondary. Returns the ZIP path and metadata about included files.

        Args:
            binaries: List of binaries to include
            secondary_id: ID of target secondary
            base_dir: Base directory containing binary files

        Returns:
            (zip_path, file_metadata) tuple where file_metadata maps
            binary_path -> (hash, already_sent)
        """
        # Generate unique ZIP name
        random_suffix = secrets.token_hex(4)
        zip_name = f"dist_{secondary_id}_{random_suffix}.zip"
        zip_path = self.srcbins_dir / zip_name

        # Ensure srcbins directory exists
        self.srcbins_dir.mkdir(parents=True, exist_ok=True)

        file_metadata: dict[str, tuple[str, bool]] = {}
        files_to_zip: list[tuple[Path, str]] = []

        logger.info(f"Creating distribution ZIP for {secondary_id}: {len(binaries)} binaries")

        # Check each binary
        for binary in binaries:
            binary_path = base_dir / binary.path

            if not binary_path.exists():
                logger.warning(f"Binary not found: {binary_path}")
                continue

            # Check if already sent
            already_sent, hash_str = self.is_already_sent(binary_path)

            if hash_str:
                file_metadata[str(binary.path)] = (hash_str, already_sent)

            if not already_sent:
                # Add to ZIP
                arcname = str(binary.path)
                files_to_zip.append((binary_path, arcname))

                if hash_str:
                    self.sent_hashes.add(hash_str)

        # Create ZIP with no compression (store only)
        logger.info(f"Writing {len(files_to_zip)} new binaries to {zip_path.name}")

        total_size = 0
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_STORED) as zf:
            for file_path, arcname in files_to_zip:
                zf.write(file_path, arcname)
                total_size += file_path.stat().st_size

        logger.info(f"ZIP created: {zip_path.name} ({total_size / (1024**2):.1f} MB)")

        return zip_path, file_metadata

    def create_batched_zips(
        self,
        binaries: list[BinaryInfo],
        secondary_id: str,
        base_dir: Path,
        min_batch_size_mb: float = 20.0,
    ) -> list[tuple[Path, list[BinaryInfo]]]:
        """Create batched ZIPs with minimum size for efficient transfer

        This batches binaries into ZIPs of at least min_batch_size_mb.
        Large binaries (>min_batch_size_mb) get their own ZIP.

        Args:
            binaries: List of binaries to distribute
            secondary_id: ID of target secondary
            base_dir: Base directory containing binary files
            min_batch_size_mb: Minimum batch size in MB

        Returns:
            List of (zip_path, binaries_in_zip) tuples
        """
        logger.info(
            f"Creating batched ZIPs for {secondary_id} ({len(binaries)} binaries, {min_batch_size_mb}MB batches)"
        )

        min_batch_size_bytes = int(min_batch_size_mb * 1024 * 1024)

        # Separate binaries by size and deduplication status
        to_send: list[tuple[BinaryInfo, Path, int]] = []  # (binary, path, size)

        for binary in binaries:
            binary_path = base_dir / binary.path

            if not binary_path.exists():
                logger.warning(f"Binary not found: {binary_path}")
                continue

            already_sent, hash_str = self.is_already_sent(binary_path)

            if not already_sent:
                file_size = binary_path.stat().st_size
                to_send.append((binary, binary_path, file_size))

                if hash_str:
                    self.sent_hashes.add(hash_str)

        logger.info(f"Sending {len(to_send)} new binaries (skipped {len(binaries) - len(to_send)} duplicates)")

        # Sort by size (largest first) for better batching
        to_send.sort(key=lambda x: x[2], reverse=True)

        batches: list[tuple[Path, list[BinaryInfo]]] = []
        current_batch: list[tuple[BinaryInfo, Path]] = []
        current_batch_size = 0
        batch_num = 0

        for binary, binary_path, file_size in to_send:
            # If binary is larger than min batch size, give it its own ZIP
            if file_size >= min_batch_size_bytes:
                # Flush current batch if not empty
                if current_batch:
                    zip_path = self._create_zip_from_batch(current_batch, secondary_id, batch_num)
                    binaries_in_zip = [b for b, _ in current_batch]
                    batches.append((zip_path, binaries_in_zip))
                    batch_num += 1
                    current_batch = []
                    current_batch_size = 0

                # Create single-file ZIP
                zip_path = self._create_zip_from_batch([(binary, binary_path)], secondary_id, batch_num)
                batches.append((zip_path, [binary]))
                batch_num += 1

            else:
                # Add to current batch
                current_batch.append((binary, binary_path))
                current_batch_size += file_size

                # If batch is large enough, flush it
                if current_batch_size >= min_batch_size_bytes:
                    zip_path = self._create_zip_from_batch(current_batch, secondary_id, batch_num)
                    binaries_in_zip = [b for b, _ in current_batch]
                    batches.append((zip_path, binaries_in_zip))
                    batch_num += 1
                    current_batch = []
                    current_batch_size = 0

        # Flush remaining batch
        if current_batch:
            zip_path = self._create_zip_from_batch(current_batch, secondary_id, batch_num)
            binaries_in_zip = [b for b, _ in current_batch]
            batches.append((zip_path, binaries_in_zip))

        logger.info(f"Created {len(batches)} ZIP batches for {secondary_id}")

        return batches

    def _create_zip_from_batch(
        self,
        batch: list[tuple[BinaryInfo, Path]],
        secondary_id: str,
        batch_num: int,
    ) -> Path:
        """Create a ZIP file from a batch of binaries

        Args:
            batch: List of (binary_info, binary_path) tuples
            secondary_id: ID of target secondary
            batch_num: Batch number for naming

        Returns:
            Path to created ZIP file
        """
        # Generate ZIP name
        random_suffix = secrets.token_hex(4)
        zip_name = f"batch_{secondary_id}_{batch_num:03d}_{random_suffix}.zip"
        zip_path = self.srcbins_dir / zip_name

        # Ensure srcbins directory exists
        self.srcbins_dir.mkdir(parents=True, exist_ok=True)

        # Create ZIP with no compression (store only)
        total_size = 0
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_STORED) as zf:
            for binary, binary_path in batch:
                arcname = str(binary.path)
                zf.write(binary_path, arcname)
                total_size += binary_path.stat().st_size

        logger.debug(f"Created batch ZIP: {zip_name} ({len(batch)} files, {total_size / (1024**2):.1f} MB)")

        return zip_path

    def extract_binaries_from_zip(
        self,
        zip_path: Path,
        extract_dir: Path,
        file_list: list[str] | None = None,
    ) -> list[Path]:
        """Extract binaries from ZIP to directory

        Args:
            zip_path: Path to ZIP file
            extract_dir: Directory to extract to
            file_list: Optional list of specific files to extract (None = all)

        Returns:
            List of extracted file paths
        """
        logger.info(f"Extracting binaries from {zip_path.name} to {extract_dir}")

        extract_dir.mkdir(parents=True, exist_ok=True)
        extracted: list[Path] = []

        with zipfile.ZipFile(zip_path, "r") as zf:
            if file_list:
                # Extract specific files
                for filename in file_list:
                    zf.extract(filename, extract_dir)
                    extracted.append(extract_dir / filename)
            else:
                # Extract all files
                zf.extractall(extract_dir)
                extracted = [extract_dir / name for name in zf.namelist()]

        logger.info(f"Extracted {len(extracted)} files")

        return extracted

    def scan_srcbins_for_hashes(self, srcbins_dir: Path) -> dict[str, tuple[str, str, str]]:
        """Scan srcbins directory for existing ZIPs and compute hashes

        This is used by the first secondary to discover already-available binaries.

        Args:
            srcbins_dir: Directory containing ZIP files

        Returns:
            Dict mapping hash -> (zip_name, local_path_in_zip, hash)
        """
        logger.info(f"Scanning {srcbins_dir} for existing binaries...")

        discovered: dict[str, tuple[str, str, str]] = {}
        zip_files = list(srcbins_dir.glob("*.zip"))

        logger.info(f"Found {len(zip_files)} ZIP files to scan")

        for zip_path in zip_files:
            # Check for corresponding .hash file
            hash_file = zip_path.with_suffix(".zip.hash")

            if not hash_file.exists():
                logger.debug(f"No hash file for {zip_path.name}, skipping")
                continue

            logger.debug(f"Scanning {zip_path.name}...")

            try:
                with zipfile.ZipFile(zip_path, "r") as zf:
                    for member in zf.namelist():
                        # Extract to temp location and hash
                        binary_data = zf.read(member)

                        hasher = hashlib.sha256()
                        hasher.update(binary_data)
                        hash_str = hasher.hexdigest()

                        discovered[hash_str] = (zip_path.name, member, hash_str)

            except Exception as e:
                logger.warning(f"Failed to scan {zip_path.name}: {e}")

        logger.info(f"Discovered {len(discovered)} binaries in srcbins")

        return discovered
