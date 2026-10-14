"""Create immutable, byte-for-byte snapshots of source CSV files."""

from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import re


LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = PROJECT_ROOT / "data" / "raw"
BATCH_ROOT = PROJECT_ROOT / "data" / "batches"
SOURCE_FILES = {
    "grammys": "the_grammy_awards.csv",
    "spotify": "spotify_dataset.csv",
}


def _sha256(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size_bytes = 0
    with path.open("rb") as source_file:
        for chunk in iter(lambda: source_file.read(1024 * 1024), b""):
            digest.update(chunk)
            size_bytes += len(chunk)
    return digest.hexdigest(), size_bytes


def _write_json_atomically(path: Path, payload: dict[str, object]) -> None:
    temporary_path = path.with_name(f".{path.name}.tmp")
    try:
        temporary_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary_path, path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise


def extract_raw_batch(source_name: str, run_id: str) -> dict[str, object]:
    """Snapshot a known source file and return paths and immutable batch metadata."""
    if source_name not in SOURCE_FILES:
        raise ValueError(f"Unsupported source name: {source_name!r}")

    source_path = RAW_ROOT / SOURCE_FILES[source_name]
    if not source_path.is_file():
        raise FileNotFoundError(f"Raw source file does not exist: {source_path}")

    batch_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", run_id).strip("._-")
    if not batch_id:
        raise ValueError("Airflow run_id must produce a non-empty batch identifier.")

    batch_directory = BATCH_ROOT / source_name / batch_id
    batch_directory.mkdir(parents=True, exist_ok=True)
    raw_copy_path = batch_directory / source_path.name
    manifest_path = batch_directory / "manifest.json"
    report_path = batch_directory / "validation_report.json"

    if manifest_path.exists():
        source_sha256, source_size = _sha256(source_path)
        with manifest_path.open(encoding="utf-8") as manifest_file:
            manifest = json.load(manifest_file)
        copy_sha256, copy_size = _sha256(raw_copy_path)
        if (
            manifest.get("sha256") != source_sha256
            or manifest.get("size_bytes") != source_size
            or copy_sha256 != source_sha256
            or copy_size != source_size
        ):
            raise FileExistsError(
                f"Immutable batch {batch_id!r} exists but no longer matches "
                f"the source file {source_path}."
            )
        LOGGER.info(
            "Reusing unchanged raw batch %s for %s (sha256=%s).",
            batch_id,
            source_name,
            source_sha256,
        )
        return {
            **manifest,
            "path": str(raw_copy_path),
            "manifest_path": str(manifest_path),
            "report_path": str(report_path),
        }

    if raw_copy_path.exists():
        raise FileExistsError(
            f"Raw batch file already exists without a manifest: {raw_copy_path}"
        )

    temporary_copy_path = batch_directory / f".{source_path.name}.tmp"
    digest = hashlib.sha256()
    size_bytes = 0
    try:
        with source_path.open("rb") as source_file, temporary_copy_path.open(
            "wb"
        ) as destination_file:
            for chunk in iter(lambda: source_file.read(1024 * 1024), b""):
                destination_file.write(chunk)
                digest.update(chunk)
                size_bytes += len(chunk)
        os.replace(temporary_copy_path, raw_copy_path)
    except Exception:
        temporary_copy_path.unlink(missing_ok=True)
        raise

    manifest: dict[str, object] = {
        "source": source_name,
        "batch_id": batch_id,
        "run_id": run_id,
        "source_file": source_path.name,
        "source_path": str(source_path),
        "path": str(raw_copy_path),
        "manifest_path": str(manifest_path),
        "report_path": str(report_path),
        "sha256": digest.hexdigest(),
        "size_bytes": size_bytes,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
    }
    _write_json_atomically(manifest_path, manifest)
    LOGGER.info(
        "Extracted immutable raw batch %s for %s: bytes=%s, sha256=%s, path=%s.",
        batch_id,
        source_name,
        size_bytes,
        manifest["sha256"],
        raw_copy_path,
    )
    return manifest

