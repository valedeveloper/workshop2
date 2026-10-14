"""Bootstrap, store, and extract Grammy source records through PostgreSQL."""

import csv
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import re
from typing import Any

import psycopg2
from psycopg2 import sql


LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
GRAMMYS_CSV_PATH = PROJECT_ROOT / "data" / "raw" / "the_grammy_awards.csv"
CLEANED_GRAMMYS_CSV_PATH = PROJECT_ROOT / "data" / "raw" / "the_grammy_awards_cleaned.csv"
BATCH_ROOT = PROJECT_ROOT / "data" / "batches" / "grammys"
GRAMMY_COLUMNS = (
    "year",
    "title",
    "published_at",
    "updated_at",
    "category",
    "nominee",
    "artist",
    "workers",
    "img",
    "winner",
)
DATABASE_NAME = os.getenv("DB_NAME", "workshop2")
TABLE_NAME = "grammys"
METADATA_TABLE_NAME = "grammys_source_load"
TABLE_COLUMNS = (
    ("year", "TEXT"),
    ("title", "TEXT"),
    ("published_at", "TEXT"),
    ("updated_at", "TEXT"),
    ("category", "TEXT"),
    ("nominee", "TEXT"),
    ("artist", "TEXT"),
    ("workers", "TEXT"),
    ("img", "TEXT"),
    ("winner", "TEXT"),
    ("source_row_number", "BIGINT NOT NULL PRIMARY KEY"),
    ("source_line_number", "BIGINT NOT NULL"),
    ("source_field_count", "INTEGER NOT NULL"),
    ("raw_fields", "JSONB NOT NULL"),
)


def _connection_parameters(database_name: str) -> dict[str, object]:
    required_variables = (
        "DB_HOST",
        "DB_PORT",
        "DB_USER",
        "DB_PASSWORD",
    )
    missing_variables = [
        variable for variable in required_variables if not os.getenv(variable)
    ]
    if missing_variables:
        raise RuntimeError(
            "Missing PostgreSQL connection settings: "
            + ", ".join(missing_variables)
        )

    return {
        "dbname": database_name,
        "host": os.environ["DB_HOST"],
        "port": int(os.environ["DB_PORT"]),
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
        "connect_timeout": 10,
    }


def connect_postgres(database_name: str) -> psycopg2.extensions.connection:
    return psycopg2.connect(**_connection_parameters(database_name))


def _sha256(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size_bytes = 0
    with path.open("rb") as source_file:
        for chunk in iter(lambda: source_file.read(1024 * 1024), b""):
            digest.update(chunk)
            size_bytes += len(chunk)
    return digest.hexdigest(), size_bytes


def _compute_cleaned_grammy_row(raw_row: list[str]) -> list[str] | None:
    """Repair the raw Grammy file enough to be safe for loading.

    The original dataset contains malformed rows and several null/blank values.
    We do not hide the issue: we preserve the original file as-is, but create a
    cleaned copy that only keeps structurally valid records. This is the required
    preprocessing before the raw batch reaches PostgreSQL.
    """
    if len(raw_row) != len(GRAMMY_COLUMNS):
        return None

    cleaned_row = []
    for index, value in enumerate(raw_row):
        normalized = (value or "").strip()
        if index in {0, 1, 3, 4, 5, 6, 7, 8, 9} and normalized == "":
            if index == 0:
                return None
            if index in {1, 3, 4, 6, 7, 8}:
                cleaned_row.append("")
                continue
        cleaned_row.append(normalized)

    year_value = cleaned_row[0]
    if year_value in ("", "nan", "NaN"):
        return None
    try:
        float(year_value)
    except ValueError:
        return None

    if cleaned_row[1] in ("", "nan", "NaN"):
        return None

    winner_value = cleaned_row[9].lower() if cleaned_row[9] else ""
    if winner_value in {"true", "t", "yes", "y", "1"}:
        cleaned_row[9] = "True"
    elif winner_value in {"false", "f", "no", "n", "0"}:
        cleaned_row[9] = "False"
    elif winner_value in {"", "nan", "none", "null"}:
        cleaned_row[9] = ""
    else:
        cleaned_row[9] = ""

    return cleaned_row


def clean_grammys_source(
    csv_path: Path = GRAMMYS_CSV_PATH,
    output_path: Path = CLEANED_GRAMMYS_CSV_PATH,
) -> Path:
    """Create a cleaned CSV that is safe to load into PostgreSQL.

    The original CSV is preserved in data/raw and the clean copy acts as the
    actual input for the project pipeline. This lets us retain the raw source and
    still satisfy the pre-load quality gate required by the ETL design.
    """
    if not csv_path.is_file():
        raise FileNotFoundError(f"Grammy bootstrap CSV does not exist: {csv_path}")

    cleaned_rows: list[list[str]] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as source_file:
        reader = csv.reader(source_file, strict=True)
        header = next(reader, None)
        if tuple(header or ()) != GRAMMY_COLUMNS:
            raise ValueError(
                "Grammy bootstrap CSV header does not match the expected source "
                f"schema. Expected {list(GRAMMY_COLUMNS)}, received {header}."
            )

        for raw_row in reader:
            cleaned_row = _compute_cleaned_grammy_row(raw_row)
            if cleaned_row is not None:
                cleaned_rows.append(cleaned_row)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as destination_file:
        writer = csv.writer(destination_file, lineterminator="\n")
        writer.writerow(GRAMMY_COLUMNS)
        writer.writerows(cleaned_rows)

    LOGGER.info(
        "Created cleaned Grammy source file %s with %s valid rows from %s.",
        output_path,
        len(cleaned_rows),
        csv_path,
    )
    return output_path


def _read_source_records(
    csv_path: Path,
) -> tuple[list[tuple[Any, ...]], list[int], str, int]:
    if not csv_path.is_file():
        raise FileNotFoundError(f"Grammy bootstrap CSV does not exist: {csv_path}")

    source_hash_before, _ = _sha256(csv_path)
    records: list[tuple[Any, ...]] = []
    malformed_physical_lines: list[int] = []

    with csv_path.open(encoding="utf-8-sig", newline="") as source_file:
        reader = csv.reader(source_file, strict=True)
        header = next(reader, None)
        if tuple(header or ()) != GRAMMY_COLUMNS:
            raise ValueError(
                "Grammy bootstrap CSV header does not match the expected source "
                f"schema. Expected {list(GRAMMY_COLUMNS)}, received {header}."
            )

        for source_row_number, row in enumerate(reader, start=1):
            source_line_number = reader.line_num
            if len(row) != len(GRAMMY_COLUMNS):
                malformed_physical_lines.append(source_line_number)

            mapped_values = row[: len(GRAMMY_COLUMNS)]
            mapped_values.extend([""] * (len(GRAMMY_COLUMNS) - len(mapped_values)))
            records.append(
                (
                    *mapped_values,
                    source_row_number,
                    source_line_number,
                    len(row),
                    json.dumps(row, ensure_ascii=False),
                )
            )

    source_hash_after, source_size = _sha256(csv_path)
    if source_hash_before != source_hash_after:
        raise RuntimeError(
            f"Grammy CSV changed while it was being read: {csv_path}"
        )
    if not records:
        raise ValueError(f"Grammy bootstrap CSV contains no data rows: {csv_path}")

    return records, malformed_physical_lines, source_hash_after, source_size


def _ensure_database() -> None:
    connection = connect_postgres("postgres")
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (DATABASE_NAME,),
            )
            if cursor.fetchone() is None:
                cursor.execute(
                    sql.SQL("CREATE DATABASE {}").format(
                        sql.Identifier(DATABASE_NAME)
                    )
                )
                LOGGER.info("Created PostgreSQL database %s.", DATABASE_NAME)
            else:
                LOGGER.info("PostgreSQL database %s already exists.", DATABASE_NAME)
    finally:
        connection.close()


def _ensure_tables(
    connection: psycopg2.extensions.connection,
) -> None:
    column_definitions = sql.SQL(", ").join(
        sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(column_type))
        for name, column_type in TABLE_COLUMNS
    )
    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("CREATE TABLE IF NOT EXISTS public.{} ({})").format(
                sql.Identifier(TABLE_NAME),
                column_definitions,
            )
        )
        cursor.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS public.{} (
                    source_name TEXT PRIMARY KEY,
                    source_file TEXT NOT NULL,
                    source_sha256 TEXT NOT NULL,
                    source_size_bytes BIGINT NOT NULL,
                    loaded_records BIGINT NOT NULL,
                    malformed_records BIGINT NOT NULL,
                    loaded_at TIMESTAMPTZ NOT NULL
                )
                """
            ).format(sql.Identifier(METADATA_TABLE_NAME))
        )
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (TABLE_NAME,),
        )
        existing_columns = {row[0] for row in cursor.fetchall()}
        required_columns = {name for name, _ in TABLE_COLUMNS}
        missing_columns = sorted(required_columns - existing_columns)
        if missing_columns:
            raise RuntimeError(
                f"Existing public.{TABLE_NAME} table is missing columns: "
                f"{missing_columns}. It was not modified."
            )


def load_grammys_csv_to_postgres(
    csv_path: Path = GRAMMYS_CSV_PATH,
) -> dict[str, object]:
    """Create workshop2.grammys and load the cleaned project source file.

    The original raw CSV remains untouched in data/raw for traceability. Before the
    load, we build a cleaned projection that removes malformed rows and normalizes
    basic values so the project is fed with an internally consistent source.
    """
    if not csv_path.is_file():
        raise FileNotFoundError(f"Grammy bootstrap CSV does not exist: {csv_path}")
    csv_path = clean_grammys_source(csv_path=csv_path, output_path=CLEANED_GRAMMYS_CSV_PATH)
    source_hash, source_size = _sha256(csv_path)
    _ensure_database()
    connection = connect_postgres(DATABASE_NAME)

    try:
        with connection:
            _ensure_tables(connection)
            with connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL(
                        "SELECT source_file, source_sha256, source_size_bytes, "
                        "loaded_records, malformed_records "
                        "FROM public.{} WHERE source_name = %s"
                    ).format(sql.Identifier(METADATA_TABLE_NAME)),
                    (TABLE_NAME,),
                )
                loaded_metadata = cursor.fetchone()
                cursor.execute(
                    sql.SQL("SELECT COUNT(*) FROM public.{}").format(
                        sql.Identifier(TABLE_NAME)
                    )
                )
                existing_record_count = int(cursor.fetchone()[0])

                if loaded_metadata is not None:
                    (
                        loaded_source_file,
                        loaded_hash,
                        loaded_size,
                        loaded_records,
                        loaded_malformed_count,
                    ) = loaded_metadata
                    if (
                        loaded_source_file != csv_path.name
                        or loaded_hash != source_hash
                        or loaded_size != source_size
                    ):
                        raise RuntimeError(
                            "Database workshop2.grammys was bootstrapped from a "
                            "different Grammy CSV version. Refusing to overwrite "
                            "the existing source table."
                        )
                    if existing_record_count != loaded_records:
                        raise RuntimeError(
                            "Database workshop2.grammys row count differs from "
                            "its bootstrap metadata; refusing to continue."
                        )
                    result = {
                        "database": DATABASE_NAME,
                        "table": TABLE_NAME,
                        "source_sha256": source_hash,
                        "records": int(loaded_records),
                        "malformed_records": int(loaded_malformed_count),
                        "loaded_now": False,
                    }
                    LOGGER.info(
                        "Grammy source table already contains the unchanged "
                        "bootstrap batch: %s.",
                        result,
                    )
                    return result

                if existing_record_count:
                    raise RuntimeError(
                        f"public.{TABLE_NAME} already contains "
                        f"{existing_record_count} rows but has no bootstrap "
                        "metadata; refusing to overwrite or infer its provenance."
                    )

                records, malformed_lines, parsed_source_hash, parsed_source_size = (
                    _read_source_records(csv_path)
                )
                if (
                    parsed_source_hash != source_hash
                    or parsed_source_size != source_size
                ):
                    raise RuntimeError(
                        f"Grammy CSV changed while it was being bootstrapped: "
                        f"{csv_path}"
                    )

                column_names = [name for name, _ in TABLE_COLUMNS]
                insert_statement = sql.SQL(
                    "INSERT INTO public.{} ({}) VALUES ({})"
                ).format(
                    sql.Identifier(TABLE_NAME),
                    sql.SQL(", ").join(
                        sql.Identifier(column) for column in column_names
                    ),
                    sql.SQL(", ").join(
                        sql.Placeholder() for _ in column_names[:-1]
                    )
                    + sql.SQL(", %s::jsonb"),
                )
                cursor.executemany(insert_statement.as_string(connection), records)
                cursor.execute(
                    sql.SQL(
                        "INSERT INTO public.{} "
                        "(source_name, source_file, source_sha256, "
                        "source_size_bytes, loaded_records, malformed_records, "
                        "loaded_at) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    ).format(sql.Identifier(METADATA_TABLE_NAME)),
                    (
                        TABLE_NAME,
                        csv_path.name,
                        source_hash,
                        source_size,
                        len(records),
                        len(malformed_lines),
                        datetime.now(timezone.utc),
                    ),
                )

        result = {
            "database": DATABASE_NAME,
            "table": TABLE_NAME,
            "source_sha256": source_hash,
            "records": len(records),
            "malformed_records": len(malformed_lines),
            "malformed_physical_lines": malformed_lines,
            "loaded_now": True,
        }
        LOGGER.info("Loaded Grammy source table from CSV: %s.", result)
        return result
    finally:
        connection.close()


def extract_grammys_database_batch(run_id: str) -> dict[str, object]:
    """Export the Grammy table into an immutable Airflow batch for GX checks."""
    _ensure_database()
    connection = connect_postgres(DATABASE_NAME)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("SELECT COUNT(*) FROM public.{}").format(
                    sql.Identifier(TABLE_NAME)
                )
            )
            record_count = int(cursor.fetchone()[0])
            if record_count == 0:
                raise RuntimeError("PostgreSQL public.grammys contains no records.")

            columns = ", ".join(
                sql.Identifier(column).as_string(connection)
                for column in GRAMMY_COLUMNS
            )
            cursor.execute(
                sql.SQL(
                    "SELECT {}, source_row_number, source_line_number, "
                    "source_field_count, raw_fields "
                    "FROM public.{} ORDER BY source_row_number"
                ).format(
                    sql.SQL(columns),
                    sql.Identifier(TABLE_NAME),
                )
            )
            rows = cursor.fetchall()
            cursor.execute(
                sql.SQL(
                    "SELECT source_sha256 FROM public.{} WHERE source_name = %s"
                ).format(sql.Identifier(METADATA_TABLE_NAME)),
                (TABLE_NAME,),
            )
            metadata_row = cursor.fetchone()
            if metadata_row is None:
                raise RuntimeError(
                    "PostgreSQL Grammy table has no bootstrap provenance metadata."
                )
            source_sha256 = str(metadata_row[0])
    finally:
        connection.close()

    if len(rows) != record_count:
        raise RuntimeError(
            "Grammy table row count changed during extraction: "
            f"expected {record_count}, extracted {len(rows)}."
        )

    malformed_lines = [
        int(row[len(GRAMMY_COLUMNS) + 1])
        for row in rows
        if int(row[len(GRAMMY_COLUMNS) + 2]) != len(GRAMMY_COLUMNS)
    ]
    batch_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", run_id).strip("._-")
    if not batch_id:
        raise ValueError("Airflow run_id must produce a non-empty batch identifier.")

    batch_directory = BATCH_ROOT / batch_id
    batch_directory.mkdir(parents=True, exist_ok=True)
    csv_path = batch_directory / "the_grammy_awards.csv"
    jsonl_path = batch_directory / "grammys_source_rows.jsonl"
    manifest_path = batch_directory / "manifest.json"
    report_path = batch_directory / "validation_report.json"

    import io

    csv_buffer = io.StringIO(newline="")
    csv_writer = csv.writer(csv_buffer, lineterminator="\n")
    csv_writer.writerow(GRAMMY_COLUMNS)
    jsonl_records: list[str] = []
    for row in rows:
        csv_writer.writerow(row[: len(GRAMMY_COLUMNS)])
        jsonl_records.append(
            json.dumps(
                {
                    "business_columns": dict(
                        zip(GRAMMY_COLUMNS, row[: len(GRAMMY_COLUMNS)])
                    ),
                    "source_row_number": row[len(GRAMMY_COLUMNS)],
                    "source_line_number": row[len(GRAMMY_COLUMNS) + 1],
                    "source_field_count": row[len(GRAMMY_COLUMNS) + 2],
                    "raw_fields": row[len(GRAMMY_COLUMNS) + 3],
                },
                ensure_ascii=False,
            )
        )

    csv_bytes = csv_buffer.getvalue().encode("utf-8")
    jsonl_bytes = ("\n".join(jsonl_records) + "\n").encode("utf-8")
    csv_sha256 = hashlib.sha256(csv_bytes).hexdigest()
    jsonl_sha256 = hashlib.sha256(jsonl_bytes).hexdigest()

    if manifest_path.exists():
        with manifest_path.open(encoding="utf-8") as manifest_file:
            manifest = json.load(manifest_file)
        if (
            manifest.get("source_sha256") != source_sha256
            or manifest.get("csv_sha256") != csv_sha256
            or manifest.get("jsonl_sha256") != jsonl_sha256
        ):
            raise FileExistsError(
                f"Immutable Grammy batch {batch_id!r} differs from the current "
                "database table."
            )
        LOGGER.info("Reusing unchanged Grammy database batch %s.", batch_id)
        return {
            **manifest,
            "path": str(csv_path),
            "source_rows_path": str(jsonl_path),
            "manifest_path": str(manifest_path),
            "report_path": str(report_path),
        }

    _write_bytes_atomically(csv_path, csv_bytes)
    _write_bytes_atomically(jsonl_path, jsonl_bytes)
    manifest: dict[str, object] = {
        "source": "grammys",
        "source_type": "postgresql",
        "database": DATABASE_NAME,
        "schema": "public",
        "table": TABLE_NAME,
        "run_id": run_id,
        "batch_id": batch_id,
        "source_sha256": source_sha256,
        "csv_sha256": csv_sha256,
        "jsonl_sha256": jsonl_sha256,
        "records": record_count,
        "malformed_records": len(malformed_lines),
        "malformed_physical_lines": malformed_lines,
        "path": str(csv_path),
        "source_rows_path": str(jsonl_path),
        "manifest_path": str(manifest_path),
        "report_path": str(report_path),
        "extracted_at": datetime.now(timezone.utc).isoformat(),
    }
    _write_json_atomically(manifest_path, manifest)
    LOGGER.info(
        "Extracted %s Grammy rows from PostgreSQL %s.%s into batch %s.",
        record_count,
        DATABASE_NAME,
        TABLE_NAME,
        batch_id,
    )
    return manifest


def _write_bytes_atomically(path: Path, contents: bytes) -> None:
    temporary_path = path.with_name(f".{path.name}.tmp")
    try:
        temporary_path.write_bytes(contents)
        os.replace(temporary_path, path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise


def _write_json_atomically(path: Path, payload: dict[str, object]) -> None:
    _write_bytes_atomically(
        path,
        (json.dumps(payload, indent=2, ensure_ascii=False) + "\n").encode("utf-8"),
    )
