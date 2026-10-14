"""Extract, gate, and expose planned downstream stages for raw source batches."""

from datetime import datetime, timezone
from collections.abc import Callable
import hashlib
import logging
from pathlib import Path

from airflow.sdk import dag, get_current_context, task


LOGGER = logging.getLogger(__name__)


def _hash_table_files(
    transformed_files: dict[str, dict[str, str]],
) -> dict[str, str]:
    digests: dict[str, str] = {}
    for tables in transformed_files.values():
        for table_name, path_value in tables.items():
            path = Path(path_value)
            if not path.is_file():
                raise FileNotFoundError(
                    f"Curated table file does not exist: {path}"
                )
            digest = hashlib.sha256()
            with path.open("rb") as source_file:
                for chunk in iter(lambda: source_file.read(1024 * 1024), b""):
                    digest.update(chunk)
            digest_value = digest.hexdigest()
            previous = digests.get(table_name)
            if previous is not None and previous != digest_value:
                raise RuntimeError(
                    f"Curated table {table_name!r} resolves to different files."
                )
            digests[table_name] = digest_value
    return digests


def _extract_source(source_name: str) -> dict[str, object]:
    from src.raw_batch import extract_raw_batch

    context = get_current_context()
    dag_run = context["dag_run"]
    return extract_raw_batch(source_name, dag_run.run_id)


def _extract_grammys_source() -> dict[str, object]:
    from src.grammys_postgres import extract_grammys_database_batch

    context = get_current_context()
    dag_run = context["dag_run"]
    return extract_grammys_database_batch(dag_run.run_id)


def _validate_source(
    raw_batch: dict[str, object], validator: Callable[..., object]
) -> dict[str, object]:
    from gx.validation_common import (
        QualityGateFailure,
        write_failure_report,
    )

    raw_path = raw_batch.get("path")
    report_path_value = raw_batch.get("report_path")
    if not isinstance(raw_path, str) or not isinstance(report_path_value, str):
        raise TypeError("Raw extraction XCom is missing its file or report path.")

    report_path = Path(report_path_value)
    try:
        summary = validator(
            data_path=Path(raw_path),
            report_path=report_path,
            batch_metadata=raw_batch,
        )
    except QualityGateFailure:
        LOGGER.exception(
            "The raw quality gate blocked batch %s.", raw_batch.get("batch_id")
        )
        raise
    except Exception as error:
        write_failure_report(report_path, raw_batch, error)
        LOGGER.exception(
            "Raw validation could not complete for batch %s.",
            raw_batch.get("batch_id"),
        )
        raise

    if not isinstance(summary, dict):
        raise TypeError("Raw validation did not return a summary dictionary.")
    LOGGER.info(
        "Raw quality gate passed for batch %s; report: %s.",
        raw_batch.get("batch_id"),
        report_path,
    )
    return dict(summary)


@dag(
    dag_id="grammys_spotify_data_quality",
    description=(
        "Extracts and validates Grammy and Spotify batches, builds the curated "
        "dimensional model, and loads it into PostgreSQL."
    ),
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["workshop", "great-expectations", "data-quality"],
)
def grammys_spotify_data_quality():
    # Raw-first execution model: the source is ingested and validated before any
    # curated transformation is attempted. This keeps the data-quality gate explicit
    # and prevents downstream processing from masking source problems.
    @task
    def load_grammys_to_postgres() -> dict[str, object]:
        from src.grammys_postgres import load_grammys_csv_to_postgres

        return load_grammys_csv_to_postgres()

    @task
    def extract_grammys_from_postgres(
        database_load: dict[str, object],
    ) -> dict[str, object]:
        if database_load.get("database") != "workshop2":
            raise ValueError(
                "Grammy bootstrap did not confirm PostgreSQL database workshop2."
            )
        return _extract_grammys_source()

    @task
    def validate_raw_grammys(
        raw_batch: dict[str, object],
    ) -> dict[str, object]:
        from gx.grammys_validation import run_validation

        return _validate_source(raw_batch, run_validation)

    @task
    def extract_spotify() -> dict[str, object]:
        return _extract_source("spotify")

    @task
    def validate_raw_spotify(
        raw_batch: dict[str, object],
    ) -> dict[str, object]:
        from gx.spotify_validation import run_validation

        return _validate_source(raw_batch, run_validation)

    @task
    def transform_curated_warehouse(
        grammys_batch: dict[str, object],
        grammys_validation: dict[str, object],
        spotify_validation: dict[str, object],
    ) -> dict[str, dict[str, str]]:
        from src.transform import run_transformations

        grammys_path = grammys_batch.get("path")
        spotify_path = spotify_validation.get("normalized_path")
        if grammys_validation.get("suite_name") != "grammys_quality_suite":
            raise ValueError("Grammy raw validation did not return a successful summary.")
        if spotify_validation.get("suite_name") != "spotify_quality_suite":
            raise ValueError("Spotify raw validation did not return a successful summary.")
        if grammys_validation.get("blocking_failures") or spotify_validation.get(
            "blocking_failures"
        ):
            raise ValueError("A raw quality gate reported blocking failures.")
        if not isinstance(grammys_path, str) or not isinstance(spotify_path, str):
            raise TypeError(
                "Raw validated source paths are missing from the Airflow task inputs."
            )

        result = run_transformations(
            grammy_source=Path(grammys_path),
            spotify_source=Path(spotify_path),
        )
        return {
            domain: {table_name: str(path) for table_name, path in tables.items()}
            for domain, tables in result.items()
        }

    @task
    def validate_curated_warehouse(
        transformed_files: dict[str, dict[str, str]],
    ) -> dict[str, object]:
        from gx.curated_validation import run_validation
        from gx.validation_common import (
            QualityGateFailure,
            write_failure_report,
        )

        output_paths = [
            path
            for tables in transformed_files.values()
            for path in tables.values()
        ]
        if not output_paths:
            raise ValueError("Curated transformation returned no output files.")

        output_directory = Path(output_paths[0]).parent
        report_path = output_directory / "curated_validation_report.json"
        metadata: dict[str, object] = {
            "source": "curated_warehouse",
            "tables": transformed_files,
        }
        input_hashes = _hash_table_files(transformed_files)
        metadata["input_sha256"] = input_hashes
        try:
            summary = run_validation(
                data_directory=output_directory,
                report_path=report_path,
                batch_metadata=metadata,
            )
            if _hash_table_files(transformed_files) != input_hashes:
                raise RuntimeError(
                    "Curated files changed while Great Expectations was validating."
                )
            summary["validated_files_sha256"] = input_hashes
            return summary
        except QualityGateFailure:
            LOGGER.exception("The curated warehouse quality gate failed.")
            raise
        except Exception as error:
            write_failure_report(report_path, metadata, error)
            LOGGER.exception("Curated warehouse validation could not complete.")
            raise

    @task
    def load_curated_warehouse(
        transformed_files: dict[str, dict[str, str]],
        validation: dict[str, object],
    ) -> dict[str, object]:
        from src.warehouse_postgres import (
            load_curated_warehouse as load_tables_to_postgres,
        )

        if validation.get("suite_name") != "curated_warehouse_suite":
            raise ValueError(
                "PostgreSQL warehouse load requires a curated warehouse "
                "Great Expectations result."
            )
        blocking_failures = validation.get("blocking_failures")
        if not isinstance(blocking_failures, list):
            raise TypeError("Curated GX result is missing blocking_failures.")
        if blocking_failures:
            raise ValueError(
                "PostgreSQL warehouse load was blocked by curated GX failures: "
                + "; ".join(str(failure) for failure in blocking_failures)
            )
        validated_hashes = validation.get("validated_files_sha256")
        if not isinstance(validated_hashes, dict):
            raise TypeError(
                "Curated GX result is missing validated file fingerprints."
            )
        if _hash_table_files(transformed_files) != validated_hashes:
            raise RuntimeError(
                "Curated files differ from the files that passed Great Expectations."
            )

        table_paths = {
            table_name: path
            for tables in transformed_files.values()
            for table_name, path in tables.items()
        }
        result = load_tables_to_postgres(table_paths)
        LOGGER.info(
            "Loaded %s curated rows into %s.%s.",
            result["total_rows_loaded"],
            result["database"],
            result["schema"],
        )
        return result

    # Grammy path: CSV -> PostgreSQL raw table -> immutable batch -> raw validation.
    grammys_database = load_grammys_to_postgres()
    grammys_raw_batch = extract_grammys_from_postgres(grammys_database)
    grammys_raw_gate = validate_raw_grammys(grammys_raw_batch)

    # Spotify path: raw file -> immutable batch -> raw validation.
    spotify_raw_batch = extract_spotify()
    spotify_raw_gate = validate_raw_spotify(spotify_raw_batch)

    # Build the two subject areas together so they share exactly one set of
    # artist surrogate keys and consume the output of the raw Spotify gate.
    curated_warehouse = transform_curated_warehouse(
        grammys_raw_batch,
        grammys_raw_gate,
        spotify_raw_gate,
    )
    curated_gate = validate_curated_warehouse(curated_warehouse)
    load_curated_warehouse(curated_warehouse, curated_gate)


grammys_spotify_data_quality()
