"""Three-rule Great Expectations gate for Spotify analytical data."""

from pathlib import Path

import great_expectations as gx
import pandas as pd

from gx.validation_common import (
    QualityRule,
    ValidationSummary,
    raise_for_blocking_failures,
    run_expectation_suite,
    write_validation_report,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = PROJECT_ROOT / "data" / "raw" / "spotify_dataset.csv"
REQUIRED_COLUMNS = (
    "track_id",
    "track_genre",
    "popularity",
    "duration_ms",
    "energy",
    "valence",
    "danceability",
)
NUMERIC_COLUMNS = (
    "popularity",
    "duration_ms",
    "energy",
    "valence",
    "danceability",
)
ANALYTICAL_METRICS = ("popularity", "energy", "valence", "danceability")
GRAIN_COLUMNS = ("track_id", "track_genre")


def _read_spotify_csv(data_path: Path) -> tuple[pd.DataFrame, bool]:
    dataframe = pd.read_csv(data_path)
    unnamed_columns = [
        column
        for column in dataframe.columns
        if str(column).startswith("Unnamed:")
    ]
    index_column_removed = False

    if unnamed_columns:
        if len(unnamed_columns) != 1 or dataframe.columns[0] != unnamed_columns[0]:
            raise ValueError(
                "Spotify CSV contains unexpected unnamed columns: "
                f"{unnamed_columns}"
            )
        index_values = pd.to_numeric(dataframe[unnamed_columns[0]], errors="coerce")
        if index_values.isna().any() or index_values.tolist() != list(
            range(len(dataframe))
        ):
            raise ValueError(
                "Spotify's unnamed first column is not a sequential exported index."
            )
        dataframe = dataframe.drop(columns=unnamed_columns[0])
        index_column_removed = True

    missing_columns = sorted(set(REQUIRED_COLUMNS) - set(dataframe.columns))
    if missing_columns:
        raise ValueError(
            "Spotify CSV is missing required analytical columns: "
            f"{missing_columns}"
        )
    return dataframe, index_column_removed


def _normalize_rows(
    dataframe: pd.DataFrame,
) -> tuple[pd.DataFrame, bool, int]:
    normalized = dataframe.copy()
    normalized["track_id"] = normalized["track_id"].astype("string").str.strip()
    normalized["track_genre"] = (
        normalized["track_genre"].astype("string").str.strip()
    )
    for column in NUMERIC_COLUMNS:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    duplicated = normalized.duplicated(list(GRAIN_COLUMNS), keep=False)
    duplicate_group = normalized.loc[duplicated]
    non_key_columns = [
        column for column in normalized.columns if column not in GRAIN_COLUMNS
    ]
    conflicting_rows = pd.Series(False, index=normalized.index)
    if not duplicate_group.empty:
        for _, group in duplicate_group.groupby(
            list(GRAIN_COLUMNS),
            dropna=False,
            sort=False,
        ):
            if group[non_key_columns].nunique(dropna=False).gt(1).any():
                conflicting_rows.loc[group.index] = True

    source_row_count = len(normalized)
    if conflicting_rows.any():
        normalized = normalized.reset_index(drop=True)
    else:
        normalized = normalized.drop_duplicates(
            subset=list(GRAIN_COLUMNS),
            keep="first",
        ).reset_index(drop=True)
    duplicate_rows_removed = source_row_count - len(normalized)
    return normalized, not conflicting_rows.any(), duplicate_rows_removed


def _analytical_row_validity(dataframe: pd.DataFrame) -> pd.Series:
    valid = (
        dataframe["track_id"].notna()
        & dataframe["track_id"].ne("")
        & dataframe["track_genre"].notna()
        & dataframe["track_genre"].ne("")
    )
    for column in ANALYTICAL_METRICS:
        values = pd.to_numeric(dataframe[column], errors="coerce")
        lower, upper = (0, 100) if column == "popularity" else (0, 1)
        valid &= values.notna() & values.between(lower, upper)
    return valid


def run_validation(
    data_path: Path = DATA_PATH,
    report_path: Path | None = None,
    batch_metadata: dict[str, object] | None = None,
) -> ValidationSummary:
    """Validate and normalize Spotify without creating a quarantine dataset."""
    raw_dataframe, index_column_removed = _read_spotify_csv(data_path)
    source_rows = len(raw_dataframe)
    dataframe, duplicate_groups_consistent, duplicate_rows_removed = _normalize_rows(
        raw_dataframe
    )

    rules = [
        # 1. Columnas (Esquema y presencia de columnas analíticas)
        QualityRule(
            rule_id="R-SPOT-01-COLS",
            severity="critical",
            expectation=gx.expectations.ExpectTableColumnsToMatchSet(
                column_set=list(REQUIRED_COLUMNS),
                exact_match=False,
            ),
            description="Contrato de esquema: presencia de columnas obligatorias",
        ),
        # 2. Valores permitidos (Completitud y dominios métricos)
        QualityRule(
            rule_id="R-SPOT-02-VALS-track-id",
            severity="critical",
            expectation=gx.expectations.ExpectColumnValuesToNotBeNull(
                column="track_id"
            ),
            description="Identificador track_id no nulo",
        ),
        QualityRule(
            rule_id="R-SPOT-02-VALS-genre",
            severity="critical",
            expectation=gx.expectations.ExpectColumnValuesToNotBeNull(
                column="track_genre"
            ),
            description="Categoría track_genre no nula",
        ),
        QualityRule(
            rule_id="R-SPOT-02-VALS-popularity",
            severity="high",
            expectation=gx.expectations.ExpectColumnValuesToBeBetween(
                column="popularity",
                min_value=0,
                max_value=100,
            ),
            description="Métrica popularity en dominio [0, 100]",
        ),
        QualityRule(
            rule_id="R-SPOT-02-VALS-danceability",
            severity="high",
            expectation=gx.expectations.ExpectColumnValuesToBeBetween(
                column="danceability",
                min_value=0.0,
                max_value=1.0,
            ),
            description="Métrica danceability en dominio [0.0, 1.0]",
        ),
        QualityRule(
            rule_id="R-SPOT-02-VALS-energy",
            severity="high",
            expectation=gx.expectations.ExpectColumnValuesToBeBetween(
                column="energy",
                min_value=0.0,
                max_value=1.0,
            ),
            description="Métrica energy en dominio [0.0, 1.0]",
        ),
        QualityRule(
            rule_id="R-SPOT-02-VALS-valence",
            severity="high",
            expectation=gx.expectations.ExpectColumnValuesToBeBetween(
                column="valence",
                min_value=0.0,
                max_value=1.0,
            ),
            description="Métrica valence en dominio [0.0, 1.0]",
        ),
        # 3. Filas repetidas (Unicidad en el grano track_id + track_genre)
        QualityRule(
            rule_id="R-SPOT-03-DUPS-grain",
            severity="critical",
            expectation=gx.expectations.ExpectCompoundColumnsToBeUnique(
                column_list=list(GRAIN_COLUMNS)
            ),
            description="Unicidad estricta en el grano compuesto (track_id, track_genre)",
        ),
    ]
    result = run_expectation_suite(
        dataframe=dataframe,
        suite_name="spotify_quality_suite",
        data_source_name="spotify_pandas_source",
        asset_name="spotify_analytical_dataset",
        rules=rules,
    )
    if not duplicate_groups_consistent:
        inconsistent_msg = (
            "R-SPOT-03-DUPS failed (critical): found duplicate rows with conflicting "
            "attributes across identical (track_id, track_genre) keys."
        )
        result["blocking_failures"].append(inconsistent_msg)
        result["failed"] += 1

    normalized_path = data_path.parent / "spotify_normalized.csv"
    dataframe.to_csv(normalized_path, index=False)
    result["normalized_path"] = str(normalized_path)
    result["source_rows"] = source_rows
    result["normalized_rows"] = len(dataframe)
    result["duplicate_rows_removed"] = duplicate_rows_removed
    if index_column_removed:
        result["informational"].append(
            "The sequential unnamed export-index column was excluded from the "
            "analytical dataset."
        )
    if duplicate_rows_removed:
        result["informational"].append(
            f"Removed {duplicate_rows_removed} repeated rows at the "
            "(track_id, track_genre) grain because all source attributes agreed; "
            "the original batch was preserved and no rows were quarantined."
        )

    summary_message = (
        f"Spotify source/normalized rows: {source_rows}/{len(dataframe)}; "
        f"duplicate rows removed: {duplicate_rows_removed}; "
        f"quarantined rows: 0."
    )
    print(summary_message)
    if report_path is not None:
        write_validation_report(report_path, batch_metadata or {}, result)
    raise_for_blocking_failures(result)
    return result


if __name__ == "__main__":
    run_validation()
