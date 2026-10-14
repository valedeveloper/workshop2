"""Three-rule Great Expectations gate for the cross-source artist warehouse."""

import re
from pathlib import Path
from typing import Any

import great_expectations as gx
import pandas as pd

from gx.validation_common import (
    QualityRule,
    ValidationSummary,
    raise_for_blocking_failures,
    run_expectation_suite,
    write_validation_report,
)


TABLE_COLUMNS = {
    "dim_date": ("date_sk", "year", "decade"),
    "dim_artist": ("artist_sk", "artist_name", "artist_name_normalized"),
    "dim_cancion": (
        "track_sk",
        "track_id",
        "track_name",
        "album_name",
        "artists",
        "duration_ms",
        "explicit",
    ),
    "dim_genero": ("genre_sk", "genre_name"),
    "fact_music_award": (
        "music_award_sk",
        "date_sk",
        "artist_sk",
        "category",
        "nominee",
        "title",
        "winner",
        "award_count",
    ),
    "fact_spotify_artist_track_genre": (
        "spotify_credit_sk",
        "track_sk",
        "artist_sk",
        "genre_sk",
        "popularity",
        "danceability",
        "energy",
        "valence",
    ),
}

PRIMARY_KEYS = {
    "dim_date": ("date_sk",),
    "dim_artist": ("artist_sk",),
    "dim_cancion": ("track_sk",),
    "dim_genero": ("genre_sk",),
    "fact_music_award": ("music_award_sk",),
    "fact_spotify_artist_track_genre": ("spotify_credit_sk",),
}


def _column(frame: pd.DataFrame, name: str) -> pd.Series:
    if name not in frame:
        return pd.Series(index=frame.index, dtype="object")
    return frame[name]


def _not_null_and_unique(frame: pd.DataFrame, columns: tuple[str, ...]) -> bool:
    if any(column not in frame for column in columns):
        return False
    return bool(
        frame.loc[:, list(columns)].notna().all(axis=1).all()
        and not frame.duplicated(list(columns)).any()
    )


def _unique_at_grain(frame: pd.DataFrame, columns: tuple[str, ...]) -> bool:
    if any(column not in frame for column in columns):
        return False
    return not frame.duplicated(list(columns)).any()


def _foreign_key_is_valid(
    frame: pd.DataFrame,
    column: str,
    referenced_values: set[Any],
    allow_null: bool = False,
) -> bool:
    values = _column(frame, column)
    if not allow_null and values.isna().any():
        return False
    return bool(values.dropna().isin(referenced_values).all())


def _values_in_range(
    frame: pd.DataFrame,
    column: str,
    minimum: float,
    maximum: float,
) -> bool:
    values = pd.to_numeric(_column(frame, column), errors="coerce")
    return bool(
        len(values) > 0
        and values.notna().all()
        and values.between(minimum, maximum).all()
    )


def _artist_names_are_normalized(frame: pd.DataFrame) -> bool:
    names = _column(frame, "artist_name").astype("string")
    normalized = _column(frame, "artist_name_normalized").astype("string")
    expected = names.map(
        lambda value: (
            re.sub(r"\s+", " ", value).strip().casefold()
            if not pd.isna(value)
            else pd.NA
        )
    ).astype("string")
    return bool(
        names.notna().all()
        and normalized.notna().all()
        and expected.eq(normalized).all()
        and not normalized.duplicated().any()
    )


def run_validation(
    data_directory: Path,
    report_path: Path | None = None,
    batch_metadata: dict[str, object] | None = None,
) -> ValidationSummary:
    """Validate table schemas, cross-source relationships, grains, and metrics."""
    tables: dict[str, pd.DataFrame] = {}
    schema_valid: dict[str, bool] = {}
    for table_name, expected_columns in TABLE_COLUMNS.items():
        table_path = data_directory / f"{table_name}.csv"
        if not table_path.is_file():
            tables[table_name] = pd.DataFrame(columns=expected_columns)
            schema_valid[table_name] = False
            continue

        dataframe = pd.read_csv(table_path)
        tables[table_name] = dataframe
        schema_valid[table_name] = (
            tuple(str(column) for column in dataframe.columns) == expected_columns
        )

    structure_checks: list[bool] = []
    for table_name, dataframe in tables.items():
        structure_checks.extend(
            [
                schema_valid[table_name],
                not dataframe.empty,
                _not_null_and_unique(dataframe, PRIMARY_KEYS[table_name]),
            ]
        )
    structure_checks.extend(
        [
            _not_null_and_unique(tables["dim_date"], ("year",)),
            _artist_names_are_normalized(tables["dim_artist"]),
            _not_null_and_unique(
                tables["dim_artist"], ("artist_name_normalized",)
            ),
            _not_null_and_unique(tables["dim_cancion"], ("track_id",)),
            _not_null_and_unique(tables["dim_genero"], ("genre_name",)),
            _unique_at_grain(
                tables["fact_spotify_artist_track_genre"],
                ("track_sk", "artist_sk", "genre_sk"),
            ),
        ]
    )

    dim_date_keys = set(_column(tables["dim_date"], "date_sk").dropna().tolist())
    dim_artist_keys = set(_column(tables["dim_artist"], "artist_sk").dropna().tolist())
    dim_track_keys = set(_column(tables["dim_cancion"], "track_sk").dropna().tolist())
    dim_genre_keys = set(_column(tables["dim_genero"], "genre_sk").dropna().tolist())
    fact_grammy = tables["fact_music_award"]
    fact_spotify = tables["fact_spotify_artist_track_genre"]

    relationships_checks = [
        _foreign_key_is_valid(fact_grammy, "date_sk", dim_date_keys),
        _foreign_key_is_valid(
            fact_grammy, "artist_sk", dim_artist_keys, allow_null=True
        ),
        _foreign_key_is_valid(
            fact_spotify, "artist_sk", dim_artist_keys, allow_null=True
        ),
        _foreign_key_is_valid(fact_spotify, "track_sk", dim_track_keys),
        _foreign_key_is_valid(fact_spotify, "genre_sk", dim_genre_keys),
    ]

    dim_date = tables["dim_date"]
    numeric_year = pd.to_numeric(_column(dim_date, "year"), errors="coerce")
    year_valid = bool(
        len(numeric_year) > 0
        and numeric_year.notna().all()
        and numeric_year.eq(numeric_year.round()).all()
        and numeric_year.between(1900, pd.Timestamp.now().year + 1).all()
    )
    decade_valid = bool(
        len(dim_date) > 0
        and pd.to_numeric(_column(dim_date, "decade"), errors="coerce").eq(
            (numeric_year // 10) * 10
        ).all()
    )
    domains_checks = [
        year_valid,
        decade_valid,
        _column(fact_grammy, "award_count").eq(1).all(),
        _column(fact_grammy, "winner").dropna().isin({True, False}).all(),
        _values_in_range(fact_spotify, "popularity", 0, 100),
        _values_in_range(fact_spotify, "danceability", 0, 1),
        _values_in_range(fact_spotify, "energy", 0, 1),
        _values_in_range(fact_spotify, "valence", 0, 1),
    ]

    maximum_checks = max(
        len(structure_checks),
        len(relationships_checks),
        len(domains_checks),
    )
    checks = pd.DataFrame(
        {
            "structural_integrity": structure_checks
            + [True] * (maximum_checks - len(structure_checks)),
            "cross_source_relationships_and_grain": relationships_checks
            + [True] * (maximum_checks - len(relationships_checks)),
            "analytical_domains": domains_checks
            + [True] * (maximum_checks - len(domains_checks)),
        }
    )
    rules = [
        QualityRule(
            rule_id="curated-structural-integrity",
            severity="critical",
            expectation=gx.expectations.ExpectColumnValuesToBeInSet(
                column="structural_integrity",
                value_set=[True],
            ),
        ),
        QualityRule(
            rule_id="curated-cross-source-relationships-and-grain",
            severity="critical",
            expectation=gx.expectations.ExpectColumnValuesToBeInSet(
                column="cross_source_relationships_and_grain",
                value_set=[True],
            ),
        ),
        QualityRule(
            rule_id="curated-analytical-domains",
            severity="high",
            expectation=gx.expectations.ExpectColumnValuesToBeInSet(
                column="analytical_domains",
                value_set=[True],
            ),
        ),
    ]
    result = run_expectation_suite(
        dataframe=checks,
        suite_name="curated_warehouse_suite",
        data_source_name="curated_warehouse_source",
        asset_name="curated_warehouse_checks",
        rules=rules,
    )

    missing_artist_count = int(_column(fact_grammy, "artist_sk").isna().sum())
    if missing_artist_count:
        result["warnings"].append(
            "Grammy records without an attributable artist: "
            f"{missing_artist_count}/{len(fact_grammy)}; these rows remain in "
            "fact_music_award with a null artist_sk."
        )
    spotify_artist_missing = int(_column(fact_spotify, "artist_sk").isna().sum())
    if spotify_artist_missing:
        result["warnings"].append(
            "Spotify song-genre rows without an artist credit: "
            f"{spotify_artist_missing}/{len(fact_spotify)}; they remain available "
            "for genre popularity metrics but are excluded from artist KPIs."
        )
    winner_values = _column(fact_grammy, "winner").dropna().unique().tolist()
    if winner_values and all(value is True or value == 1 for value in winner_values):
        result["warnings"].append(
            "The Grammy source contains only winner=True records; cross-source "
            "KPIs measure artists with an observed winner record, not all "
            "historical Grammy nominees or winners."
        )
    result["informational"].append(
        "Spotify artist credits are flattened to song-artist-genre rows; "
        "popularity-by-genre queries must deduplicate at song-genre grain."
    )
    if report_path is not None:
        write_validation_report(report_path, batch_metadata or {}, result)
    raise_for_blocking_failures(result)
    return result
