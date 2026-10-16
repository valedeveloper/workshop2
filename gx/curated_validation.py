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
    run_multi_asset_expectation_suite,
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

    rules: list[QualityRule] = []

    # 1. R-CUR-01-COLS: Esquemas exactos y tablas no vacías
    for table_name, expected_columns in TABLE_COLUMNS.items():
        rules.append(
            QualityRule(
                rule_id=f"R-CUR-01-COLS-{table_name}",
                severity="critical",
                target_asset=table_name,
                expectation=gx.expectations.ExpectTableColumnsToMatchSet(
                    column_set=list(expected_columns),
                    exact_match=True,
                ),
                description=f"Esquema exacto de la tabla {table_name}",
            )
        )
        rules.append(
            QualityRule(
                rule_id=f"R-CUR-01-ROWS-{table_name}",
                severity="critical",
                target_asset=table_name,
                expectation=gx.expectations.ExpectTableRowCountToBeBetween(
                    min_value=1
                ),
                description=f"Tabla {table_name} no vacía",
            )
        )

    # 2. R-CUR-02-VALS: Dominios e Integridad Referencial
    # dim_date
    rules.append(
        QualityRule(
            rule_id="R-CUR-02-VALS-date-year",
            severity="critical",
            target_asset="dim_date",
            expectation=gx.expectations.ExpectColumnValuesToBeBetween(
                column="year",
                min_value=1900,
                max_value=pd.Timestamp.now().year + 1,
            ),
            description="Año en dim_date dentro de rango histórico",
        )
    )
    # fact_music_award
    rules.append(
        QualityRule(
            rule_id="R-CUR-02-VALS-award-count",
            severity="critical",
            target_asset="fact_music_award",
            expectation=gx.expectations.ExpectColumnValuesToBeInSet(
                column="award_count",
                value_set=[1],
            ),
            description="award_count estrictamente igual a 1",
        )
    )
    rules.append(
        QualityRule(
            rule_id="R-CUR-02-VALS-award-winner",
            severity="critical",
            target_asset="fact_music_award",
            expectation=gx.expectations.ExpectColumnValuesToBeInSet(
                column="winner",
                value_set=[True, False, 1, 0, "True", "False"],
            ),
            description="winner booleano en fact_music_award",
        )
    )
    rules.append(
        QualityRule(
            rule_id="R-CUR-02-VALS-fk-date",
            severity="critical",
            target_asset="fact_music_award",
            expectation=gx.expectations.ExpectColumnValuesToBeInSet(
                column="date_sk",
                value_set=list(dim_date_keys),
            ),
            description="FK fact_music_award.date_sk existe en dim_date",
        )
    )
    rules.append(
        QualityRule(
            rule_id="R-CUR-02-VALS-fk-artist-award",
            severity="critical",
            target_asset="fact_music_award",
            expectation=gx.expectations.ExpectColumnValuesToBeInSet(
                column="artist_sk",
                value_set=list(dim_artist_keys),
            ),
            description="FK fact_music_award.artist_sk existe en dim_artist",
        )
    )
    # fact_spotify_artist_track_genre
    rules.append(
        QualityRule(
            rule_id="R-CUR-02-VALS-fk-track",
            severity="critical",
            target_asset="fact_spotify_artist_track_genre",
            expectation=gx.expectations.ExpectColumnValuesToBeInSet(
                column="track_sk",
                value_set=list(dim_track_keys),
            ),
            description="FK fact_spotify.track_sk existe en dim_cancion",
        )
    )
    rules.append(
        QualityRule(
            rule_id="R-CUR-02-VALS-fk-genre",
            severity="critical",
            target_asset="fact_spotify_artist_track_genre",
            expectation=gx.expectations.ExpectColumnValuesToBeInSet(
                column="genre_sk",
                value_set=list(dim_genre_keys),
            ),
            description="FK fact_spotify.genre_sk existe en dim_genero",
        )
    )
    rules.append(
        QualityRule(
            rule_id="R-CUR-02-VALS-fk-artist-spotify",
            severity="critical",
            target_asset="fact_spotify_artist_track_genre",
            expectation=gx.expectations.ExpectColumnValuesToBeInSet(
                column="artist_sk",
                value_set=list(dim_artist_keys),
            ),
            description="FK fact_spotify.artist_sk existe en dim_artist",
        )
    )
    rules.append(
        QualityRule(
            rule_id="R-CUR-02-VALS-spotify-popularity",
            severity="high",
            target_asset="fact_spotify_artist_track_genre",
            expectation=gx.expectations.ExpectColumnValuesToBeBetween(
                column="popularity",
                min_value=0,
                max_value=100,
            ),
            description="popularity en [0, 100]",
        )
    )
    rules.append(
        QualityRule(
            rule_id="R-CUR-02-VALS-spotify-danceability",
            severity="high",
            target_asset="fact_spotify_artist_track_genre",
            expectation=gx.expectations.ExpectColumnValuesToBeBetween(
                column="danceability",
                min_value=0.0,
                max_value=1.0,
            ),
            description="danceability en [0.0, 1.0]",
        )
    )
    rules.append(
        QualityRule(
            rule_id="R-CUR-02-VALS-spotify-energy",
            severity="high",
            target_asset="fact_spotify_artist_track_genre",
            expectation=gx.expectations.ExpectColumnValuesToBeBetween(
                column="energy",
                min_value=0.0,
                max_value=1.0,
            ),
            description="energy en [0.0, 1.0]",
        )
    )
    rules.append(
        QualityRule(
            rule_id="R-CUR-02-VALS-spotify-valence",
            severity="high",
            target_asset="fact_spotify_artist_track_genre",
            expectation=gx.expectations.ExpectColumnValuesToBeBetween(
                column="valence",
                min_value=0.0,
                max_value=1.0,
            ),
            description="valence en [0.0, 1.0]",
        )
    )

    # 3. R-CUR-03-DUPS: Unicidad de Claves Primarias y Grano Analítico
    for table_name, pk_cols in PRIMARY_KEYS.items():
        rules.append(
            QualityRule(
                rule_id=f"R-CUR-03-DUPS-pk-{table_name}",
                severity="critical",
                target_asset=table_name,
                expectation=gx.expectations.ExpectColumnValuesToBeUnique(
                    column=pk_cols[0]
                ),
                description=f"Unicidad de Primary Key {pk_cols[0]} en {table_name}",
            )
        )
    rules.append(
        QualityRule(
            rule_id="R-CUR-03-DUPS-artist-normalized",
            severity="critical",
            target_asset="dim_artist",
            expectation=gx.expectations.ExpectColumnValuesToBeUnique(
                column="artist_name_normalized"
            ),
            description="Unicidad de artist_name_normalized en dim_artist",
        )
    )
    rules.append(
        QualityRule(
            rule_id="R-CUR-03-DUPS-track-id",
            severity="critical",
            target_asset="dim_cancion",
            expectation=gx.expectations.ExpectColumnValuesToBeUnique(
                column="track_id"
            ),
            description="Unicidad de track_id en dim_cancion",
        )
    )
    rules.append(
        QualityRule(
            rule_id="R-CUR-03-DUPS-genre-name",
            severity="critical",
            target_asset="dim_genero",
            expectation=gx.expectations.ExpectColumnValuesToBeUnique(
                column="genre_name"
            ),
            description="Unicidad de genre_name en dim_genero",
        )
    )
    rules.append(
        QualityRule(
            rule_id="R-CUR-03-DUPS-year",
            severity="critical",
            target_asset="dim_date",
            expectation=gx.expectations.ExpectColumnValuesToBeUnique(
                column="year"
            ),
            description="Unicidad de year en dim_date",
        )
    )
    rules.append(
        QualityRule(
            rule_id="R-CUR-03-DUPS-spotify-grain",
            severity="critical",
            target_asset="fact_spotify_artist_track_genre",
            expectation=gx.expectations.ExpectCompoundColumnsToBeUnique(
                column_list=["track_sk", "artist_sk", "genre_sk"]
            ),
            description="Unicidad estricta del grano (track_sk, artist_sk, genre_sk)",
        )
    )

    result = run_multi_asset_expectation_suite(
        tables=tables,
        suite_name="curated_warehouse_suite",
        data_source_name="curated_warehouse_source",
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
