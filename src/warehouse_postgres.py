"""Load validated dimensional CSVs into the PostgreSQL warehouse schema."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd
from psycopg2 import sql
from psycopg2.extras import execute_values

from src.grammys_postgres import DATABASE_NAME, connect_postgres


LOGGER = logging.getLogger(__name__)
SCHEMA_NAME = "public"

TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
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

INTEGER_COLUMNS: dict[str, tuple[str, ...]] = {
    "dim_date": ("date_sk", "year", "decade"),
    "dim_artist": ("artist_sk",),
    "dim_cancion": ("track_sk", "duration_ms"),
    "dim_genero": ("genre_sk",),
    "fact_music_award": (
        "music_award_sk",
        "date_sk",
        "artist_sk",
        "award_count",
    ),
    "fact_spotify_artist_track_genre": (
        "spotify_credit_sk",
        "track_sk",
        "artist_sk",
        "genre_sk",
        "popularity",
    ),
}

CREATE_TABLE_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS public.dim_date (
        date_sk BIGINT PRIMARY KEY,
        year INTEGER NOT NULL UNIQUE,
        decade INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS public.dim_artist (
        artist_sk BIGINT PRIMARY KEY,
        artist_name TEXT NOT NULL,
        artist_name_normalized TEXT NOT NULL UNIQUE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS public.dim_cancion (
        track_sk BIGINT PRIMARY KEY,
        track_id TEXT NOT NULL UNIQUE,
        track_name TEXT,
        album_name TEXT,
        artists TEXT,
        duration_ms BIGINT,
        explicit BOOLEAN
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS public.dim_genero (
        genre_sk BIGINT PRIMARY KEY,
        genre_name TEXT NOT NULL UNIQUE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS public.fact_music_award (
        music_award_sk BIGINT PRIMARY KEY,
        date_sk BIGINT NOT NULL REFERENCES public.dim_date (date_sk),
        artist_sk BIGINT REFERENCES public.dim_artist (artist_sk),
        category TEXT NOT NULL,
        nominee TEXT,
        title TEXT,
        winner BOOLEAN,
        award_count INTEGER NOT NULL CHECK (award_count = 1)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS public.fact_spotify_artist_track_genre (
        spotify_credit_sk BIGINT PRIMARY KEY,
        track_sk BIGINT NOT NULL REFERENCES public.dim_cancion (track_sk),
        artist_sk BIGINT REFERENCES public.dim_artist (artist_sk),
        genre_sk BIGINT NOT NULL REFERENCES public.dim_genero (genre_sk),
        popularity SMALLINT NOT NULL CHECK (popularity BETWEEN 0 AND 100),
        danceability DOUBLE PRECISION NOT NULL
            CHECK (danceability BETWEEN 0 AND 1),
        energy DOUBLE PRECISION NOT NULL CHECK (energy BETWEEN 0 AND 1),
        valence DOUBLE PRECISION NOT NULL CHECK (valence BETWEEN 0 AND 1),
        UNIQUE (track_sk, artist_sk, genre_sk)
    )
    """,
)

INSERT_ORDER = tuple(TABLE_COLUMNS)
TRUNCATE_ORDER = (
    "fact_spotify_artist_track_genre",
    "fact_music_award",
    "dim_cancion",
    "dim_artist",
    "dim_genero",
    "dim_date",
)

def _postgres_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def _read_curated_tables(
    table_paths: dict[str, str],
) -> dict[str, pd.DataFrame]:
    if set(table_paths) != set(TABLE_COLUMNS):
        missing = sorted(set(TABLE_COLUMNS) - set(table_paths))
        unexpected = sorted(set(table_paths) - set(TABLE_COLUMNS))
        raise ValueError(
            "Curated table list does not match the warehouse model; "
            f"missing={missing}, unexpected={unexpected}."
        )

    tables: dict[str, pd.DataFrame] = {}
    for table_name, expected_columns in TABLE_COLUMNS.items():
        table_path = Path(table_paths[table_name])
        if not table_path.is_file():
            raise FileNotFoundError(
                f"Validated warehouse file does not exist: {table_path}"
            )
        dataframe = pd.read_csv(table_path)
        if tuple(str(column) for column in dataframe.columns) != expected_columns:
            raise ValueError(
                f"Warehouse input {table_path} has an unexpected schema. "
                f"Expected {expected_columns}, received {tuple(dataframe.columns)}."
            )
        for column in INTEGER_COLUMNS[table_name]:
            dataframe[column] = pd.to_numeric(
                dataframe[column],
                errors="raise",
            ).astype("Int64")
        tables[table_name] = dataframe
    return tables


def _ensure_warehouse_tables(connection: Any) -> None:
    with connection.cursor() as cursor:
        for statement in CREATE_TABLE_STATEMENTS:
            cursor.execute(statement)

        for table_name, expected_columns in TABLE_COLUMNS.items():
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (SCHEMA_NAME, table_name),
            )
            actual_columns = tuple(row[0] for row in cursor.fetchall())
            if actual_columns != expected_columns:
                raise RuntimeError(
                    f"Existing {SCHEMA_NAME}.{table_name} has an unexpected "
                    f"schema. Expected {expected_columns}, received {actual_columns}."
                )


def _insert_dataframe(
    connection: Any,
    table_name: str,
    dataframe: pd.DataFrame,
) -> None:
    if dataframe.empty:
        return

    columns = TABLE_COLUMNS[table_name]
    statement = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
        sql.Identifier(SCHEMA_NAME, table_name),
        sql.SQL(", ").join(sql.Identifier(column) for column in columns),
    )
    rows = (
        tuple(_postgres_value(value) for value in row)
        for row in dataframe.itertuples(index=False, name=None)
    )
    with connection.cursor() as cursor:
        execute_values(cursor, statement.as_string(connection), rows, page_size=1000)


def load_curated_warehouse(
    table_paths: dict[str, str],
) -> dict[str, object]:
    """Atomically replace the public dimensional warehouse snapshot."""
    tables = _read_curated_tables(table_paths)
    connection = connect_postgres(DATABASE_NAME)
    try:
        with connection:
            _ensure_warehouse_tables(connection)
            with connection.cursor() as cursor:
                truncate_statement = sql.SQL("TRUNCATE TABLE {}").format(
                    sql.SQL(", ").join(
                        sql.Identifier(SCHEMA_NAME, table_name)
                        for table_name in TRUNCATE_ORDER
                    )
                )
                cursor.execute(truncate_statement)

            for table_name in INSERT_ORDER:
                _insert_dataframe(connection, table_name, tables[table_name])
    finally:
        connection.close()

    row_counts = {
        table_name: len(dataframe) for table_name, dataframe in tables.items()
    }
    result: dict[str, object] = {
        "database": DATABASE_NAME,
        "schema": SCHEMA_NAME,
        "tables_loaded": len(row_counts),
        "rows_loaded": row_counts,
        "total_rows_loaded": sum(row_counts.values()),
        "load_strategy": "transactional_full_refresh",
    }
    LOGGER.info("Curated warehouse loaded into PostgreSQL: %s.", result)
    return result
