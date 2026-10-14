"""Materialize curated warehouse tables for the Grammy and Spotify sources."""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = PROJECT_ROOT / "data" / "raw"
TRANSFORM_ROOT = PROJECT_ROOT / "data" / "transform"

GRAMMY_BATCH_ROOT = PROJECT_ROOT / "data" / "batches" / "grammys"
SPOTIFY_BATCH_ROOT = PROJECT_ROOT / "data" / "batches" / "spotify"

LOGGER = logging.getLogger(__name__)


def _normalize_text(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _normalize_artist_name(value: Any) -> str:
    text = _normalize_text(value)
    if not text or text.casefold() in {"n/a", "na", "nan", "none", "null"}:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def _artist_name_key(value: Any) -> str:
    return _normalize_artist_name(value).casefold()


def _normalize_boolean(value: Any) -> bool | None:
    if pd.isna(value):
        return None
    if isinstance(value, str):
        value = value.strip().casefold()
        if value in {"true", "t", "yes", "y", "1"}:
            return True
        if value in {"false", "f", "no", "n", "0"}:
            return False
        return None
    return bool(value)


def _resolve_spotify_source() -> Path:
    accepted_paths = list(SPOTIFY_BATCH_ROOT.glob("**/spotify_accepted.csv"))
    raw_accepted_path = RAW_ROOT / "spotify_accepted.csv"
    if raw_accepted_path.is_file():
        accepted_paths.append(raw_accepted_path)
    if accepted_paths:
        return max(accepted_paths, key=lambda path: path.stat().st_mtime)
    raise FileNotFoundError(
        "No accepted Spotify batch found. Run the raw Great Expectations gate "
        "before transforming Spotify."
    )


def _resolve_grammy_source() -> Path:
    valid_batches: list[Path] = []
    for candidate in GRAMMY_BATCH_ROOT.glob("*/the_grammy_awards.csv"):
        manifest_path = candidate.parent / "manifest.json"
        if not manifest_path.is_file():
            continue
        with manifest_path.open(encoding="utf-8") as manifest_file:
            manifest = json.load(manifest_file)
        if (
            manifest.get("source_type") == "postgresql"
            and manifest.get("database") == "workshop2"
            and manifest.get("table") == "grammys"
        ):
            valid_batches.append(candidate)
    if valid_batches:
        return max(valid_batches, key=lambda path: path.stat().st_mtime)
    raise FileNotFoundError(
        "No Grammy batch extracted from workshop2.public.grammys was found. "
        "Run the PostgreSQL extraction and raw quality gate before transforming Grammy."
    )


def _write_csv(dataframe: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(path, index=False)


def _build_artist_dimension(
    grammy_frame: pd.DataFrame,
    spotify_frame: pd.DataFrame,
) -> pd.DataFrame:
    grammy_names = grammy_frame["artist"].map(_normalize_artist_name)
    spotify_names = spotify_frame["artists"].map(
        lambda value: [
            name
            for name in (
                _normalize_artist_name(part)
                for part in _normalize_text(value).split(";")
            )
            if name
        ]
    ).explode()
    artist_frame = pd.concat([grammy_names, spotify_names], ignore_index=True)
    artist_frame = pd.DataFrame({"artist_name": artist_frame})
    artist_frame["artist_name"] = artist_frame["artist_name"].map(
        _normalize_artist_name
    )
    artist_frame["artist_name_normalized"] = artist_frame[
        "artist_name"
    ].map(_artist_name_key)
    artist_frame = artist_frame.loc[
        artist_frame["artist_name_normalized"].ne(""),
        ["artist_name", "artist_name_normalized"],
    ]
    artist_frame = (
        artist_frame.sort_values(["artist_name_normalized", "artist_name"])
        .drop_duplicates(subset=["artist_name_normalized"])
        .reset_index(drop=True)
    )
    artist_frame.insert(0, "artist_sk", range(1, len(artist_frame) + 1))
    return artist_frame


def transform_grammys_curated(
    source_path: Path | None = None,
    output_dir: Path | None = None,
    artist_dimension: pd.DataFrame | None = None,
) -> dict[str, Path]:
    """Construct the annual date and Grammy fact tables."""
    if source_path is None:
        source_path = _resolve_grammy_source()
    if output_dir is None:
        output_dir = TRANSFORM_ROOT
    grammy_frame = pd.read_csv(source_path)
    grammy_frame = grammy_frame.copy()
    grammy_frame["year"] = pd.to_numeric(grammy_frame["year"], errors="coerce")
    grammy_frame["winner"] = grammy_frame["winner"].map(_normalize_boolean)
    grammy_frame = grammy_frame[
        grammy_frame["year"].notna()
        & grammy_frame["category"].notna()
        & grammy_frame["category"].astype(str).str.strip().ne("")
    ].copy()
    grammy_frame["year"] = grammy_frame["year"].astype("Int64")

    if artist_dimension is None:
        raise ValueError(
            "A cross-source artist dimension is required for Grammy transformation."
        )
    dim_artist = artist_dimension
    dim_date = (
        grammy_frame[["year"]]
        .drop_duplicates()
        .sort_values("year")
        .reset_index(drop=True)
    )
    dim_date.insert(0, "date_sk", range(1, len(dim_date) + 1))
    dim_date["decade"] = (dim_date["year"] // 10) * 10

    fact_frame = grammy_frame[
        ["year", "artist", "category", "nominee", "title", "winner"]
    ].copy()
    fact_frame["music_award_sk"] = range(1, len(fact_frame) + 1)
    artist_lookup = dim_artist.set_index("artist_name_normalized")["artist_sk"].to_dict()
    fact_frame["artist_sk"] = (
        fact_frame["artist"].map(_artist_name_key).map(artist_lookup)
    )
    date_lookup = dim_date.set_index("year")["date_sk"].to_dict()
    fact_frame["date_sk"] = fact_frame["year"].map(date_lookup)
    fact_music_award = fact_frame[
        [
            "music_award_sk",
            "date_sk",
            "artist_sk",
            "category",
            "nominee",
            "title",
            "winner",
        ]
    ].copy()
    fact_music_award["artist_sk"] = fact_music_award["artist_sk"].astype("Int64")
    fact_music_award["award_count"] = 1

    output_dir.mkdir(parents=True, exist_ok=True)
    dim_date_path = output_dir / "dim_date.csv"
    dim_artist_path = output_dir / "dim_artist.csv"
    fact_music_award_path = output_dir / "fact_music_award.csv"

    _write_csv(dim_date, dim_date_path)
    _write_csv(dim_artist, dim_artist_path)
    _write_csv(fact_music_award, fact_music_award_path)

    return {
        "dim_date": dim_date_path,
        "dim_artist": dim_artist_path,
        "fact_music_award": fact_music_award_path,
    }


def transform_spotify_curated(
    source_path: Path | None = None,
    output_dir: Path | None = None,
    artist_dimension: pd.DataFrame | None = None,
) -> dict[str, Path]:
    """Construct Spotify facts at the song-artist-genre credit grain."""
    if source_path is None:
        source_path = _resolve_spotify_source()
    if output_dir is None:
        output_dir = TRANSFORM_ROOT

    spotify_frame = pd.read_csv(source_path)
    spotify_frame = spotify_frame.copy()
    spotify_frame["track_id"] = spotify_frame["track_id"].astype("string")
    spotify_frame["track_genre"] = spotify_frame["track_genre"].astype("string")
    spotify_frame["artists"] = spotify_frame["artists"].fillna("").astype(str)
    for column in (
        "popularity",
        "duration_ms",
        "danceability",
        "energy",
        "valence",
    ):
        spotify_frame[column] = pd.to_numeric(
            spotify_frame[column],
            errors="coerce",
        )
    if "duration_ms" in spotify_frame:
        spotify_frame.loc[spotify_frame["duration_ms"].le(0), "duration_ms"] = pd.NA

    spotify_frame = spotify_frame[
        spotify_frame["track_id"].notna() & spotify_frame["track_genre"].notna()
    ].copy()
    if artist_dimension is None:
        raise ValueError(
            "A cross-source artist dimension is required for Spotify transformation."
        )
    dim_artist = artist_dimension

    dim_genero = (
        spotify_frame[["track_genre"]]
        .drop_duplicates()
        .rename(columns={"track_genre": "genre_name"})
        .sort_values("genre_name")
        .reset_index(drop=True)
    )
    dim_genero.insert(0, "genre_sk", range(1, len(dim_genero) + 1))

    dim_cancion = (
        spotify_frame[
            [
                "track_id",
                "track_name",
                "album_name",
                "artists",
                "duration_ms",
                "explicit",
            ]
        ]
        .drop_duplicates(subset=["track_id"])
        .dropna(subset=["track_id"])
        .sort_values("track_id")
        .reset_index(drop=True)
    )
    dim_cancion.insert(0, "track_sk", range(1, len(dim_cancion) + 1))

    fact_frame = spotify_frame[
        [
            "track_id",
            "track_genre",
            "artists",
            "popularity",
            "danceability",
            "energy",
            "valence",
        ]
    ].copy()
    fact_frame["artist_name"] = fact_frame["artists"].map(
        lambda value: sorted(
            {
                _normalize_artist_name(part)
                for part in value.split(";")
                if _normalize_artist_name(part)
            }
        )
        or [None]
    )
    fact_frame = fact_frame.explode("artist_name", ignore_index=True)
    artist_lookup = dim_artist.set_index("artist_name_normalized")["artist_sk"].to_dict()
    fact_frame["artist_sk"] = (
        fact_frame["artist_name"].map(_artist_name_key).map(artist_lookup)
    ).astype("Int64")
    fact_frame = fact_frame.merge(
        dim_cancion[["track_sk", "track_id"]],
        on="track_id",
        how="left",
    ).merge(
        dim_genero,
        left_on="track_genre",
        right_on="genre_name",
        how="left",
    )
    fact_spotify_artist_track_genre = fact_frame[
        [
            "track_sk",
            "artist_sk",
            "genre_sk",
            "popularity",
            "danceability",
            "energy",
            "valence",
        ]
    ].copy()
    fact_spotify_artist_track_genre = fact_spotify_artist_track_genre.drop_duplicates(
        subset=["track_sk", "artist_sk", "genre_sk"],
        keep="first",
    ).reset_index(drop=True)
    fact_spotify_artist_track_genre.insert(
        0,
        "spotify_credit_sk",
        range(1, len(fact_spotify_artist_track_genre) + 1),
    )
    fact_spotify_artist_track_genre["track_sk"] = (
        fact_spotify_artist_track_genre["track_sk"].astype(
        "Int64"
        )
    )
    fact_spotify_artist_track_genre["genre_sk"] = (
        fact_spotify_artist_track_genre["genre_sk"].astype("Int64")
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    dim_genero_path = output_dir / "dim_genero.csv"
    dim_cancion_path = output_dir / "dim_cancion.csv"
    fact_spotify_artist_track_genre_path = (
        output_dir / "fact_spotify_artist_track_genre.csv"
    )

    _write_csv(dim_cancion, dim_cancion_path)
    _write_csv(dim_genero, dim_genero_path)
    _write_csv(
        fact_spotify_artist_track_genre,
        fact_spotify_artist_track_genre_path,
    )

    return {
        "dim_cancion": dim_cancion_path,
        "dim_genero": dim_genero_path,
        "fact_spotify_artist_track_genre": fact_spotify_artist_track_genre_path,
    }


def run_transformations(
    grammy_source: Path | None = None,
    spotify_source: Path | None = None,
    output_dir: Path | None = None,
) -> dict[str, dict[str, Path]]:
    """Create both curated subject areas with one consistent artist dimension."""
    if output_dir is None:
        output_dir = TRANSFORM_ROOT
    output_dir.mkdir(parents=True, exist_ok=True)

    if spotify_source is None:
        spotify_source = _resolve_spotify_source()
    if grammy_source is None:
        grammy_source = _resolve_grammy_source()
    grammy_frame = pd.read_csv(grammy_source)
    spotify_frame = pd.read_csv(spotify_source)
    artist_dimension = _build_artist_dimension(grammy_frame, spotify_frame)
    grammy_tables = transform_grammys_curated(
        source_path=grammy_source,
        output_dir=output_dir,
        artist_dimension=artist_dimension,
    )
    spotify_tables = transform_spotify_curated(
        source_path=spotify_source,
        output_dir=output_dir,
        artist_dimension=artist_dimension,
    )
    return {
        "grammy": grammy_tables,
        "spotify": spotify_tables,
    }


if __name__ == "__main__":
    results = run_transformations()
    for domain, tables in results.items():
        print(f"{domain.upper()} tables generated:")
        for table_name, path in tables.items():
            print(f"  - {table_name}: {path}")
