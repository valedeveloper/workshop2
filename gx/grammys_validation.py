"""Great Expectations validation for a batch extracted from PostgreSQL."""

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


REQUIRED_COLUMNS = (
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

def run_validation(
    data_path: Path,
    report_path: Path | None = None,
    batch_metadata: dict[str, object] | None = None,
) -> ValidationSummary:
    """Validate a PostgreSQL-extracted Grammy batch without cleaning its values."""
    if batch_metadata is None or batch_metadata.get("source_type") != "postgresql":
        raise ValueError(
            "Grammy validation requires a batch extracted from PostgreSQL "
            "workshop2.public.grammys."
        )

    dataframe = pd.read_csv(data_path)
    malformed_rows_value = batch_metadata.get("malformed_physical_lines", [])
    if not isinstance(malformed_rows_value, list) or not all(
        isinstance(line, int) for line in malformed_rows_value
    ):
        raise ValueError(
            "PostgreSQL batch metadata has invalid malformed-CSV line numbers."
        )
    malformed_rows = malformed_rows_value

    current_year = pd.Timestamp.now().year
    dataframe["year"] = pd.to_numeric(dataframe["year"], errors="coerce")

    rules = [
        # 1. Columnas (Contrato y esquema de la extracción relacional)
        QualityRule(
            rule_id="R-GRAM-01-COLS",
            severity="critical",
            expectation=gx.expectations.ExpectTableColumnsToMatchSet(
                column_set=list(REQUIRED_COLUMNS),
                exact_match=True,
            ),
            description="Esquema exacto de 10 columnas extraídas de PostgreSQL",
        ),
        # 2. Valores permitidos (Dominios y no-nulidad)
        QualityRule(
            rule_id="R-GRAM-02-VALS-year-null",
            severity="critical",
            expectation=gx.expectations.ExpectColumnValuesToNotBeNull(
                column="year"
            ),
            description="Campo year no nulo",
        ),
        QualityRule(
            rule_id="R-GRAM-02-VALS-year-range",
            severity="critical",
            expectation=gx.expectations.ExpectColumnValuesToBeBetween(
                column="year",
                min_value=1900,
                max_value=current_year + 1,
            ),
            description=f"Campo year en rango histórico [1900, {current_year + 1}]",
        ),
        QualityRule(
            rule_id="R-GRAM-02-VALS-category",
            severity="critical",
            expectation=gx.expectations.ExpectColumnValuesToNotBeNull(
                column="category"
            ),
            description="Campo category no nulo",
        ),
        QualityRule(
            rule_id="R-GRAM-02-VALS-winner",
            severity="critical",
            expectation=gx.expectations.ExpectColumnValuesToNotBeNull(
                column="winner"
            ),
            description="Campo winner no nulo",
        ),
        # 3. Filas repetidas (Unicidad de registros)
        QualityRule(
            rule_id="R-GRAM-03-DUPS-rows",
            severity="high",
            expectation=gx.expectations.ExpectCompoundColumnsToBeUnique(
                column_list=list(REQUIRED_COLUMNS),
            ),
            description="Ausencia de filas duplicadas en el lote extraído",
        ),
        QualityRule(
            rule_id="R-GRAM-03-DUPS-event",
            severity="high",
            expectation=gx.expectations.ExpectCompoundColumnsToBeUnique(
                column_list=["year", "category", "nominee", "title", "artist"],
            ),
            description="Unicidad del evento de premiación (year, category, nominee, title, artist)",
        ),
    ]

    result = run_expectation_suite(
        dataframe=dataframe,
        suite_name="grammys_quality_suite",
        data_source_name="grammys_pandas_source",
        asset_name="grammys_records",
        rules=rules,
    )

    if malformed_rows:
        malformed_msg = (
            f"R-GRAM-01-COLS failed (critical): raw batch contains "
            f"malformed physical lines: {malformed_rows}"
        )
        result["blocking_failures"].append(malformed_msg)
        result["failed"] += 1

    winner_counts = (
        dataframe["winner"].value_counts(dropna=False).to_dict()
        if "winner" in dataframe
        else {}
    )
    null_winner_count = (
        int(dataframe["winner"].isna().sum()) if "winner" in dataframe else len(dataframe)
    )
    winner_values = {
        value.casefold() if isinstance(value, str) else value
        for value in dataframe["winner"].dropna().unique()
    } if "winner" in dataframe else set()
    has_both_outcomes = {True, False}.issubset(winner_values) or {
        "true",
        "false",
    }.issubset(winner_values)
    if not has_both_outcomes or null_winner_count:
        warning = (
            "R-GRAM-02-VALS-coverage: "
            f"nulls={null_winner_count}, both outcomes present={has_both_outcomes}; "
            "the source is winner-biased and does not support complete nomination "
            "or victory-rate KPIs."
        )
        result["warnings"].append(warning)
        print(f"WARNING: {warning}")

    null_artist_count = int(dataframe["artist"].isna().sum())
    if null_artist_count:
        artist_warning = (
            f"R-GRAM-02-VALS-artist-coverage: {null_artist_count}/{len(dataframe)} "
            f"records ({round(100.0 * null_artist_count / len(dataframe), 2)}%) have null artist; "
            "these awards will be assigned artist_sk = NULL without artificial imputation."
        )
        result["warnings"].append(artist_warning)
    print(f"Grammy winner value counts: {winner_counts}")
    print(f"Grammy GX validation summary: {result}")
    if report_path is not None:
        write_validation_report(report_path, batch_metadata or {}, result)
    raise_for_blocking_failures(result)
    return result
