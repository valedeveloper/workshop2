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
    schema_valid = (
        tuple(str(column) for column in dataframe.columns) == REQUIRED_COLUMNS
        and not malformed_rows
        and not dataframe.empty
    )
    required_fields_valid = all(
        column in dataframe
        and dataframe[column].notna().all()
        and dataframe[column].astype("string").str.strip().ne("").all()
        for column in ("year", "category", "winner")
    )
    year_values = (
        pd.to_numeric(dataframe["year"], errors="coerce")
        if "year" in dataframe
        else pd.Series(dtype="float64")
    )
    winner_values = (
        dataframe["winner"].astype("string").str.casefold()
        if "winner" in dataframe
        else pd.Series(dtype="string")
    )
    domains_valid = (
        not year_values.empty
        and year_values.between(1900, current_year + 1).all()
        and winner_values.isin({"true", "false"}).all()
    )
    checks = pd.DataFrame(
        {
            "schema_valid": [schema_valid],
            "required_fields_valid": [required_fields_valid],
            "domains_valid": [domains_valid],
        }
    )
    rules = [
        QualityRule(
            rule_id="Q01-schema",
            severity="critical",
            expectation=gx.expectations.ExpectColumnValuesToBeInSet(
                column="schema_valid",
                value_set=[True],
            ),
        ),
        QualityRule(
            rule_id="Q02-required-analytical-fields",
            severity="high",
            expectation=gx.expectations.ExpectColumnValuesToBeInSet(
                column="required_fields_valid",
                value_set=[True],
            ),
        ),
        QualityRule(
            rule_id="Q03-year-and-winner-domains",
            severity="high",
            expectation=gx.expectations.ExpectColumnValuesToBeInSet(
                column="domains_valid",
                value_set=[True],
            ),
        ),
    ]

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
            "Q10-winner-coverage: "
            f"nulls={null_winner_count}, both outcomes present={has_both_outcomes}; "
            "the source is winner-biased and does not support complete nomination "
            "or victory-rate KPIs."
        )
    result = run_expectation_suite(
        dataframe=checks,
        suite_name="grammys_quality_suite",
        data_source_name="grammys_pandas_source",
        asset_name="grammys_records",
        rules=rules,
    )
    if not has_both_outcomes or null_winner_count:
        result["warnings"].append(warning)
        print(f"WARNING: {warning}")
    print(f"Grammy winner value counts: {winner_counts}")
    print(f"Grammy GX validation summary: {result}")
    if report_path is not None:
        write_validation_report(report_path, batch_metadata or {}, result)
    raise_for_blocking_failures(result)
    return result
