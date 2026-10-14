"""Shared Great Expectations setup and severity handling."""

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
from typing import Any, NotRequired, TypedDict

import great_expectations as gx
import pandas as pd


LOGGER = logging.getLogger(__name__)


class ValidationSummary(TypedDict):
    suite_name: str
    expectations: int
    passed: int
    failed: int
    blocking_failures: list[str]
    warnings: list[str]
    informational: list[str]
    rules: list[dict[str, object]]
    accepted_path: NotRequired[str]
    normalized_path: NotRequired[str]
    source_rows: NotRequired[int]
    normalized_rows: NotRequired[int]
    duplicate_rows_removed: NotRequired[int]
    validated_files_sha256: NotRequired[dict[str, str]]


class RuleResult(TypedDict):
    rule_id: str
    severity: str
    success: bool
    evaluated_count: int | None
    unexpected_count: int | None
    unexpected_percent: float | None
    message: str | None


@dataclass(frozen=True)
class QualityRule:
    rule_id: str
    severity: str
    expectation: Any


class QualityGateFailure(ValueError):
    """A completed validation whose blocking expectations did not pass."""


def run_expectation_suite(
    dataframe: pd.DataFrame,
    suite_name: str,
    data_source_name: str,
    asset_name: str,
    rules: list[QualityRule],
) -> ValidationSummary:
    """Register and execute a code-defined suite against a Pandas batch."""
    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas(name=data_source_name)
    data_asset = data_source.add_dataframe_asset(name=asset_name)
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        name=f"{asset_name}_whole_dataframe"
    )
    batch = batch_definition.get_batch(
        batch_parameters={"dataframe": dataframe}
    )

    suite = gx.ExpectationSuite(name=suite_name)
    for rule in rules:
        suite.add_expectation(rule.expectation)
    context.suites.add(suite)

    failures: list[str] = []
    warnings: list[str] = []
    informational: list[str] = []
    rule_results: list[RuleResult] = []
    passed = 0
    failed = 0
    for rule in rules:
        validation_result = batch.validate(rule.expectation)
        metrics = validation_result.result
        unexpected_count = metrics.get("unexpected_count")
        element_count = metrics.get("element_count")
        unexpected_percent = metrics.get("unexpected_percent")
        if validation_result.success:
            passed += 1
            rule_results.append(
                {
                    "rule_id": rule.rule_id,
                    "severity": rule.severity,
                    "success": True,
                    "evaluated_count": element_count,
                    "unexpected_count": unexpected_count,
                    "unexpected_percent": unexpected_percent,
                    "message": None,
                }
            )
            LOGGER.info("%s passed (%s).", rule.rule_id, rule.severity)
            continue

        failed += 1
        message = (
            f"{rule.rule_id} failed ({rule.severity}); "
            f"unexpected={unexpected_count}, evaluated={element_count}."
        )
        rule_results.append(
            {
                "rule_id": rule.rule_id,
                "severity": rule.severity,
                "success": False,
                "evaluated_count": element_count,
                "unexpected_count": unexpected_count,
                "unexpected_percent": unexpected_percent,
                "message": message,
            }
        )
        severity = rule.severity.casefold()
        if severity in {"critical", "high"}:
            failures.append(message)
            LOGGER.error(message)
        elif severity in {"warning", "warn"}:
            warnings.append(message)
            LOGGER.warning(message)
        elif severity in {"informational", "information", "info"}:
            informational.append(message)
            LOGGER.info(message)
        else:
            raise ValueError(
                f"Unsupported severity {rule.severity!r} for rule {rule.rule_id!r}."
            )

    summary: ValidationSummary = {
        "suite_name": suite_name,
        "expectations": len(rules),
        "passed": passed,
        "failed": failed,
        "blocking_failures": failures,
        "warnings": warnings,
        "informational": informational,
        "rules": rule_results,
    }
    LOGGER.info("GX validation summary: %s", summary)
    return summary


def write_validation_report(
    report_path: Path,
    batch_metadata: dict[str, object],
    summary: ValidationSummary,
) -> None:
    """Persist a JSON summary for the exact immutable batch evaluated."""
    if summary["blocking_failures"]:
        status = "failed"
    elif summary["warnings"]:
        status = "passed_with_warnings"
    else:
        status = "passed"

    _write_report_atomically(
        report_path,
        {
            "status": status,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "batch": batch_metadata,
            "validation": summary,
        },
    )
    LOGGER.info("Validation report saved to %s (status=%s).", report_path, status)


def write_failure_report(
    report_path: Path,
    batch_metadata: dict[str, object],
    error: Exception,
) -> None:
    """Persist an explicit failure if validation could not produce a GX summary."""
    _write_report_atomically(
        report_path,
        {
            "status": "error",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "batch": batch_metadata,
            "error": {
                "type": type(error).__name__,
                "message": str(error),
            },
        },
    )
    LOGGER.error(
        "Validation could not produce a Great Expectations summary; "
        "failure report saved to %s (%s: %s).",
        report_path,
        type(error).__name__,
        error,
    )


def _write_report_atomically(
    report_path: Path, payload: dict[str, object]
) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = report_path.with_name(f".{report_path.name}.tmp")
    try:
        temporary_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary_path, report_path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise


def raise_for_blocking_failures(summary: ValidationSummary) -> None:
    """Fail the calling Airflow task after all checks and warnings are reported."""
    failures = summary["blocking_failures"]
    if failures:
        raise QualityGateFailure(
            f"Great Expectations suite {summary['suite_name']!r} failed: "
            + "; ".join(failures)
        )
