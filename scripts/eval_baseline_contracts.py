#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

REQUIRED_TOP_LEVEL_KEYS = {
    "doc_id",
    "path",
    "doc_type",
    "language",
    "native_or_scanned",
    "expected_fields",
    "notes",
    "status",
}

REQUIRED_EXPECTED_FIELD_KEYS = {
    "governing_law",
    "jurisdiction",
    "effective_date",
    "expiration_date",
    "counterparties",
}

METRIC_SCHEMA = {
    "structural": [
        "clause_count_error",
        "reversed_clause_number_count",
        "section_boundary_uncertain_count",
        "header_footer_leakage_count",
        "signature_leakage_count",
        "appendix_form_contamination_count",
    ],
    "field": {
        "governing_law": ["exact", "partial", "missing"],
        "jurisdiction": ["exact", "partial", "missing"],
        "effective_date": ["exact", "placeholder", "missing"],
        "expiration_date": ["exact", "placeholder", "missing"],
        "counterparties": ["exact", "partial", "missing"],
    },
    "review_quality": [
        "review_item_count",
        "true_review_needed_count",
        "false_clean_count",
    ],
}

PLACEHOLDER_DATE_RE = re.compile(r"(?:[○◯〇□■]*\s*年\s*[○◯〇□■]*\s*月\s*[○◯〇□■]*\s*日|年\s*月\s*日)")


class ManifestError(Exception):
    pass


def _load_jsonl(manifest_path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    if not manifest_path.exists():
        raise ManifestError(f"manifest not found: {manifest_path}")
    for line_no, raw in enumerate(manifest_path.read_text(encoding="utf-8").splitlines(), start=1):
        text = raw.strip()
        if not text:
            continue
        try:
            row = json.loads(text)
        except json.JSONDecodeError as exc:
            errors.append(f"line {line_no}: invalid JSON ({exc})")
            continue
        if not isinstance(row, dict):
            errors.append(f"line {line_no}: row must be a JSON object")
            continue
        rows.append(row)
    return rows, errors


def _validate_manifest_row(row: dict[str, Any], index: int) -> list[str]:
    errors: list[str] = []
    missing_keys = sorted(REQUIRED_TOP_LEVEL_KEYS - set(row.keys()))
    if missing_keys:
        errors.append(f"row {index}: missing keys: {', '.join(missing_keys)}")

    expected_fields = row.get("expected_fields")
    if not isinstance(expected_fields, dict):
        errors.append(f"row {index}: expected_fields must be an object")
    else:
        missing_expected = sorted(REQUIRED_EXPECTED_FIELD_KEYS - set(expected_fields.keys()))
        if missing_expected:
            errors.append(
                f"row {index}: expected_fields missing keys: {', '.join(missing_expected)}"
            )

    for key in ["doc_id", "path", "doc_type", "language", "native_or_scanned", "notes", "status"]:
        if key in row and not isinstance(row[key], str):
            errors.append(f"row {index}: {key} must be a string")

    if "expected_structure" in row and not isinstance(row["expected_structure"], dict):
        errors.append(f"row {index}: expected_structure must be an object when present")

    return errors


def _resolve_doc_path(path_value: str, manifest_path: Path) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path

    cwd_candidate = Path.cwd() / path
    if cwd_candidate.exists():
        return cwd_candidate

    manifest_candidate = manifest_path.parent / path
    if manifest_candidate.exists():
        return manifest_candidate

    repo_candidate = manifest_path.parent.parent / path
    return repo_candidate


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return re.sub(r"\s+", "", value).lower()
    if isinstance(value, list):
        return ",".join(_normalize_text(item) for item in value)
    return re.sub(r"\s+", "", str(value)).lower()


def _is_placeholder_date(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    normalized = value.strip()
    return bool(PLACEHOLDER_DATE_RE.search(normalized))


def _extract_nested(data: dict[str, Any], path: list[str]) -> Any:
    current: Any = data
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def _extract_field_value(result: dict[str, Any], field_name: str) -> Any:
    candidate_paths = [
        ["fields", field_name, "value"],
        ["fields", field_name],
        ["result", "fields", field_name, "value"],
        ["result", "fields", field_name],
        ["extracted_fields", field_name],
    ]
    for path in candidate_paths:
        value = _extract_nested(result, path)
        if isinstance(value, dict) and "value" in value:
            return value.get("value")
        if value is not None:
            return value
    return None


def _extract_issues(result: dict[str, Any]) -> list[dict[str, Any]]:
    for path in (["issues"], ["result", "issues"]):
        value = _extract_nested(result, list(path))
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _extract_reason_code(issue: dict[str, Any]) -> str:
    reason = issue.get("reason_code")
    if isinstance(reason, str):
        return reason
    if isinstance(reason, dict):
        value = reason.get("value")
        if isinstance(value, str):
            return value
    return ""


def _extract_clause_count(result: dict[str, Any]) -> int | None:
    clauses = result.get("clauses")
    if isinstance(clauses, list):
        return len(clauses)

    structure_count = _extract_nested(result, ["structure", "clause_count"])
    if isinstance(structure_count, int):
        return structure_count
    return None


def _classify_field_match(field_name: str, expected: Any, actual: Any) -> str:
    if actual is None or actual == "" or actual == []:
        return "missing"

    if field_name in {"effective_date", "expiration_date"} and _is_placeholder_date(actual):
        return "placeholder"

    if expected is None or expected == "" or expected == []:
        return "unscored"

    if field_name == "counterparties":
        if isinstance(expected, list) and isinstance(actual, list):
            exp_set = {_normalize_text(x) for x in expected if x is not None}
            act_set = {_normalize_text(x) for x in actual if x is not None}
            if exp_set and exp_set == act_set:
                return "exact"
            if exp_set and act_set and exp_set.intersection(act_set):
                return "partial"
            return "missing"

    expected_norm = _normalize_text(expected)
    actual_norm = _normalize_text(actual)
    if expected_norm == actual_norm:
        return "exact"
    if expected_norm and actual_norm and (expected_norm in actual_norm or actual_norm in expected_norm):
        return "partial"
    return "missing"


def _evaluate_against_results(rows: list[dict[str, Any]], results_dir: Path) -> dict[str, Any]:
    field_metrics: dict[str, Counter[str]] = {
        field: Counter() for field in REQUIRED_EXPECTED_FIELD_KEYS
    }
    structural_metrics = Counter()
    review_quality = Counter()

    evaluated_docs = 0
    missing_result_files = 0
    unknown_review_expectation = 0

    for row in rows:
        result_path = results_dir / f"{row['doc_id']}.json"
        if not result_path.exists():
            missing_result_files += 1
            continue

        try:
            result = json.loads(result_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            missing_result_files += 1
            continue
        if not isinstance(result, dict):
            missing_result_files += 1
            continue

        evaluated_docs += 1
        expected_fields = row["expected_fields"]
        for field_name in REQUIRED_EXPECTED_FIELD_KEYS:
            expected = expected_fields.get(field_name)
            actual = _extract_field_value(result, field_name)
            classification = _classify_field_match(field_name, expected, actual)
            field_metrics[field_name][classification] += 1

        expected_structure = row.get("expected_structure") or {}
        expected_clause_count = expected_structure.get("clause_count")
        actual_clause_count = _extract_clause_count(result)
        if isinstance(expected_clause_count, int) and isinstance(actual_clause_count, int):
            diff = abs(expected_clause_count - actual_clause_count)
            structural_metrics["clause_count_error"] += diff

        issues = _extract_issues(result)
        review_quality["review_item_count"] += len(issues)

        reason_codes = [_extract_reason_code(issue) for issue in issues]
        structural_metrics["reversed_clause_number_count"] += reason_codes.count("REVERSED_CLAUSE_NUMBER")
        structural_metrics["section_boundary_uncertain_count"] += reason_codes.count("SECTION_BOUNDARY_UNCERTAIN")

        for issue in issues:
            reason = _extract_reason_code(issue)
            details = issue.get("details")
            if not isinstance(details, dict):
                details = {}

            if reason == "SECTION_BOUNDARY_UNCERTAIN":
                category_counts = details.get("category_counts")
                if isinstance(category_counts, dict):
                    form_count = int(category_counts.get("form_or_instruction_boundary", 0) or 0)
                    appendix_count = int(category_counts.get("appendix_boundary", 0) or 0)
                    signature_count = int(category_counts.get("signature_boundary", 0) or 0)
                    structural_metrics["appendix_form_contamination_count"] += form_count + appendix_count
                    structural_metrics["signature_leakage_count"] += signature_count

            text_blob = " ".join([
                reason,
                str(issue.get("message", "")),
                json.dumps(details, ensure_ascii=False),
            ]).lower()
            if "header" in text_blob or "footer" in text_blob:
                structural_metrics["header_footer_leakage_count"] += 1
            if "signature" in text_blob and reason != "SECTION_BOUNDARY_UNCERTAIN":
                structural_metrics["signature_leakage_count"] += 1
            if "appendix" in text_blob or "form" in text_blob or "instruction" in text_blob:
                structural_metrics["appendix_form_contamination_count"] += 1

        review_expectation = row.get("review_expectation")
        predicted_review_needed = len(issues) > 0
        expected_review_needed = None
        if isinstance(review_expectation, dict):
            flag = review_expectation.get("review_needed")
            if isinstance(flag, bool):
                expected_review_needed = flag

        if expected_review_needed is None:
            unknown_review_expectation += 1
        else:
            if expected_review_needed and predicted_review_needed:
                review_quality["true_review_needed_count"] += 1
            if expected_review_needed and not predicted_review_needed:
                review_quality["false_clean_count"] += 1

    return {
        "evaluated_docs": evaluated_docs,
        "missing_result_files": missing_result_files,
        "unknown_review_expectation_docs": unknown_review_expectation,
        "structural_metrics": dict(structural_metrics),
        "field_metrics": {field: dict(counts) for field, counts in field_metrics.items()},
        "review_quality": dict(review_quality),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate baseline manifest and optionally score extracted outputs."
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        required=True,
        help="Path to baseline manifest JSONL.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate manifest and readiness summary without reading extraction outputs.",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=None,
        help="Optional directory containing extraction outputs as <doc_id>.json.",
    )
    args = parser.parse_args()

    try:
        rows, parse_errors = _load_jsonl(args.manifest)
    except ManifestError as exc:
        print(f"[error] {exc}")
        return 2

    validation_errors = list(parse_errors)
    for idx, row in enumerate(rows, start=1):
        validation_errors.extend(_validate_manifest_row(row, idx))

    by_status = Counter()
    by_doc_type = Counter()
    by_language = Counter()
    by_native_or_scanned = Counter()

    file_exists = 0
    file_missing = 0
    extraction_ready = 0
    extraction_blocked = 0

    for row in rows:
        by_status[row.get("status", "")] += 1
        by_doc_type[row.get("doc_type", "")] += 1
        by_language[row.get("language", "")] += 1
        by_native_or_scanned[row.get("native_or_scanned", "")] += 1

        resolved_path = _resolve_doc_path(str(row.get("path", "")), args.manifest)
        exists = resolved_path.exists()
        if exists:
            file_exists += 1
        else:
            file_missing += 1

        status = row.get("status")
        if status == "ready" and exists:
            extraction_ready += 1
        elif status in {"ready", "needs_local_pdf"}:
            extraction_blocked += 1

    print("=== Baseline Manifest Summary ===")
    print(json.dumps({
        "manifest": str(args.manifest),
        "rows": len(rows),
        "validation_errors": len(validation_errors),
        "counts": {
            "status": dict(by_status),
            "doc_type": dict(by_doc_type),
            "language": dict(by_language),
            "native_or_scanned": dict(by_native_or_scanned),
        },
        "availability": {
            "pdf_exists": file_exists,
            "pdf_missing": file_missing,
            "extraction_ready_docs": extraction_ready,
            "extraction_blocked_docs": extraction_blocked,
        },
    }, ensure_ascii=False, indent=2))

    if validation_errors:
        print("=== Manifest Validation Errors ===")
        for err in validation_errors:
            print(f"- {err}")

    print("=== Metric Schema ===")
    print(json.dumps(METRIC_SCHEMA, ensure_ascii=False, indent=2))

    if args.validate_only:
        print("=== Evaluation Availability ===")
        print("validate-only mode: extraction result comparison was not executed")
        return 2 if validation_errors else 0

    if args.results_dir is None:
        print("=== Evaluation Availability ===")
        print("no --results-dir provided; extraction result comparison is not available")
        return 2 if validation_errors else 0

    results_dir = args.results_dir
    if not results_dir.exists() or not results_dir.is_dir():
        print("=== Evaluation Availability ===")
        print(f"results directory not found or not a directory: {results_dir}")
        return 2 if validation_errors else 0

    evaluation = _evaluate_against_results(rows, results_dir)
    print("=== Evaluation Summary ===")
    print(json.dumps(evaluation, ensure_ascii=False, indent=2))

    return 2 if validation_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
