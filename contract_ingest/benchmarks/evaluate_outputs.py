from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

_TRACKED_REASON_CODES = {
    "UNSTABLE_CLAUSE_SPLIT": "unstable_clause_split_count",
    "REVERSED_CLAUSE_NUMBER": "reversed_clause_number_count",
    "MISSING_GOVERNING_LAW": "missing_governing_law_count",
    "MISSING_JURISDICTION": "missing_jurisdiction_count",
    "LOW_QUALITY_COUNTERPARTY": "low_quality_counterparty_count",
    "LOW_QUALITY_JURISDICTION": "low_quality_jurisdiction_count",
    "LOW_QUALITY_GOVERNING_LAW": "low_quality_governing_law_count",
    "ANCHOR_ONLY_EFFECTIVE_DATE": "anchor_only_effective_date_count",
}
_FILL_FIELDS = [
    "counterparties",
    "effective_date",
    "expiration_date",
    "governing_law",
    "jurisdiction",
]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate baseline outputs")
    parser.add_argument("--baseline-root", type=Path, default=Path("out/baseline"))
    parser.add_argument("--reports-dir", type=Path, default=Path("out/reports"))
    parser.add_argument("--manifest", type=Path, default=Path("out/reports/run_manifest.json"))
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    warnings: list[str] = []
    doc_dirs = _collect_document_dirs(args.baseline_root, args.manifest, warnings)

    documents: list[dict[str, Any]] = []
    for doc_dir in doc_dirs:
        documents.append(_evaluate_document(doc_dir, warnings))

    aggregate = _aggregate_documents(documents)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_root": str(args.baseline_root),
        "document_count": len(documents),
        "documents": documents,
        "aggregate": aggregate,
        "warnings": warnings,
    }

    args.reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = args.reports_dir / "eval_summary.json"
    csv_path = args.reports_dir / "eval_summary.csv"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(csv_path, documents)
    print(json.dumps({"json": str(json_path), "csv": str(csv_path), "documents": len(documents)}, ensure_ascii=False))
    return 0


def _collect_document_dirs(baseline_root: Path, manifest_path: Path, warnings: list[str]) -> list[Path]:
    doc_dirs: list[Path] = []
    seen: set[str] = set()

    manifest = _read_json(manifest_path, warnings, "manifest")
    if isinstance(manifest, dict) and isinstance(manifest.get("documents"), list):
        for entry in manifest["documents"]:
            if not isinstance(entry, dict):
                continue
            output_dir = entry.get("output_dir")
            if not isinstance(output_dir, str):
                continue
            path = Path(output_dir)
            key = str(path.resolve()) if path.exists() else str(path)
            if key in seen:
                continue
            seen.add(key)
            doc_dirs.append(path)

    if baseline_root.exists():
        for path in sorted(baseline_root.iterdir()):
            if not path.is_dir():
                continue
            key = str(path.resolve())
            if key in seen:
                continue
            seen.add(key)
            doc_dirs.append(path)
    else:
        warnings.append(f"baseline root missing: {baseline_root}")
    return doc_dirs


def _evaluate_document(doc_dir: Path, warnings: list[str]) -> dict[str, Any]:
    review_path = doc_dir / "review.json"
    document_path = doc_dir / "document.json"

    review = _read_json(review_path, warnings, f"review:{doc_dir.name}")
    document = _read_json(document_path, warnings, f"document:{doc_dir.name}")

    metrics: dict[str, Any] = {
        "pdf_name": doc_dir.name,
        "output_dir": str(doc_dir),
        "review_required": False,
        "review_total": 0,
        "warning_count": 0,
        "critical_count": 0,
        "field_fill_rate": 0.0,
        "field_quality_score": 0.5,
        "document_pass_fail": "fail",
        "degraded_mode": True,
        "top_issues": [],
    }
    for metric in _TRACKED_REASON_CODES.values():
        metrics[metric] = 0

    reason_counts: dict[str, int] = {}
    if isinstance(review, dict):
        metrics["review_required"] = bool(review.get("review_required", False))
        items = review.get("items")
        if isinstance(items, list):
            metrics["review_total"] = len(items)
            for item in items:
                if not isinstance(item, dict):
                    continue
                reason_codes = item.get("reason_codes")
                if reason_codes is None and isinstance(item.get("code"), str):
                    reason_codes = [item["code"]]
                elif isinstance(reason_codes, str):
                    reason_codes = [reason_codes]
                elif not isinstance(reason_codes, list):
                    reason_codes = []
                for reason in [str(code) for code in reason_codes if code]:
                    reason_counts[reason] = reason_counts.get(reason, 0) + 1
        summary = review.get("summary", {})
        if isinstance(summary, dict):
            metrics["warning_count"] = int(summary.get("warning_count", 0) or 0)
            metrics["critical_count"] = int(summary.get("critical_count", 0) or 0)
    else:
        warnings.append(f"review json unavailable or invalid: {review_path}")

    for reason_code, metric_name in _TRACKED_REASON_CODES.items():
        metrics[metric_name] = int(reason_counts.get(reason_code, 0))

    metrics["top_issues"] = _top_issues(reason_counts)

    fill_rate, quality_score, degraded = _evaluate_fields(document, reason_counts, warnings, doc_dir.name)
    metrics["field_fill_rate"] = fill_rate
    metrics["field_quality_score"] = quality_score
    metrics["degraded_mode"] = degraded
    metrics["document_pass_fail"] = _judge_pass_fail(metrics)

    return metrics


def _evaluate_fields(
    document: Any,
    reason_counts: dict[str, int],
    warnings: list[str],
    doc_name: str,
) -> tuple[float, float, bool]:
    penalties = 0.0
    penalties += 0.18 * reason_counts.get("LOW_QUALITY_COUNTERPARTY", 0)
    penalties += 0.20 * reason_counts.get("LOW_QUALITY_JURISDICTION", 0)
    penalties += 0.18 * reason_counts.get("LOW_QUALITY_GOVERNING_LAW", 0)
    penalties += 0.10 * reason_counts.get("ANCHOR_ONLY_EFFECTIVE_DATE", 0)

    if not isinstance(document, dict):
        # degraded mode: review only
        return 0.0, max(0.0, min(1.0, 0.80 - penalties)), True

    fields = document.get("fields")
    if not isinstance(fields, dict):
        warnings.append(f"document fields unavailable: {doc_name}")
        return 0.0, max(0.0, min(1.0, 0.75 - penalties)), True

    filled = 0
    quality_seen = False
    for field_name in _FILL_FIELDS:
        field_obj = fields.get(field_name)
        if not isinstance(field_obj, dict):
            continue
        if _is_filled_value(field_obj.get("value")):
            filled += 1

        quality = field_obj.get("quality")
        if isinstance(quality, dict):
            quality_seen = True
            if bool(quality.get("anchor_only")):
                penalties += 0.10
            quality_flags = quality.get("quality_flags")
            if isinstance(quality_flags, list):
                penalties += 0.04 * len([flag for flag in quality_flags if isinstance(flag, str)])

    fill_rate = filled / float(len(_FILL_FIELDS))
    base_score = 1.0
    degraded_mode = not quality_seen
    if degraded_mode:
        base_score = min(base_score, 0.85)
    quality_score = max(0.0, min(1.0, base_score - penalties))
    return fill_rate, quality_score, degraded_mode


def _is_filled_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, list):
        return any(isinstance(item, str) and item.strip() for item in value)
    if isinstance(value, bool):
        return True
    return bool(value)


def _top_issues(reason_counts: dict[str, int], top_n: int = 3) -> list[str]:
    ordered = sorted(reason_counts.items(), key=lambda item: (-item[1], item[0]))
    return [f"{code}:{count}" for code, count in ordered[:top_n]]


def _judge_pass_fail(metrics: dict[str, Any]) -> str:
    critical = int(metrics.get("critical_count", 0))
    missing_major = int(metrics.get("missing_governing_law_count", 0)) + int(metrics.get("missing_jurisdiction_count", 0))
    low_quality_total = (
        int(metrics.get("low_quality_counterparty_count", 0))
        + int(metrics.get("low_quality_jurisdiction_count", 0))
        + int(metrics.get("low_quality_governing_law_count", 0))
        + int(metrics.get("anchor_only_effective_date_count", 0))
    )
    fill_rate = float(metrics.get("field_fill_rate", 0.0))
    quality_score = float(metrics.get("field_quality_score", 0.0))

    if critical > 0:
        return "fail"
    if missing_major >= 2:
        return "fail"
    if low_quality_total >= 3:
        return "fail"
    if fill_rate < 0.60:
        return "fail"
    if quality_score < 0.55:
        return "fail"
    return "pass"


def _aggregate_documents(documents: list[dict[str, Any]]) -> dict[str, Any]:
    pass_count = sum(1 for doc in documents if doc.get("document_pass_fail") == "pass")
    fail_count = sum(1 for doc in documents if doc.get("document_pass_fail") == "fail")
    review_required_count = sum(1 for doc in documents if bool(doc.get("review_required")))
    avg_fill = sum(float(doc.get("field_fill_rate", 0.0)) for doc in documents) / max(len(documents), 1)
    avg_quality = sum(float(doc.get("field_quality_score", 0.0)) for doc in documents) / max(len(documents), 1)
    return {
        "pass_count": pass_count,
        "fail_count": fail_count,
        "review_required_count": review_required_count,
        "average_field_fill_rate": round(avg_fill, 4),
        "average_field_quality_score": round(avg_quality, 4),
    }


def _write_csv(path: Path, documents: list[dict[str, Any]]) -> None:
    columns = [
        "pdf_name",
        "review_required",
        "review_total",
        "warning_count",
        "critical_count",
        "unstable_clause_split_count",
        "reversed_clause_number_count",
        "missing_governing_law_count",
        "missing_jurisdiction_count",
        "low_quality_counterparty_count",
        "low_quality_jurisdiction_count",
        "low_quality_governing_law_count",
        "anchor_only_effective_date_count",
        "field_fill_rate",
        "field_quality_score",
        "document_pass_fail",
        "degraded_mode",
        "top_issues",
    ]
    with path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=columns)
        writer.writeheader()
        for doc in documents:
            row = {key: doc.get(key) for key in columns}
            top_issues = row.get("top_issues")
            if isinstance(top_issues, list):
                row["top_issues"] = "; ".join(str(item) for item in top_issues)
            writer.writerow(row)


def _read_json(path: Path, warnings: list[str], label: str) -> Any:
    if not path.exists():
        warnings.append(f"{label} missing: {path}")
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        warnings.append(f"{label} parse error: {path} ({exc})")
        return None


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
