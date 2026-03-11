from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print baseline evaluation report")
    parser.add_argument("--summary-json", type=Path, default=Path("out/reports/eval_summary.json"))
    parser.add_argument("--summary-csv", type=Path, default=Path("out/reports/eval_summary.csv"))
    parser.add_argument("--top", type=int, default=3, help="number of top issues to show")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    documents = _load_documents(args.summary_json, args.summary_csv)
    if not documents:
        print("No benchmark summary data found.")
        return 1

    print(_format_table(documents, top_n=max(1, args.top)))
    return 0


def _load_documents(summary_json: Path, summary_csv: Path) -> list[dict[str, Any]]:
    if summary_json.exists():
        try:
            payload = json.loads(summary_json.read_text(encoding="utf-8"))
            docs = payload.get("documents")
            if isinstance(docs, list):
                return [doc for doc in docs if isinstance(doc, dict)]
        except Exception:
            pass

    if summary_csv.exists():
        rows: list[dict[str, Any]] = []
        with summary_csv.open("r", encoding="utf-8", newline="") as fp:
            reader = csv.DictReader(fp)
            for row in reader:
                rows.append(dict(row))
        return rows
    return []


def _format_table(documents: list[dict[str, Any]], top_n: int) -> str:
    headers = [
        "pdf_name",
        "review_required",
        "review_total",
        "field_fill_rate",
        "field_quality_score",
        "document_pass_fail",
        "top_issues",
    ]
    rows: list[list[str]] = []
    for doc in documents:
        top_issues = doc.get("top_issues", [])
        if isinstance(top_issues, list):
            top_text = ", ".join(str(item) for item in top_issues[:top_n])
        else:
            top_text = str(top_issues or "")
        rows.append(
            [
                str(doc.get("pdf_name", "")),
                str(doc.get("review_required", "")),
                str(doc.get("review_total", "")),
                _format_ratio(doc.get("field_fill_rate")),
                _format_ratio(doc.get("field_quality_score")),
                str(doc.get("document_pass_fail", "")),
                top_text,
            ]
        )

    widths = [len(header) for header in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    def fmt_row(cells: list[str]) -> str:
        return " | ".join(cell.ljust(widths[idx]) for idx, cell in enumerate(cells))

    border = "-+-".join("-" * width for width in widths)
    lines = [fmt_row(headers), border]
    lines.extend(fmt_row(row) for row in rows)
    return "\n".join(lines)


def _format_ratio(value: Any) -> str:
    try:
        num = float(value)
    except Exception:
        return ""
    return f"{num:.2f}"


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
