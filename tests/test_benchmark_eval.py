from __future__ import annotations

import json
from pathlib import Path

from contract_ingest.benchmarks import evaluate_outputs, report_baseline


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def test_evaluate_outputs_supports_review_reason_codes_and_degraded_mode(tmp_path) -> None:
    baseline_root = tmp_path / "baseline"
    reports_dir = tmp_path / "reports"

    # document with review + document quality present
    doc_a = baseline_root / "doc_a"
    _write_json(
        doc_a / "review.json",
        {
            "doc_id": "doc_a",
            "review_required": True,
            "items": [
                {
                    "review_id": "rev_0001",
                    "level": "warning",
                    "reason_codes": ["LOW_QUALITY_COUNTERPARTY", "ANCHOR_ONLY_EFFECTIVE_DATE"],
                    "message": "test",
                    "page_refs": [1],
                    "block_ids": ["p1_b001"],
                    "field_names": ["counterparties"],
                }
            ],
            "summary": {"warning_count": 1, "critical_count": 0},
        },
    )
    _write_json(
        doc_a / "document.json",
        {
            "fields": {
                "counterparties": {"value": ["株式会社A"], "quality": {"quality_flags": ["counterparty_partial_accept"]}},
                "effective_date": {"value": "契約締結日から", "quality": {"anchor_only": True}},
                "expiration_date": {"value": "2026-01-01"},
                "governing_law": {"value": "日本法"},
                "jurisdiction": {"value": "東京地方裁判所"},
            }
        },
    )

    # review-only document (degraded mode)
    doc_b = baseline_root / "doc_b"
    _write_json(
        doc_b / "review.json",
        {
            "doc_id": "doc_b",
            "review_required": True,
            "items": [
                {
                    "review_id": "rev_0001",
                    "level": "warning",
                    "reason_codes": ["MISSING_GOVERNING_LAW", "MISSING_JURISDICTION"],
                    "message": "test",
                    "page_refs": [2],
                    "block_ids": ["p2_b001"],
                    "field_names": ["governing_law", "jurisdiction"],
                }
            ],
            "summary": {"warning_count": 1, "critical_count": 0},
        },
    )

    # malformed review should not crash whole run
    doc_c = baseline_root / "doc_c"
    doc_c.mkdir(parents=True, exist_ok=True)
    (doc_c / "review.json").write_text("{broken json", encoding="utf-8")

    exit_code = evaluate_outputs.run(
        [
            "--baseline-root",
            str(baseline_root),
            "--reports-dir",
            str(reports_dir),
            "--manifest",
            str(tmp_path / "missing_manifest.json"),
        ]
    )
    assert exit_code == 0

    summary = json.loads((reports_dir / "eval_summary.json").read_text(encoding="utf-8"))
    assert (reports_dir / "eval_summary.csv").exists()
    assert summary["document_count"] == 3

    rows = {row["pdf_name"]: row for row in summary["documents"]}
    assert rows["doc_a"]["low_quality_counterparty_count"] == 1
    assert rows["doc_a"]["anchor_only_effective_date_count"] == 1
    assert rows["doc_a"]["degraded_mode"] is False

    assert rows["doc_b"]["review_total"] == 1
    assert rows["doc_b"]["missing_governing_law_count"] == 1
    assert rows["doc_b"]["missing_jurisdiction_count"] == 1
    assert rows["doc_b"]["degraded_mode"] is True

    assert len(summary["warnings"]) >= 1


def test_report_baseline_prints_table_from_eval_summary_json(tmp_path, capsys) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "documents": [
            {
                "pdf_name": "doc_a",
                "review_required": True,
                "review_total": 3,
                "field_fill_rate": 0.8,
                "field_quality_score": 0.7,
                "document_pass_fail": "pass",
                "top_issues": ["LOW_QUALITY_COUNTERPARTY:1", "ANCHOR_ONLY_EFFECTIVE_DATE:1"],
            }
        ]
    }
    (reports_dir / "eval_summary.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    exit_code = report_baseline.run(["--summary-json", str(reports_dir / "eval_summary.json")])
    assert exit_code == 0

    captured = capsys.readouterr()
    assert "pdf_name" in captured.out
    assert "doc_a" in captured.out
