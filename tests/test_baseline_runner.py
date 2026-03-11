from __future__ import annotations

import json
from pathlib import Path

from contract_ingest.benchmarks import baseline_runner


def _touch_pdf(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"%PDF-1.4\n%test\n")


def test_baseline_runner_skips_missing_pdfs_and_writes_manifest(tmp_path) -> None:
    raw_dir = tmp_path / "raw"
    output_root = tmp_path / "baseline"
    reports_dir = tmp_path / "reports"
    _touch_pdf(raw_dir / "nda_hinagata_2020.pdf")

    def fake_ingest_runner(argv: list[str]) -> int:
        output_dir = Path(argv[argv.index("--output-dir") + 1])
        output_dir.mkdir(parents=True, exist_ok=True)
        return 0

    exit_code = baseline_runner.run(
        [
            "--raw-dir",
            str(raw_dir),
            "--output-root",
            str(output_root),
            "--reports-dir",
            str(reports_dir),
            "--layout-engine",
            "current",
        ],
        ingest_runner=fake_ingest_runner,
    )

    assert exit_code == 0
    manifest_path = reports_dir / "run_manifest.json"
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["summary"]["target_count"] == 6
    assert manifest["summary"]["success_count"] == 1
    assert manifest["summary"]["skipped_count"] == 5
    assert manifest["summary"]["error_count"] == 0


def test_baseline_runner_continues_even_if_one_document_fails(tmp_path) -> None:
    raw_dir = tmp_path / "raw"
    output_root = tmp_path / "baseline"
    reports_dir = tmp_path / "reports"
    _touch_pdf(raw_dir / "nda_hinagata_2020.pdf")
    _touch_pdf(raw_dir / "4_keiyakusyo.pdf")

    def fake_ingest_runner(argv: list[str]) -> int:
        input_pdf = Path(argv[argv.index("--input") + 1]).name
        output_dir = Path(argv[argv.index("--output-dir") + 1])
        output_dir.mkdir(parents=True, exist_ok=True)
        if input_pdf == "4_keiyakusyo.pdf":
            return 1
        return 0

    exit_code = baseline_runner.run(
        [
            "--raw-dir",
            str(raw_dir),
            "--output-root",
            str(output_root),
            "--reports-dir",
            str(reports_dir),
            "--layout-engine",
            "current",
        ],
        ingest_runner=fake_ingest_runner,
    )

    assert exit_code == 0
    manifest = json.loads((reports_dir / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["summary"]["success_count"] == 1
    assert manifest["summary"]["error_count"] == 1
    assert manifest["summary"]["skipped_count"] == 4
    statuses = {item["pdf_name"]: item["status"] for item in manifest["documents"]}
    assert statuses["nda_hinagata_2020.pdf"] == "success"
    assert statuses["4_keiyakusyo.pdf"] == "error"
