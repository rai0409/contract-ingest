from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Callable

from contract_ingest.cli import ingest_contract

_BASELINE_PDFS = [
    "nda_hinagata_2020.pdf",
    "2-4himitsuhozikeiyakusyo.pdf",
    "gyoumu-k230101.pdf",
    "kagawa_itaku.pdf",
    "usp_itaku.pdf",
    "4_keiyakusyo.pdf",
]
_OPTIONAL_PDFS = ["210402_himitsuhoji.pdf"]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ingestion baseline batch")
    parser.add_argument("--layout-engine", choices=["current", "ppstructure", "compare"], default="current")
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"), help="directory containing baseline PDFs")
    parser.add_argument("--output-root", type=Path, default=Path("out/baseline"), help="baseline output root")
    parser.add_argument("--reports-dir", type=Path, default=Path("out/reports"), help="reports output directory")
    parser.add_argument("--include-optional", action="store_true", help="include optional baseline PDFs")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def run(
    argv: list[str] | None = None,
    *,
    ingest_runner: Callable[[list[str]], int] | None = None,
) -> int:
    args = parse_args(argv)
    ingest_fn = ingest_runner or ingest_contract.run

    target_names = list(_BASELINE_PDFS)
    if args.include_optional:
        target_names.extend(_OPTIONAL_PDFS)

    documents: list[dict[str, object]] = []
    warnings: list[str] = []
    success_count = 0
    skipped_count = 0
    error_count = 0

    for pdf_name in target_names:
        pdf_path = args.raw_dir / pdf_name
        output_dir = args.output_root / Path(pdf_name).stem
        entry: dict[str, object] = {
            "pdf_name": pdf_name,
            "pdf_path": str(pdf_path),
            "output_dir": str(output_dir),
            "status": "pending",
            "exit_code": None,
            "warning": None,
            "error": None,
        }

        if not pdf_path.exists():
            warning = f"missing baseline PDF: {pdf_path}"
            entry["status"] = "skipped_missing"
            entry["warning"] = warning
            skipped_count += 1
            warnings.append(warning)
            documents.append(entry)
            continue

        try:
            exit_code = ingest_fn(
                [
                    "--input",
                    str(pdf_path),
                    "--output-dir",
                    str(output_dir),
                    "--layout-engine",
                    str(args.layout_engine),
                    "--log-level",
                    str(args.log_level),
                ]
            )
            entry["exit_code"] = int(exit_code)
            if int(exit_code) == 0:
                entry["status"] = "success"
                success_count += 1
            else:
                entry["status"] = "error"
                entry["error"] = f"ingest exited with non-zero code: {exit_code}"
                error_count += 1
        except Exception as exc:
            entry["status"] = "error"
            entry["error"] = f"ingest raised exception: {exc}"
            error_count += 1
        documents.append(entry)

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "layout_engine": args.layout_engine,
        "raw_dir": str(args.raw_dir),
        "output_root": str(args.output_root),
        "documents": documents,
        "summary": {
            "target_count": len(target_names),
            "success_count": success_count,
            "skipped_count": skipped_count,
            "error_count": error_count,
            "had_errors": error_count > 0,
        },
        "warnings": warnings,
    }

    args.reports_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = args.reports_dir / "run_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"manifest_path": str(manifest_path), "summary": manifest["summary"]}, ensure_ascii=False))

    # Keep batch non-fatal when at least one document succeeded or was processed.
    if success_count > 0 or skipped_count > 0:
        return 0
    return 1


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
