#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/rai/contract-ingest"
INVENTORY="$ROOT/benchmarks/pdf_inventory.csv"
PDF_DIR="$ROOT/fixtures/pdfs"
OUT_ROOT="$ROOT/runs/benchmark_all_current"

mkdir -p "$OUT_ROOT"

python - <<'PY'
import csv
from pathlib import Path
import subprocess

root = Path("/home/rai/contract-ingest")
inventory = root / "benchmarks/pdf_inventory.csv"
pdf_dir = root / "fixtures/pdfs"
out_root = root / "runs/benchmark_all_current"

rows = list(csv.DictReader(inventory.open(encoding="utf-8")))

for row in rows:
    pdf_file = row["pdf_file"]
    pdf_path = pdf_dir / pdf_file
    if not pdf_path.exists():
        print(f"MISSING: {pdf_path}")
        continue

    doc_stem = pdf_path.stem
    out_dir = out_root / doc_stem

    print(f"RUN: {pdf_file}")
    print(f"  input: {pdf_path}")
    print(f"  out  : {out_dir}")

    out_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        str(root / ".venv/bin/python"),
        "-m",
        "contract_ingest.cli.ingest_contract",
        "--input",
        str(pdf_path),
        "--output-dir",
        str(out_dir),
        "--doc-id",
        doc_stem,
        "--log-level",
        "WARNING",
        "--layout-engine",
        "current",
    ]
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"FAILED: {pdf_file} (exit={result.returncode})")
PY
