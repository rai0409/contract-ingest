from __future__ import annotations

import csv
import json
import re
from pathlib import Path

ROOT = Path("/home/rai/contract-ingest")
INVENTORY = ROOT / "benchmarks/pdf_inventory.csv"
RUN_ROOT = ROOT / "runs/benchmark_all_current"

_EFFECTIVE_ANCHOR_TOKENS = ("契約締結日から", "本契約締結日から", "締結日から")
_INVALID_EFFECTIVE_TOKENS = ("0年間", "0年", "0日間", "O0年間", "O年間")
_PERIOD_WORD_RE = re.compile(r"(?:日|週間|週|か月|ヶ月|ヵ月|月|年間|年)")


def normalize_effective(value: object) -> str:
    if value is None:
        return "absent"
    s = str(value)
    if any(token in s for token in _INVALID_EFFECTIVE_TOKENS):
        return "invalid_or_ocr_anomaly"
    if any(token in s for token in _EFFECTIVE_ANCHOR_TOKENS):
        return "anchor_only"
    if "○" in s or "令和" in s:
        return "placeholder"
    if "から" in s and _PERIOD_WORD_RE.search(s):
        return "relative"
    return "present"


def match_effective(expected: str, got: str) -> bool:
    allowed = {
        "absent": {"absent"},
        "anchor_only": {"anchor_only"},
        "relative": {"relative"},
        "placeholder": {"placeholder"},
        "present": {"present"},
        "invalid_or_ocr_anomaly": {"invalid_or_ocr_anomaly"},
        "placeholder_or_relative": {"placeholder", "relative"},
        "relative_or_placeholder": {"relative", "placeholder"},
    }
    return got in allowed.get(expected, {expected})


def normalize_governing(value: object) -> str:
    return "present" if value else "absent"


def match_governing(expected: str, got: str) -> bool:
    if expected == "unknown":
        return True
    return expected == got


def normalize_jurisdiction(value: object) -> str:
    if not value:
        return "absent"
    if "所在地" in str(value):
        return "relative"
    return "explicit"


def match_jurisdiction(expected: str, got: str) -> bool:
    allowed = {
        "explicit": {"explicit"},
        "relative": {"relative"},
        "absent": {"absent"},
        "relative_or_absent": {"relative", "absent"},
        "unknown_or_fragment_risk": {"explicit", "relative", "absent"},
    }
    return got in allowed.get(expected, {expected})


def normalize_counterparties(value: object) -> str:
    if value is None:
        return "absent"
    if isinstance(value, list):
        if len(value) >= 2:
            return "both"
        if len(value) == 1:
            return "one_side_only"
        return "unclear"
    return "unclear"


def match_counterparties(expected: str, got: str) -> bool:
    allowed = {
        "both": {"both"},
        "one_side_only": {"one_side_only"},
        "unclear": {"unclear"},
        "absent": {"absent"},
        "one_side_only_or_blank": {"one_side_only", "absent"},
    }
    return got in allowed.get(expected, {expected})


inventory_rows = list(csv.DictReader(INVENTORY.open(encoding="utf-8")))

print(
    "pdf_file\texpected_governing\tgot_governing\tgoverning_match\t"
    "expected_jurisdiction\tgot_jurisdiction\tjurisdiction_match\t"
    "expected_counterparties\tgot_counterparties\tcounterparties_match\t"
    "expected_effective\tgot_effective\teffective_match\twarnings\terrors"
)

for row in inventory_rows:
    pdf_file = row["pdf_file"]
    stem = Path(pdf_file).stem
    doc_path = RUN_ROOT / stem / "document.json"

    if not doc_path.exists():
        print(f"{pdf_file}\tMISSING_RUN\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-")
        continue

    data = json.loads(doc_path.read_text(encoding="utf-8"))
    fields = data.get("fields", {})

    governing = normalize_governing(fields.get("governing_law", {}).get("value"))
    jurisdiction = normalize_jurisdiction(fields.get("jurisdiction", {}).get("value"))
    counterparties = normalize_counterparties(fields.get("counterparties", {}).get("value"))
    effective = normalize_effective(fields.get("effective_date", {}).get("value"))

    warnings = len(data.get("warnings", []))
    errors = len(data.get("errors", []))

    print(
        f"{pdf_file}\t"
        f"{row['governing_law_expected']}\t{governing}\t{match_governing(row['governing_law_expected'], governing)}\t"
        f"{row['jurisdiction_expected']}\t{jurisdiction}\t{match_jurisdiction(row['jurisdiction_expected'], jurisdiction)}\t"
        f"{row['counterparties_expected']}\t{counterparties}\t{match_counterparties(row['counterparties_expected'], counterparties)}\t"
        f"{row['effective_date_expected']}\t{effective}\t{match_effective(row['effective_date_expected'], effective)}\t"
        f"{warnings}\t{errors}"
    )
