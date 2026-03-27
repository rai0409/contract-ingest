from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Any

ROOT = Path("/home/rai/contract-ingest")
INVENTORY = ROOT / "benchmarks/pdf_inventory.csv"
RUN_ROOT = ROOT / "runs/benchmark_all_current"

_EFFECTIVE_ANCHOR_TOKENS = ("契約締結日から", "本契約締結日から", "締結日から")
_INVALID_EFFECTIVE_TOKENS = ("0年間", "0年", "0日間", "O0年間", "O年間")
_PERIOD_WORD_RE = re.compile(r"(?:日|週間|週|か月|ヶ月|ヵ月|月|年間|年)")
_BLANK_DATE_PLACEHOLDER_RE = re.compile(r"^\s*[○◯〇□■◻◼]*\s*年\s*[○◯〇□■◻◼]*\s*月\s*[○◯〇□■◻◼]*\s*日\s*$")
_COUNTERPARTY_PLACEHOLDER_RE = re.compile(r"^[\s□■◻◼○◯〇]+$")


def _as_field_payload(field: object) -> tuple[object, set[str], str | None]:
    if not isinstance(field, dict):
        return field, set(), None

    value = field.get("value")
    flags = {
        str(flag)
        for flag in field.get("flags", [])
        if isinstance(flag, str)
    }
    quality = field.get("quality")
    semantic_type: str | None = None
    if isinstance(quality, dict):
        quality_flags = quality.get("quality_flags", [])
        flags.update(str(flag) for flag in quality_flags if isinstance(flag, str))
        semantic_raw = quality.get("semantic_type")
        if isinstance(semantic_raw, str):
            semantic = semantic_raw.strip()
            semantic_type = semantic if semantic else None
        if quality.get("anchor_only") is True:
            flags.add("anchor_only_effective_date")
    return value, flags, semantic_type


def normalize_effective(field: object) -> str:
    value, flags, semantic_type = _as_field_payload(field)
    if value is None:
        return "absent"

    s = str(value)
    if any(token in s for token in _INVALID_EFFECTIVE_TOKENS) or "ocr_numeric_anomaly" in flags:
        return "invalid_or_ocr_anomaly"
    if semantic_type == "anchor_only" or "anchor_only_effective_date" in flags:
        return "anchor_only"
    if semantic_type in {"placeholder_term"} or "placeholder_date" in flags:
        return "placeholder"
    if semantic_type in {"relative_term", "renewable_term"} or "relative_period_only" in flags:
        return "relative"
    if any(token in s for token in _EFFECTIVE_ANCHOR_TOKENS):
        return "anchor_only"
    if _BLANK_DATE_PLACEHOLDER_RE.match(s) is not None:
        return "placeholder"
    if "○" in s or "◯" in s or "〇" in s:
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


def _is_placeholder_counterparty_name(value: object) -> bool:
    if not isinstance(value, str):
        return False
    compact = value.strip()
    if not compact:
        return False
    return _COUNTERPARTY_PLACEHOLDER_RE.fullmatch(compact) is not None


def normalize_counterparties(field: object) -> str:
    value, flags, _ = _as_field_payload(field)
    if value is None:
        return "absent"
    if not isinstance(value, list):
        return "unclear"
    if len(value) == 0:
        return "absent"
    if len(value) == 1:
        return "one_side_only"
    if len(value) >= 2:
        placeholder_or_partial = (
            "counterparty_partial_accept" in flags
            or "counterparty_placeholder" in flags
            or any(_is_placeholder_counterparty_name(item) for item in value)
        )
        if placeholder_or_partial:
            return "both_with_placeholder_or_partial"
        return "both"
    return "unclear"


def match_counterparties(expected: str, got: str) -> bool:
    allowed = {
        "both": {"both"},
        "both_with_placeholder_or_partial": {"both_with_placeholder_or_partial"},
        "one_side_only": {"one_side_only"},
        "unclear": {"unclear"},
        "absent": {"absent"},
        "one_side_only_or_blank": {"one_side_only", "absent"},
    }
    return got in allowed.get(expected, {expected})


def build_summary_lines(
    *,
    inventory_path: Path = INVENTORY,
    run_root: Path = RUN_ROOT,
) -> list[str]:
    inventory_rows = list(csv.DictReader(inventory_path.open(encoding="utf-8")))
    header = (
        "pdf_file\texpected_governing\tgot_governing\tgoverning_match\t"
        "expected_jurisdiction\tgot_jurisdiction\tjurisdiction_match\t"
        "expected_counterparties\tgot_counterparties\tcounterparties_match\t"
        "expected_effective\tgot_effective\teffective_match\twarnings\terrors"
    )
    lines = [header]

    for row in inventory_rows:
        pdf_file = row["pdf_file"]
        stem = Path(pdf_file).stem
        doc_path = run_root / stem / "document.json"

        if not doc_path.exists():
            lines.append(f"{pdf_file}\tMISSING_RUN\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-")
            continue

        data = json.loads(doc_path.read_text(encoding="utf-8"))
        fields: dict[str, Any] = data.get("fields", {})

        governing = normalize_governing(fields.get("governing_law", {}).get("value"))
        jurisdiction = normalize_jurisdiction(fields.get("jurisdiction", {}).get("value"))
        counterparties = normalize_counterparties(fields.get("counterparties", {}))
        effective = normalize_effective(fields.get("effective_date", {}))

        warnings = len(data.get("warnings", []))
        errors = len(data.get("errors", []))

        lines.append(
            f"{pdf_file}\t"
            f"{row['governing_law_expected']}\t{governing}\t{match_governing(row['governing_law_expected'], governing)}\t"
            f"{row['jurisdiction_expected']}\t{jurisdiction}\t{match_jurisdiction(row['jurisdiction_expected'], jurisdiction)}\t"
            f"{row['counterparties_expected']}\t{counterparties}\t{match_counterparties(row['counterparties_expected'], counterparties)}\t"
            f"{row['effective_date_expected']}\t{effective}\t{match_effective(row['effective_date_expected'], effective)}\t"
            f"{warnings}\t{errors}"
        )
    return lines


def main() -> int:
    for line in build_summary_lines():
        print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
