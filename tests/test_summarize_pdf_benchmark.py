from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "summarize_pdf_benchmark.py"
    spec = importlib.util.spec_from_file_location("summarize_pdf_benchmark", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load summarize_pdf_benchmark module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_normalize_effective_prefers_semantic_metadata_for_placeholder_or_relative() -> None:
    mod = _load_module()

    placeholder_field = {
        "value": "年 月 日",
        "flags": ["period_clause_fallback", "placeholder_date"],
        "quality": {"semantic_type": "placeholder_term", "quality_flags": ["placeholder_date"]},
    }
    relative_field = {
        "value": "2025-01-01",
        "quality": {"semantic_type": "relative_term", "quality_flags": ["relative_period_only"]},
    }

    assert mod.normalize_effective(placeholder_field) == "placeholder"
    assert mod.normalize_effective(relative_field) == "relative"


def test_normalize_counterparties_distinguishes_placeholder_assisted_both() -> None:
    mod = _load_module()

    partial_both = {
        "value": ["国立研究開発法人新エネルギー・産業技術総合開発機構", "□□□□□"],
        "flags": ["counterparty_placeholder"],
        "quality": {"quality_flags": ["counterparty_partial_accept", "counterparty_placeholder"]},
    }

    got = mod.normalize_counterparties(partial_both)
    assert got == "both_with_placeholder_or_partial"
    assert mod.match_counterparties("both", got) is False


def test_summarize_pdf_benchmark_preserves_simple_cases() -> None:
    mod = _load_module()

    assert mod.normalize_effective({"value": None}) == "absent"
    assert mod.normalize_effective({"value": "契約締結日から"}) == "anchor_only"
    assert mod.normalize_effective({"value": "2025-01-01"}) == "present"

    assert mod.normalize_counterparties({"value": None}) == "absent"
    assert mod.normalize_counterparties({"value": ["株式会社A"]}) == "one_side_only"
    assert mod.normalize_counterparties({"value": ["株式会社A", "株式会社B"]}) == "both"
