from __future__ import annotations

from contract_ingest.normalize.field_validators import (
    validate_counterparties,
    validate_effective_date,
    validate_expiration_date,
    validate_governing_law,
    validate_jurisdiction,
)


def test_validate_jurisdiction_rejects_generic_and_fragment_values() -> None:
    assert validate_jurisdiction("裁判所").accepted is False
    assert validate_jurisdiction("地方裁判所").accepted is False
    assert validate_jurisdiction("番の専属的合意管轄裁判所").accepted is False


def test_validate_jurisdiction_accepts_specific_court_name() -> None:
    result = validate_jurisdiction("東京地方裁判所を第一審の専属的合意管轄裁判所とする")

    assert result.accepted is True
    assert result.normalized_value == "東京地方裁判所"


def test_validate_governing_law_accepts_japanese_law_and_rejects_generic_tokens() -> None:
    assert validate_governing_law("日本法").accepted is True
    assert validate_governing_law("法").accepted is False
    assert validate_governing_law("準拠").accepted is False


def test_validate_effective_date_marks_anchor_only_value() -> None:
    result = validate_effective_date("契約締結日から")

    assert result.accepted is True
    assert result.anchor_only is True
    assert "anchor_only_effective_date" in result.quality_flags
    assert "semantic_type:anchor_only" in result.quality_flags


def test_validate_date_semantic_types_cover_absolute_relative_placeholder_and_renewable() -> None:
    assert "semantic_type:absolute" in validate_effective_date("2025-04-01").quality_flags
    assert "semantic_type:relative_term" in validate_effective_date("本契約締結日から1年間").quality_flags
    assert "semantic_type:placeholder_term" in validate_expiration_date("令和○○年○○月○○日まで").quality_flags
    assert "semantic_type:renewable_term" in validate_expiration_date("1年ごとに自動更新するものとする").quality_flags


def test_validate_jurisdiction_marks_relative_expression_as_low_quality() -> None:
    result = validate_jurisdiction("甲の所在地を管轄する裁判所")

    assert result.accepted is False
    assert result.reason == "relative_jurisdiction_expression"
    assert "relative_jurisdiction_expression" in result.quality_flags


def test_validate_counterparties_rejects_fragment_like_values() -> None:
    assert validate_counterparties(["という。) と"]).accepted is False

    partial = validate_counterparties(["国立研究開発法人産業技術総合研究所", "という。)と、○○株式会社"])
    assert partial.accepted is True
    assert partial.normalized_value == ["国立研究開発法人産業技術総合研究所", "○○株式会社"]
    assert "counterparty_partial_accept" in partial.quality_flags
