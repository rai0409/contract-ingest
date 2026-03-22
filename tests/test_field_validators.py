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


def test_validate_governing_law_rejects_japanese_weak_context_terms() -> None:
    assert validate_governing_law("本契約は規程による。").accepted is False
    assert validate_governing_law("誠実に協議して解決する。").accepted is False
    assert validate_governing_law("別途協議して定める。").accepted is False
    assert validate_governing_law("法令遵守を徹底する。").accepted is False
    assert validate_governing_law("適用法令を遵守する。").accepted is False


def test_validate_governing_law_accepts_clause_level_english_and_rejects_weak_context_mentions() -> None:
    new_york = validate_governing_law("This Agreement shall be governed by the laws of the State of New York.")
    england = validate_governing_law(
        "This Agreement and any dispute arising hereunder shall be construed in accordance with the laws of England and Wales."
    )

    assert new_york.accepted is True
    assert new_york.normalized_value == "State of New York law"
    assert england.accepted is True
    assert england.normalized_value == "England and Wales law"

    assert validate_governing_law("The parties will discuss the applicable law in good faith.").accepted is False
    assert validate_governing_law("This policy is intended to comply with applicable laws.").accepted is False
    assert validate_governing_law("Japanese law may apply in some cases.").accepted is False
    assert validate_governing_law("The court shall consider the law and facts.").accepted is False


def test_validate_effective_date_marks_anchor_only_value() -> None:
    result = validate_effective_date("契約締結日から")

    assert result.accepted is True
    assert result.anchor_only is True
    assert "anchor_only_effective_date" in result.quality_flags
    assert "semantic_type:anchor_only" in result.quality_flags


def test_validate_effective_date_classifies_english_absolute_and_anchor_only_values() -> None:
    absolute = validate_effective_date("April 1, 2025")
    anchor = validate_effective_date("on the execution date")

    assert absolute.accepted is True
    assert absolute.normalized_value == "2025-04-01"
    assert "semantic_type:absolute" in absolute.quality_flags
    assert anchor.accepted is True
    assert anchor.anchor_only is True
    assert "semantic_type:anchor_only" in anchor.quality_flags


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


def test_validate_counterparties_rejects_entity_type_only_values() -> None:
    assert validate_counterparties(["株式会社"]).accepted is False
    assert validate_counterparties(["公益財団法人"]).accepted is False

    proper = validate_counterparties(["株式会社テスト"])
    partial = validate_counterparties(["公益財団法人沖縄県国際交流・人材育成財団", "という。)と、株式会社"])

    assert proper.accepted is True
    assert proper.normalized_value == ["株式会社テスト"]
    assert partial.accepted is True
    assert partial.normalized_value == ["公益財団法人沖縄県国際交流・人材育成財団"]
    assert "counterparty_partial_accept" in partial.quality_flags


def test_validate_effective_date_rejects_zero_length_relative_terms() -> None:
    zero_year = validate_effective_date("本契約締結日から0年間")
    zero_day = validate_effective_date("契約締結日から0日間")

    assert zero_year.accepted is False
    assert zero_year.reason == "zero_length_effective_period"
    assert "low_quality_effective_date" in zero_year.quality_flags
    assert zero_day.accepted is False
    assert zero_day.reason == "zero_length_effective_period"
