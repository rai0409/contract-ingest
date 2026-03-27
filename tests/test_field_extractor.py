from __future__ import annotations

from contract_ingest.domain.enums import BlockType, ExtractMethod, ReasonCode
from contract_ingest.domain.models import BBox, ClauseUnit, EvidenceBlock, EvidenceRef
from contract_ingest.normalize.field_extractor import ContractFieldExtractor


def _make_block(
    order: int,
    text: str,
    block_type: BlockType = BlockType.TEXT,
    *,
    page: int = 1,
) -> EvidenceBlock:
    y0 = 10.0 + (order - 1) * 20.0
    return EvidenceBlock(
        page=page,
        block_id=f"p{page}_b{order:03d}",
        block_type=block_type,
        bbox=BBox(x0=10.0, y0=y0, x1=580.0, y1=y0 + 16.0),
        text=text,
        engine="native_text",
        extract_method=ExtractMethod.NATIVE_TEXT,
        confidence=None,
        searchable=True,
        reading_order=order,
        source_hash="sha256:test",
        pipeline_version="0.1.0",
    )


def _make_clause(block: EvidenceBlock, clause_no: str, clause_title: str | None, text: str) -> ClauseUnit:
    return ClauseUnit(
        clause_id=f"clause_{clause_no}",
        clause_no=clause_no,
        clause_title=clause_title,
        text=text,
        page_start=block.page,
        page_end=block.page,
        block_ids=[block.block_id],
        evidence_refs=[
            EvidenceRef(
                page=block.page,
                block_id=block.block_id,
                bbox=block.bbox,
                confidence=block.confidence,
                engine=block.engine,
            )
        ],
        flags=[],
    )


def test_contract_type_infers_from_purpose_clause() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(1, "契約書"),
        _make_block(2, "第1条（目的）甲は乙に対して業務を委託し、乙はこれを受託する。"),
    ]

    result = extractor.extract(blocks)

    assert result.fields.contract_type.value == "業務委託契約書"


def test_counterparties_extracts_kou_otsu_with_legal_entity_normalization() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(1, "株式会社サンプル（以下「甲」という。）"),
        _make_block(2, "乙：㈱テスト"),
    ]

    result = extractor.extract(blocks)

    assert result.fields.counterparties.value == ["株式会社サンプル", "株式会社テスト"]


def test_counterparties_alias_canonicalization_does_not_fall_back_to_absent() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(1, "国立大学法人九州大学（以下「甲」という。）"),
        _make_block(2, "九州大学（以下「乙」という。）"),
    ]

    result = extractor.extract(blocks)
    reason_codes = {
        issue.reason_code.value if hasattr(issue.reason_code, "value") else str(issue.reason_code)
        for issue in result.issues
    }

    assert result.fields.counterparties.value == ["国立大学法人九州大学"]
    assert "counterparty_alias_merged" in result.fields.counterparties.flags
    assert "counterparty_one_side_only" in result.fields.counterparties.flags
    assert ReasonCode.LOW_QUALITY_COUNTERPARTY.value not in reason_codes


def test_effective_date_anchor_is_not_reported_as_pure_missing() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(1, "第3条 委託業務の委託期間は、契約締結日から令和5年6月30日までとする。"),
    ]

    result = extractor.extract(blocks)

    assert result.fields.expiration_date.value == "2023-06-30"
    assert result.fields.effective_date.value == "契約締結日から"
    assert result.fields.effective_date.reason == "matched_effective_date_anchor_rule"
    reason_codes = {
        issue.reason_code.value if hasattr(issue.reason_code, "value") else str(issue.reason_code)
        for issue in result.issues
    }
    assert ReasonCode.MISSING_EFFECTIVE_DATE.value not in reason_codes
    assert ReasonCode.ANCHOR_ONLY_EFFECTIVE_DATE.value in reason_codes


def test_governing_law_and_jurisdiction_from_clause_scoped_text() -> None:
    extractor = ContractFieldExtractor()
    law_block = _make_block(1, "第10条（準拠法）本契約の準拠法は【日本法】とする。")
    j_block = _make_block(2, "第11条（管轄）東京地方裁判所 を 第一審 の 専属的合意管轄裁判所 とする。")
    clauses = [
        _make_clause(law_block, "第10条", "準拠法", law_block.text),
        _make_clause(j_block, "第11条", "管轄", j_block.text),
    ]

    result = extractor.extract([law_block, j_block], clauses=clauses)

    assert result.fields.governing_law.value == "日本法"
    assert result.fields.jurisdiction.value == "東京地方裁判所"


def test_date_normalization_supports_slash_dot_and_era() -> None:
    assert ContractFieldExtractor._normalize_date_token("2025/01/01") == "2025-01-01"
    assert ContractFieldExtractor._normalize_date_token("2025.01.01") == "2025-01-01"
    assert ContractFieldExtractor._normalize_date_token("令和5年6月30日") == "2023-06-30"


def test_governing_law_and_jurisdiction_clause_priority_with_spaced_titles() -> None:
    extractor = ContractFieldExtractor()
    law_block = _make_block(1, "第20条（準 拠 法）本契約の準拠法は日本法とする。")
    j_block = _make_block(2, "第21条（合意 管轄）東京地方裁判所を第一審の専属的合意管轄裁判所とする。")
    clauses = [
        _make_clause(law_block, "第20条", "準 拠 法", law_block.text),
        _make_clause(j_block, "第21条", "合意 管轄", j_block.text),
    ]

    result = extractor.extract([law_block, j_block], clauses=clauses)

    assert result.fields.governing_law.value == "日本法"
    assert result.fields.jurisdiction.value == "東京地方裁判所"


def test_governing_law_extracts_additional_japanese_clause_level_variants() -> None:
    extractor = ContractFieldExtractor()
    applied_law = _make_block(1, "第18条（適用法）本契約に適用される法は日本法とする。")
    all_matters = _make_block(2, "第19条（準拠法）本契約に関する一切の事項は日本法に従う。")

    applied_result = extractor.extract([applied_law])
    all_matters_result = extractor.extract([all_matters])

    assert applied_result.fields.governing_law.value == "日本法"
    assert all_matters_result.fields.governing_law.value == "日本法"


def test_governing_law_and_jurisdiction_ambiguous_case_stays_unresolved() -> None:
    extractor = ContractFieldExtractor()
    law_block = _make_block(1, "第12条（準拠法）本契約の準拠法は当事者間の協議により定める。")
    j_block = _make_block(2, "第13条（管轄）紛争の管轄については別途協議する。")
    clauses = [
        _make_clause(law_block, "第12条", "準拠法", law_block.text),
        _make_clause(j_block, "第13条", "管轄", j_block.text),
    ]

    result = extractor.extract([law_block, j_block], clauses=clauses)
    reason_codes = {
        issue.reason_code.value if hasattr(issue.reason_code, "value") else str(issue.reason_code)
        for issue in result.issues
    }

    assert result.fields.governing_law.value is None
    assert result.fields.jurisdiction.value is None
    assert ReasonCode.MISSING_GOVERNING_LAW.value in reason_codes
    assert ReasonCode.MISSING_JURISDICTION.value in reason_codes


def test_jurisdiction_low_quality_candidate_is_rejected_by_validator_gate() -> None:
    extractor = ContractFieldExtractor()
    bad_j_block = _make_block(1, "第11条（管轄）番の専属的合意管轄裁判所とする。")
    clauses = [
        _make_clause(bad_j_block, "第11条", "管轄", bad_j_block.text),
    ]

    result = extractor.extract([bad_j_block], clauses=clauses)
    reason_codes = {
        issue.reason_code.value if hasattr(issue.reason_code, "value") else str(issue.reason_code)
        for issue in result.issues
    }

    assert result.fields.jurisdiction.value is None
    assert ReasonCode.LOW_QUALITY_JURISDICTION.value in reason_codes
    assert ReasonCode.MISSING_JURISDICTION.value in reason_codes


def test_counterparties_fragment_like_candidate_is_not_adopted_as_is() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(1, "甲：という。) と"),
        _make_block(2, "乙：○○株式会社"),
    ]

    result = extractor.extract(blocks)
    reason_codes = {
        issue.reason_code.value if hasattr(issue.reason_code, "value") else str(issue.reason_code)
        for issue in result.issues
    }

    assert isinstance(result.fields.counterparties.value, list)
    assert "という。) と" not in result.fields.counterparties.value
    assert "○○株式会社" in result.fields.counterparties.value
    assert ReasonCode.PARTIAL_COUNTERPARTY.value in reason_codes


def test_field_extractor_rejects_counterparty_entity_type_only_role_hit() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(1, "公益財団法人沖縄県国際交流・人材育成財団（以下「甲」という。）"),
        _make_block(2, "乙：株式会社"),
    ]

    result = extractor.extract(blocks)
    reason_codes = {
        issue.reason_code.value if hasattr(issue.reason_code, "value") else str(issue.reason_code)
        for issue in result.issues
    }

    assert result.fields.counterparties.value == ["公益財団法人沖縄県国際交流・人材育成財団"]
    assert ReasonCode.PARTIAL_COUNTERPARTY.value in reason_codes


def test_field_extractor_tail_finder_recovers_governing_law_and_jurisdiction_when_base_rule_misses() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(1, "業務委託契約書", page=1),
        _make_block(2, "第1条 目的", page=1),
        _make_block(70, "第14条 本契約は日本国法によるものとする。", page=4),
        _make_block(71, "第15条 この契約に関する訴えは九州大学所在地を管轄区域とする福岡地方裁判所とする。", page=4),
    ]

    result = extractor.extract(blocks)
    reason_codes = {
        issue.reason_code.value if hasattr(issue.reason_code, "value") else str(issue.reason_code)
        for issue in result.issues
    }

    assert result.fields.governing_law.value == "日本法"
    assert result.fields.jurisdiction.value == "福岡地方裁判所"
    assert (
        "finder:tail_clause_governing_law" in result.fields.governing_law.reason
        or result.fields.governing_law.reason == "matched_governing_law_clause_rule"
    )
    assert "finder:" in result.fields.jurisdiction.reason
    assert ReasonCode.MISSING_GOVERNING_LAW.value not in reason_codes
    assert ReasonCode.MISSING_JURISDICTION.value not in reason_codes
    assert ReasonCode.MISSING_GOVERNING_LAW.value not in result.fields.governing_law.flags
    assert ReasonCode.MISSING_JURISDICTION.value not in result.fields.jurisdiction.flags


def test_field_extractor_counterparty_finder_recovers_from_preamble_and_signature_paths() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(
            1,
            "公立大学法人滋賀県立大学(以下「発注者」という。)と、という。)と、○○株式会社(以下「受託者」という。)は契約を締結する。",
            page=1,
        ),
        _make_block(
            80,
            "委託者(甲) 住 所 滋賀県彦根市八坂町2500 氏 名 公立大学法人滋賀県立大学",
            block_type=BlockType.SIGNATURE_AREA,
            page=6,
        ),
        _make_block(
            81,
            "受託者(乙) 住 所 氏 名 ○○株式会社",
            block_type=BlockType.SIGNATURE_AREA,
            page=6,
        ),
    ]

    result = extractor.extract(blocks)

    assert isinstance(result.fields.counterparties.value, list)
    assert "公立大学法人滋賀県立大学" in result.fields.counterparties.value
    assert "○○株式会社" in result.fields.counterparties.value
    assert all("という。)" not in value for value in result.fields.counterparties.value)
    assert "finder_counterparty_supplemented" in result.fields.counterparties.flags


def test_field_extractor_tail_finder_recovers_effective_anchor_from_contract_teiketsubi_no() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(
            1,
            "第4条 本契約に定める履行期間は、契約締結の日から令和7年6月30日までとする。",
            page=1,
        ),
    ]

    result = extractor.extract(blocks)
    reason_codes = {
        issue.reason_code.value if hasattr(issue.reason_code, "value") else str(issue.reason_code)
        for issue in result.issues
    }

    assert result.fields.effective_date.value == "契約締結日から"
    assert result.fields.expiration_date.value == "2025-06-30"
    assert "semantic_type:anchor_only" in result.fields.effective_date.flags
    assert "semantic_type:absolute" in result.fields.expiration_date.flags
    assert ReasonCode.ANCHOR_ONLY_EFFECTIVE_DATE.value in reason_codes


def test_field_extractor_tail_finder_recovers_placeholder_expiration_range() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(1, "第3条 委託期間は、令和○○年○○月○○日から令和○○年○○月○○日までとする。", page=1),
    ]

    result = extractor.extract(blocks)

    assert result.fields.expiration_date.value is not None
    assert "令和○○年○○月○○日" in str(result.fields.expiration_date.value)
    assert "finder:" in result.fields.expiration_date.reason
    assert "semantic_type:placeholder_term" in result.fields.expiration_date.flags


def test_field_extractor_falls_back_to_placeholder_effective_date_from_period_clause() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(1, "第9条 本契約の有効期間は、令和○○年○○月○○日から令和○○年○○月○○日までとする。", page=2),
    ]

    result = extractor.extract(blocks)

    assert result.fields.effective_date.value == "令和○○年○○月○○日"
    assert result.fields.effective_date.reason == "matched_effective_date_period_clause_fallback"
    assert "semantic_type:placeholder_term" in result.fields.effective_date.flags


def test_field_extractor_falls_back_to_explicit_effective_date_from_period_clause() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(1, "第3条 契約期間は、2025年4月1日から2026年3月31日までとする。", page=1),
    ]

    result = extractor.extract(blocks)

    assert result.fields.effective_date.value == "2025-04-01"
    assert result.fields.effective_date.reason == "matched_effective_date_period_clause_fallback"
    assert "semantic_type:absolute" in result.fields.effective_date.flags


def test_field_extractor_tail_finder_recovers_jurisdiction_from_split_neighbor_blocks() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(10, "第13条 本契約に関する一切の紛争については、", page=4),
        _make_block(11, "東京地方裁判所を第一審の専属的合意", page=4),
        _make_block(12, "管轄裁判所とする。", page=4),
    ]

    result = extractor.extract(blocks)
    reason_codes = {
        issue.reason_code.value if hasattr(issue.reason_code, "value") else str(issue.reason_code)
        for issue in result.issues
    }

    assert result.fields.jurisdiction.value == "東京地方裁判所"
    assert "finder:" in result.fields.jurisdiction.reason
    assert ReasonCode.MISSING_JURISDICTION.value not in reason_codes
    assert ReasonCode.MISSING_JURISDICTION.value not in result.fields.jurisdiction.flags


def test_field_extractor_marks_relative_jurisdiction_expression_without_forcing_normalized_value() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(1, "第35条 本契約について訴訟の必要が生じたときは、甲の所在地を管轄する裁判所を第一審の専属的合意管轄裁判所とする。"),
    ]

    result = extractor.extract(blocks)
    reason_codes = {
        issue.reason_code.value if hasattr(issue.reason_code, "value") else str(issue.reason_code)
        for issue in result.issues
    }
    low_quality_issue = next(
        issue
        for issue in result.issues
        if (issue.reason_code.value if hasattr(issue.reason_code, "value") else str(issue.reason_code))
        == ReasonCode.LOW_QUALITY_JURISDICTION.value
    )

    assert result.fields.jurisdiction.value is None
    assert "relative_jurisdiction_expression" in result.fields.jurisdiction.flags
    assert any(flag.startswith("relative_jurisdiction_expression:") for flag in result.fields.jurisdiction.flags)
    assert ReasonCode.LOW_QUALITY_JURISDICTION.value in reason_codes
    assert ReasonCode.MISSING_JURISDICTION.value in reason_codes
    assert low_quality_issue.details.get("why_rejected") == "relative_jurisdiction_expression"
    assert "所在地を管轄する裁判所" in str(low_quality_issue.details.get("candidate_value"))


def test_field_extractor_effective_relative_term_is_kept_with_semantic_type() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(1, "第2条 本契約は契約締結日から1年間効力を有する。"),
    ]

    result = extractor.extract(blocks)

    assert result.fields.effective_date.value is not None
    assert "契約締結日から1年間" in str(result.fields.effective_date.value)
    assert "semantic_type:relative_term" in result.fields.effective_date.flags


def test_field_extractor_rejects_zero_length_effective_relative_term() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(1, "第2条 本契約は本契約締結日から0年間効力を有する。"),
    ]

    result = extractor.extract(blocks)
    reason_codes = {
        issue.reason_code.value if hasattr(issue.reason_code, "value") else str(issue.reason_code)
        for issue in result.issues
    }

    assert result.fields.effective_date.value is None
    assert ReasonCode.MISSING_EFFECTIVE_DATE.value in reason_codes


def test_field_extractor_recovers_effective_placeholder_from_signature_execution_context() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(1, "上記契約の成立を証するため、発注者と受託者は次に記名し、印を押すものとする。", page=5),
        _make_block(2, "令和○○年○○月○○日", page=5),
    ]

    result = extractor.extract(blocks)
    reason_codes = {
        issue.reason_code.value if hasattr(issue.reason_code, "value") else str(issue.reason_code)
        for issue in result.issues
    }

    assert result.fields.effective_date.value == "令和○○年○○月○○日"
    assert "semantic_type:placeholder_term" in result.fields.effective_date.flags
    assert ReasonCode.MISSING_EFFECTIVE_DATE.value not in reason_codes


def test_field_extractor_handles_composite_governing_law_heading_pattern() -> None:
    extractor = ContractFieldExtractor()
    block = _make_block(
        1,
        "第20条（裁判管轄及び準拠法）本契約に関する紛争には日本法を適用する。",
        page=4,
    )

    result = extractor.extract([block])

    assert result.fields.governing_law.value == "日本法"


def test_field_extractor_extracts_english_governing_law_and_effective_date() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(
            1,
            "This Agreement shall be governed by and construed in accordance with the laws of Japan.",
        ),
        _make_block(
            2,
            "This Agreement is made and entered into as of April 1, 2025.",
        ),
    ]

    result = extractor.extract(blocks)

    assert result.fields.governing_law.value == "日本法"
    assert result.fields.effective_date.value == "2025-04-01"


def test_field_extractor_extracts_english_governing_law_clause_variants_beyond_japan() -> None:
    extractor = ContractFieldExtractor()
    new_york_blocks = [
        _make_block(1, "This Agreement shall be governed by the laws of the State of New York."),
    ]
    england_blocks = [
        _make_block(
            1,
            "This Agreement and any dispute arising hereunder shall be construed in accordance with the laws of England and Wales.",
        ),
    ]

    new_york = extractor.extract(new_york_blocks)
    england = extractor.extract(england_blocks)

    assert new_york.fields.governing_law.value == "State of New York law"
    assert england.fields.governing_law.value == "England and Wales law"


def test_field_extractor_rejects_weak_context_english_law_mentions() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(1, "The parties will discuss the applicable law in good faith."),
        _make_block(2, "This policy is intended to comply with applicable laws."),
        _make_block(3, "Japanese law may apply in some cases."),
        _make_block(4, "The court shall consider the law and facts."),
    ]

    result = extractor.extract(blocks)

    assert result.fields.governing_law.value is None


def test_field_extractor_extracts_additional_english_effective_date_absolute_pattern() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(1, "This Agreement shall be effective on and after 2025-04-01."),
    ]

    result = extractor.extract(blocks)

    assert result.fields.effective_date.value == "2025-04-01"
    assert "semantic_type:absolute" in result.fields.effective_date.flags


def test_field_extractor_keeps_relative_english_effective_reference_without_over_normalization() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(1, "This Agreement shall be effective as of the date first written above."),
    ]

    result = extractor.extract(blocks)

    assert result.fields.effective_date.value == "as of the date first written above"
    assert "semantic_type:relative_term" in result.fields.effective_date.flags


def test_field_extractor_keeps_last_signature_english_effective_reference_without_over_normalization() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(1, "This Agreement shall become effective on the date of last signature."),
    ]

    result = extractor.extract(blocks)

    assert result.fields.effective_date.value == "on the date of last signature"
    assert "semantic_type:relative_term" in result.fields.effective_date.flags


def test_field_extractor_keeps_nda_like_governing_law_and_jurisdiction_quality() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(1, "秘密保持契約書"),
        _make_block(2, "第10条（準拠法）本契約は日本法に準拠する。"),
        _make_block(3, "第11条（合意管轄）東京地方裁判所を第一審の専属的合意管轄裁判所とする。"),
    ]

    result = extractor.extract(blocks)

    assert result.fields.governing_law.value == "日本法"
    assert result.fields.jurisdiction.value == "東京地方裁判所"


def test_field_extractor_marks_governing_law_missing_as_source_not_explicit_when_law_clause_absent() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(1, "第11条（管轄）福岡地方裁判所を第一審の専属的合意管轄裁判所とする。"),
    ]

    result = extractor.extract(blocks)
    missing_issue = next(
        issue
        for issue in result.issues
        if (issue.reason_code.value if hasattr(issue.reason_code, "value") else str(issue.reason_code))
        == ReasonCode.MISSING_GOVERNING_LAW.value
    )
    reason_codes = {
        issue.reason_code.value if hasattr(issue.reason_code, "value") else str(issue.reason_code)
        for issue in result.issues
    }

    assert result.fields.governing_law.value is None
    assert "source_not_explicit_governing_law" in result.fields.governing_law.flags
    assert missing_issue.details.get("why_rejected") == "source_not_explicit_governing_law"
    assert ReasonCode.LOW_QUALITY_GOVERNING_LAW.value not in reason_codes


def test_field_extractor_does_not_mark_source_absent_when_governing_law_clause_text_exists() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(1, "第12条（準拠法）本契約の準拠法は当事者間の協議により定める。"),
        _make_block(2, "第13条（管轄）福岡地方裁判所を専属的合意管轄裁判所とする。"),
    ]

    result = extractor.extract(blocks)
    missing_issue = next(
        issue
        for issue in result.issues
        if (issue.reason_code.value if hasattr(issue.reason_code, "value") else str(issue.reason_code))
        == ReasonCode.MISSING_GOVERNING_LAW.value
    )

    assert result.fields.governing_law.value is None
    assert "source_not_explicit_governing_law" not in result.fields.governing_law.flags
    assert missing_issue.details.get("why_rejected") != "source_not_explicit_governing_law"


def test_field_extractor_suppresses_governing_law_false_positive_from_tekiyou_hourei_houjin_context() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(1, "第4条 乙は適用法令を遵守するものとする。"),
        _make_block(2, "これは、国立研究開発法人新エネルギー・産業技術総合開発機構(NEDO)の様式である。"),
        _make_block(3, "第47条 本契約に関する訴えは、横浜地方裁判所を第一審の専属的合意管轄裁判所とする。"),
    ]

    result = extractor.extract(blocks)

    assert result.fields.governing_law.value is None
    assert "source_not_explicit_governing_law" in result.fields.governing_law.flags
    assert result.fields.jurisdiction.value == "横浜地方裁判所"


def test_field_extractor_recovers_counterparties_from_late_role_lines_in_long_template() -> None:
    extractor = ContractFieldExtractor()
    filler_blocks = [_make_block(i, f"前文ダミー{i}") for i in range(1, 101)]
    role_blocks = [
        _make_block(101, "国立研究開発法人新エネルギー・産業技術総合開発機構（以下「甲」という。）"),
        _make_block(102, "□□□□□（以下「乙」という。）"),
    ]

    result = extractor.extract([*filler_blocks, *role_blocks])

    assert isinstance(result.fields.counterparties.value, list)
    assert "国立研究開発法人新エネルギー・産業技術総合開発機構" in result.fields.counterparties.value
    assert "□□□□□" in result.fields.counterparties.value


def test_field_extractor_recovers_blank_placeholder_effective_date_from_signature_context() -> None:
    extractor = ContractFieldExtractor()
    blocks = [
        _make_block(1, "第3条 委託期間 年 月 日から 年 月 日まで"),
        _make_block(2, "本契約の締結を証するため、契約書○通を作成し、双方記名押印の上、各1通を保有する。"),
        _make_block(3, "年 月 日", block_type=BlockType.TABLE),
    ]

    result = extractor.extract(blocks)

    assert result.fields.effective_date.value == "年 月 日"
    assert "semantic_type:placeholder_term" in result.fields.effective_date.flags
