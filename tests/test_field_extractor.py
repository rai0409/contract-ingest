from __future__ import annotations

from contract_ingest.domain.enums import BlockType, ExtractMethod, ReasonCode
from contract_ingest.domain.models import BBox, ClauseUnit, EvidenceBlock, EvidenceRef
from contract_ingest.normalize.field_extractor import ContractFieldExtractor


def _make_block(order: int, text: str, block_type: BlockType = BlockType.TEXT) -> EvidenceBlock:
    y0 = 10.0 + (order - 1) * 20.0
    return EvidenceBlock(
        page=1,
        block_id=f"p1_b{order:03d}",
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
