from __future__ import annotations

from contract_ingest.domain.enums import BlockType, ExtractMethod
from contract_ingest.domain.models import BBox, ClauseUnit, EvidenceBlock, EvidenceRef
from contract_ingest.normalize.tail_clause_finder import (
    find_tail_effective_date_candidates,
    find_tail_expiration_candidates,
    find_tail_governing_law_candidates,
    find_tail_jurisdiction_candidates,
)


def _make_block(page: int, order: int, text: str, block_type: BlockType = BlockType.TEXT) -> EvidenceBlock:
    y0 = 10.0 + order * 18.0
    return EvidenceBlock(
        page=page,
        block_id=f"p{page}_b{order:03d}",
        block_type=block_type,
        bbox=BBox(x0=20.0, y0=y0, x1=580.0, y1=y0 + 16.0),
        text=text,
        engine="native_text",
        extract_method=ExtractMethod.NATIVE_TEXT,
        confidence=0.95,
        searchable=True,
        reading_order=order,
        source_hash="sha256:test",
        pipeline_version="0.1.0",
    )


def _make_clause(block: EvidenceBlock, clause_no: str, clause_title: str) -> ClauseUnit:
    return ClauseUnit(
        clause_id=f"clause_{clause_no}",
        clause_no=clause_no,
        clause_title=clause_title,
        text=block.text,
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


def test_tail_clause_finder_extracts_governing_law_and_jurisdiction_candidates() -> None:
    blocks = [
        _make_block(1, 1, "業務委託契約書"),
        _make_block(1, 2, "第1条 目的"),
        _make_block(3, 20, "第14条 本契約は日本国法によるものとする。"),
        _make_block(4, 21, "第15条 この契約に関する訴えは東京地方裁判所を第一審の専属的合意管轄裁判所とする。"),
    ]
    clauses = [
        _make_clause(blocks[2], "第14条", "準拠法"),
        _make_clause(blocks[3], "第15条", "管轄"),
    ]

    governing_candidates = find_tail_governing_law_candidates(blocks, clauses=clauses, route="SERVICE")
    jurisdiction_candidates = find_tail_jurisdiction_candidates(blocks, clauses=clauses, route="SERVICE")

    assert any(candidate.value == "日本法" for candidate in governing_candidates)
    assert any(candidate.value == "東京地方裁判所" for candidate in jurisdiction_candidates)


def test_tail_clause_finder_extracts_effective_expiration_and_ignores_generic_fragment() -> None:
    blocks = [
        _make_block(2, 10, "本文"),
        _make_block(3, 20, "有効期間は契約締結日から12か月間とする。"),
        _make_block(3, 21, "本契約の効力は契約締結日から生じる。"),
        _make_block(3, 22, "裁判所"),
    ]

    expiration_candidates = find_tail_expiration_candidates(blocks, route="SERVICE")
    effective_candidates = find_tail_effective_date_candidates(blocks, route="SERVICE")
    jurisdiction_candidates = find_tail_jurisdiction_candidates(blocks, route="SERVICE")

    assert any("契約締結日から12か月間" in candidate.value for candidate in expiration_candidates)
    assert any(candidate.value == "契約締結日から" for candidate in effective_candidates)
    assert all(candidate.value not in {"裁判所", "地方裁判所"} for candidate in jurisdiction_candidates)


def test_tail_clause_finder_recovers_jurisdiction_from_neighbor_spans() -> None:
    blocks = [
        _make_block(4, 20, "第13条 本契約に関する一切の紛争については、"),
        _make_block(4, 21, "東京地方裁判所を第一審の専属的合意"),
        _make_block(4, 22, "管轄裁判所とする。"),
    ]

    jurisdiction_candidates = find_tail_jurisdiction_candidates(blocks, route="NDA")

    assert any(candidate.value == "東京地方裁判所" for candidate in jurisdiction_candidates)
    assert any(candidate.reason in {"tail_clause_jurisdiction_span", "tail_clause_jurisdiction_recovered_span"} for candidate in jurisdiction_candidates)


def test_tail_clause_finder_captures_governing_law_and_placeholder_term_spans() -> None:
    blocks = [
        _make_block(5, 31, "第20条 本契約に関しては日本法を適用する。"),
        _make_block(1, 5, "第3条 委託期間は、令和○○年○○月○○日から令和○○年○○月○○日までとする。"),
        _make_block(1, 6, "第4条 本契約に定める履行期間は、契約締結の日から令和7年6月30日までとする。"),
    ]

    law_candidates = find_tail_governing_law_candidates(blocks, route="NDA")
    expiration_candidates = find_tail_expiration_candidates(blocks, route="SERVICE")
    effective_candidates = find_tail_effective_date_candidates(blocks, route="SERVICE")

    assert any(candidate.value == "日本法" for candidate in law_candidates)
    assert any("令和○○年○○月○○日から令和○○年○○月○○日まで" in candidate.value for candidate in expiration_candidates)
    assert any(candidate.value == "契約締結日から" for candidate in effective_candidates)


def test_tail_clause_finder_supports_governing_law_variants() -> None:
    blocks = [
        _make_block(4, 10, "第18条（準拠法等）本契約の成立、効力、履行および解釈は日本法による。"),
        _make_block(4, 11, "第19条（適用法）本契約に関しては日本法を適用する。"),
    ]

    candidates = find_tail_governing_law_candidates(blocks, route="NDA")

    assert any(candidate.value == "日本法" for candidate in candidates)
    assert all(candidate.value not in {"法", "準拠"} for candidate in candidates)


def test_tail_clause_finder_captures_relative_and_renewable_date_terms() -> None:
    blocks = [
        _make_block(3, 20, "第9条 本契約は契約締結日から1年間効力を有する。"),
        _make_block(3, 21, "第10条 契約期間は1年ごとに自動更新するものとする。"),
    ]

    effective_candidates = find_tail_effective_date_candidates(blocks, route="SERVICE")
    expiration_candidates = find_tail_expiration_candidates(blocks, route="SERVICE")

    assert any("契約締結日から1年間" in candidate.value for candidate in effective_candidates)
    assert any("自動更新" in candidate.value for candidate in expiration_candidates)


def test_tail_clause_finder_supports_composite_heading_for_governing_law() -> None:
    blocks = [
        _make_block(5, 40, "（裁判管轄及び準拠法）"),
        _make_block(5, 41, "本契約に関する紛争には日本法を適用する。"),
    ]

    candidates = find_tail_governing_law_candidates(blocks, route="LICENSE_OR_ITAKU")

    assert any(candidate.value == "日本法" for candidate in candidates)
