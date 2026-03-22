from __future__ import annotations

from contract_ingest.domain.enums import BlockType, ExtractMethod, SectionType
from contract_ingest.domain.models import BBox, ClauseUnit, ContractFields, EvidenceBlock, EvidenceRef, ExtractedField
from contract_ingest.normalize.chunk_builder import ChunkBuilder


def _make_block(page: int, order: int, text: str) -> EvidenceBlock:
    y0 = 10.0 + order * 20.0
    return EvidenceBlock(
        page=page,
        block_id=f"p{page}_b{order:03d}",
        block_type=BlockType.TEXT,
        bbox=BBox(x0=20.0, y0=y0, x1=580.0, y1=y0 + 16.0),
        text=text,
        engine="native_text",
        extract_method=ExtractMethod.NATIVE_TEXT,
        confidence=0.95,
        searchable=True,
        reading_order=order,
        source_hash="sha256:test",
        pipeline_version="0.1.0",
        section_type=SectionType.PREAMBLE,
    )


def _field(name: str, value: str | bool | list[str] | None) -> ExtractedField:
    return ExtractedField(field_name=name, value=value, confidence=0.9, reason="test", evidence_refs=[], flags=[])


def _fields() -> ContractFields:
    return ContractFields(
        contract_type=_field("contract_type", "業務委託契約書"),
        counterparties=_field("counterparties", ["株式会社A", "株式会社B"]),
        effective_date=_field("effective_date", "2025-01-01"),
        expiration_date=_field("expiration_date", "2026-01-01"),
        auto_renewal=_field("auto_renewal", True),
        termination_notice_period=_field("termination_notice_period", "30日"),
        governing_law=_field("governing_law", "日本法"),
        jurisdiction=_field("jurisdiction", "東京地方裁判所"),
    )


def test_chunk_builder_emits_section_type_metadata_and_avoids_duplicate_clause_prefix() -> None:
    block = _make_block(1, 1, "前文")
    clause = ClauseUnit(
        clause_id="clause_001",
        clause_no="第2条",
        clause_title=None,
        text="第2条 乙は委託業務を履行する。",
        page_start=1,
        page_end=1,
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
        section_type=SectionType.MAIN_CONTRACT,
    )

    chunks = ChunkBuilder().build(doc_id="doc_001", clauses=[clause], evidence_blocks=[block], fields=_fields()).chunks

    assert chunks[0]["metadata"]["section_type"] == "main_contract"
    assert chunks[0]["text"].startswith("第2条 乙は")
    assert "第2条 第2条" not in chunks[0]["text"]
