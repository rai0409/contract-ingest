from __future__ import annotations

import json

from contract_ingest.domain.enums import DocumentKind, ErrorSeverity, ReasonCode
from contract_ingest.domain.models import (
    BBox,
    ContractFields,
    EvidenceRef,
    ExtractedField,
    MergedPage,
    ProcessingIssue,
)
from contract_ingest.export.write_document_json import DocumentJsonWriter
from contract_ingest.export.write_review_json import ReviewJsonWriter
from contract_ingest.review.review_queue import ReviewQueueBuilder
from contract_ingest.review.scorer import ReviewScorer


def _field(name: str, value: str | bool | list[str] | None, *, flags: list[str] | None = None) -> ExtractedField:
    return ExtractedField(
        field_name=name,
        value=value,
        confidence=0.9 if value is not None else None,
        reason="test",
        evidence_refs=[
            EvidenceRef(
                page=1,
                block_id="p1_b001",
                bbox=BBox(10.0, 10.0, 100.0, 40.0),
                confidence=0.9,
                engine="native_text",
            )
        ],
        flags=flags or [],
    )


def _complete_fields() -> ContractFields:
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


def test_review_queue_keeps_reason_codes_and_rejection_context() -> None:
    scorer = ReviewScorer()
    queue_builder = ReviewQueueBuilder()
    fields = _complete_fields()
    issues = [
        ProcessingIssue(
            severity=ErrorSeverity.REVIEW,
            reason_code=ReasonCode.LOW_QUALITY_JURISDICTION,
            message="jurisdiction candidate was rejected by quality gate",
            page=3,
            block_id="p3_b005",
            details={
                "field_name": "jurisdiction",
                "candidate_value": "番の専属的合意管轄裁判所",
                "why_rejected": "broken_jurisdiction_fragment",
                "bbox": {"x0": 10.0, "y0": 20.0, "x1": 200.0, "y1": 60.0},
                "snippet": "番の専属的合意管轄裁判所",
                "confidence": 0.84,
            },
        )
    ]
    assessment = scorer.score(
        issues=issues,
        merged_pages=[
            MergedPage(
                page=1,
                page_kind=DocumentKind.TEXT_NATIVE,
                native_text_char_count=1200,
                ocr_ratio=0.02,
                classification_reason="test",
            )
        ],
        fields=fields,
    )
    review = queue_builder.build(doc_id="doc_001", assessment=assessment).payload
    item = review["items"][0]

    assert isinstance(item["reason_codes"], list)
    assert ReasonCode.LOW_QUALITY_JURISDICTION.value in item["reason_codes"]
    assert item["field"] == "jurisdiction"
    assert item["candidate_value"] == "番の専属的合意管轄裁判所"
    assert item["why_rejected"] == "broken_jurisdiction_fragment"
    assert item["suggested_action"] is not None


def test_review_json_writer_accepts_reason_codes_and_legacy_code_shape(tmp_path) -> None:
    writer = ReviewJsonWriter()
    payload = {
        "doc_id": "doc_legacy",
        "review_required": True,
        "items": [
            {
                "review_id": "rev_0001",
                "level": "warning",
                "code": ReasonCode.LOW_QUALITY_COUNTERPARTY.value,
                "message": "counterparty low quality",
                "page_refs": [1],
                "block_ids": ["p1_b001"],
                "field_names": ["counterparties"],
            }
        ],
        "summary": {"warning_count": 1, "critical_count": 0},
    }
    output = writer.write(tmp_path, payload)
    data = json.loads(output.read_text(encoding="utf-8"))

    assert data["items"][0]["reason_codes"] == [ReasonCode.LOW_QUALITY_COUNTERPARTY.value]


def test_document_writer_field_dict_contains_optional_quality_info() -> None:
    field = _field("effective_date", "契約締結日から", flags=["anchor_only_effective_date"])
    payload = DocumentJsonWriter._field_to_dict(field)

    assert "quality" in payload
    assert payload["quality"]["anchor_only"] is True


def test_document_writer_field_dict_includes_semantic_type_and_relative_jurisdiction_expression() -> None:
    field = _field(
        "jurisdiction",
        None,
        flags=[
            "low_quality_jurisdiction",
            "semantic_type:relative_term",
            "relative_jurisdiction_expression",
            "relative_jurisdiction_expression:甲の所在地を管轄する裁判所",
        ],
    )
    payload = DocumentJsonWriter._field_to_dict(field)

    assert "quality" in payload
    assert payload["quality"]["semantic_type"] == "relative_term"
    assert payload["quality"]["relative_jurisdiction_expression"] == "甲の所在地を管轄する裁判所"
    assert "low_quality_jurisdiction" in payload["quality"]["quality_flags"]
