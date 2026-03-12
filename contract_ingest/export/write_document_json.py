from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from contract_ingest.config import Settings, get_settings
from contract_ingest.domain.enums import DocumentKind, ReasonCode
from contract_ingest.domain.models import (
    ClauseSplitResult,
    ContractFields,
    EvidenceBlock,
    FieldExtractionResult,
    MergeResult,
    ProcessingIssue,
)
from contract_ingest.domain.schemas import DocumentSchema


class DocumentWriteError(RuntimeError):
    """Raised when document.json creation or validation fails."""


class DocumentJsonWriter:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def write(
        self,
        output_dir: Path,
        doc_id: str,
        source_file: str,
        document_kind: DocumentKind,
        source_hash: str,
        merge_result: MergeResult,
        evidence_blocks: list[EvidenceBlock],
        clause_result: ClauseSplitResult,
        field_result: FieldExtractionResult,
        issues: list[ProcessingIssue],
    ) -> Path:
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "document.json"

        payload = self._build_payload(
            doc_id=doc_id,
            source_file=source_file,
            document_kind=document_kind,
            source_hash=source_hash,
            merge_result=merge_result,
            evidence_blocks=evidence_blocks,
            clause_result=clause_result,
            fields=field_result.fields,
            issues=issues,
        )

        try:
            validated = DocumentSchema.model_validate(payload)
        except ValidationError as exc:
            raise DocumentWriteError("document.json validation failed") from exc

        output_path.write_text(
            json.dumps(
                validated.model_dump(mode="json"),
                ensure_ascii=False,
                indent=self.settings.output_indent,
            ),
            encoding="utf-8",
        )
        return output_path

    def _build_payload(
        self,
        doc_id: str,
        source_file: str,
        document_kind: DocumentKind,
        source_hash: str,
        merge_result: MergeResult,
        evidence_blocks: list[EvidenceBlock],
        clause_result: ClauseSplitResult,
        fields: ContractFields,
        issues: list[ProcessingIssue],
    ) -> dict[str, Any]:
        warning_records: list[dict[str, Any]] = []
        error_records: list[dict[str, Any]] = []

        for issue in issues:
            issue_dict = self._issue_to_dict(issue)
            if issue.severity.value in {"fatal", "recoverable"}:
                error_records.append(issue_dict)
            else:
                warning_records.append(issue_dict)

        pages_payload = [
            {
                "page": page.page,
                "page_kind": page.page_kind.value,
                "native_text_char_count": page.native_text_char_count,
                "ocr_ratio": page.ocr_ratio,
                "classification_reason": page.classification_reason,
            }
            for page in merge_result.pages
        ]

        blocks_payload = [
            {
                "page": block.page,
                "block_id": block.block_id,
                "block_type": block.block_type.value,
                "bbox": block.bbox.to_dict(),
                "text": block.text,
                "engine": block.engine,
                "extract_method": block.extract_method.value,
                "confidence": block.confidence,
                "searchable": block.searchable,
                "reading_order": block.reading_order,
                "source_hash": block.source_hash,
                "pipeline_version": block.pipeline_version,
            }
            for block in evidence_blocks
        ]

        clauses_payload = [
            {
                "clause_id": clause.clause_id,
                "clause_no": clause.clause_no,
                "clause_title": clause.clause_title,
                "text": clause.text,
                "page_start": clause.page_start,
                "page_end": clause.page_end,
                "block_ids": clause.block_ids,
                "evidence_refs": [
                    {
                        "page": ref.page,
                        "block_id": ref.block_id,
                        "bbox": ref.bbox.to_dict(),
                        "confidence": ref.confidence,
                        "engine": ref.engine,
                    }
                    for ref in clause.evidence_refs
                ],
                "flags": clause.flags,
            }
            for clause in clause_result.clauses
        ]

        fields_payload = self._fields_to_dict(fields)

        return {
            "doc_id": doc_id,
            "source_file": source_file,
            "document_kind": document_kind.value,
            "pipeline_version": self.settings.pipeline_version,
            "source_hash": source_hash,
            "pages": pages_payload,
            "blocks": blocks_payload,
            "clauses": clauses_payload,
            "fields": fields_payload,
            "warnings": warning_records,
            "errors": error_records,
        }

    def _fields_to_dict(self, fields: ContractFields) -> dict[str, Any]:
        return {
            "contract_type": self._field_to_dict(fields.contract_type),
            "counterparties": self._field_to_dict(fields.counterparties),
            "effective_date": self._field_to_dict(fields.effective_date),
            "expiration_date": self._field_to_dict(fields.expiration_date),
            "auto_renewal": self._field_to_dict(fields.auto_renewal),
            "termination_notice_period": self._field_to_dict(fields.termination_notice_period),
            "governing_law": self._field_to_dict(fields.governing_law),
            "jurisdiction": self._field_to_dict(fields.jurisdiction),
        }

    @staticmethod
    def _field_to_dict(field: Any) -> dict[str, Any]:
        quality: dict[str, Any] = {}
        if "anchor_only_effective_date" in field.flags:
            quality["anchor_only"] = True
        semantic_type = None
        for flag in field.flags:
            if isinstance(flag, str) and flag.startswith("semantic_type:"):
                semantic_type = flag.split(":", 1)[1]
                break
        if semantic_type:
            quality["semantic_type"] = semantic_type

        relative_expression = None
        for flag in field.flags:
            if isinstance(flag, str) and flag.startswith("relative_jurisdiction_expression:"):
                relative_expression = flag.split(":", 1)[1]
                break
        if relative_expression:
            quality["relative_jurisdiction_expression"] = relative_expression

        low_quality_flags = [
            flag
            for flag in field.flags
            if flag.startswith("low_quality") or flag.startswith("counterparty_") or flag == "rejected_by_validator"
        ]
        if low_quality_flags:
            quality["quality_flags"] = low_quality_flags

        payload = {
            "field_name": field.field_name,
            "value": field.value,
            "confidence": field.confidence,
            "reason": field.reason,
            "evidence_refs": [
                {
                    "page": ref.page,
                    "block_id": ref.block_id,
                    "bbox": ref.bbox.to_dict(),
                    "confidence": ref.confidence,
                    "engine": ref.engine,
                }
                for ref in field.evidence_refs
            ],
            "flags": list(field.flags),
        }
        if quality:
            payload["quality"] = quality
        return payload

    @staticmethod
    def _issue_to_dict(issue: ProcessingIssue) -> dict[str, Any]:
        reason_code = issue.reason_code.value if isinstance(issue.reason_code, ReasonCode) else str(issue.reason_code)
        return {
            "severity": issue.severity.value,
            "reason_code": reason_code,
            "message": issue.message,
            "page": issue.page,
            "block_id": issue.block_id,
            "details": issue.details,
        }
