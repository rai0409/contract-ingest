from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from contract_ingest.config import Settings, get_settings
from contract_ingest.domain.enums import ChunkType, ErrorSeverity, ExtractMethod, ReasonCode, SectionType
from contract_ingest.domain.models import ClauseUnit, ContractFields, EvidenceBlock, ProcessingIssue
from contract_ingest.utils.text import normalize_text, unique_preserve_order


@dataclass(frozen=True)
class ChunkBuildResult:
    chunks: list[dict[str, Any]]
    issues: list[ProcessingIssue] = field(default_factory=list)


class ChunkBuilder:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def build(
        self,
        doc_id: str,
        clauses: list[ClauseUnit],
        evidence_blocks: list[EvidenceBlock],
        fields: ContractFields,
    ) -> ChunkBuildResult:
        issues: list[ProcessingIssue] = []
        evidence_by_block = {block.block_id: block for block in evidence_blocks}

        contract_type: str | None
        contract_type_value = fields.contract_type.value
        if isinstance(contract_type_value, str):
            contract_type = contract_type_value
        else:
            contract_type = None

        chunks: list[dict[str, Any]] = []

        for idx, clause in enumerate(clauses):
            chunk_type = self._resolve_chunk_type(clause)
            chunk_text = self._build_chunk_text(clause)
            block_ids = unique_preserve_order(list(clause.block_ids))

            evidence_refs: list[dict[str, Any]] = []
            extract_methods: set[ExtractMethod] = set()
            searchable = 0

            for block_id in block_ids:
                block = evidence_by_block.get(block_id)
                if block is None:
                    continue
                extract_methods.add(block.extract_method)
                if block.searchable:
                    searchable = 1
                evidence_refs.append(
                    {
                        "page": block.page,
                        "block_id": block.block_id,
                        "bbox": block.bbox.to_dict(),
                        "confidence": block.confidence,
                        "engine": block.engine,
                    }
                )

            if not evidence_refs and clause.evidence_refs:
                for ref in clause.evidence_refs:
                    evidence_refs.append(
                        {
                            "page": ref.page,
                            "block_id": ref.block_id,
                            "bbox": ref.bbox.to_dict(),
                            "confidence": ref.confidence,
                            "engine": ref.engine,
                        }
                    )
                    if ref.confidence is None or ref.confidence >= self.settings.low_confidence_threshold:
                        searchable = 1

            source_pages = sorted({int(ref["page"]) for ref in evidence_refs})
            quality = self._resolve_quality(extract_methods)

            if not block_ids:
                issues.append(
                    ProcessingIssue(
                        severity=ErrorSeverity.REVIEW,
                        reason_code=ReasonCode.PARTIAL_EXTRACTION_FAILURE,
                        message="clause has no block_ids for chunk metadata",
                        page=clause.page_start,
                        details={"clause_id": clause.clause_id},
                    )
                )

            chunks.append(
                {
                    "id": f"{doc_id}__chunk_{idx:04d}",
                    "text": chunk_text,
                    "metadata": {
                        "doc_id": doc_id,
                        "chunk_index": idx,
                        "type": chunk_type.value,
                        "quality": quality.value,
                        "searchable": searchable,
                        "clause_no": clause.clause_no,
                        "clause_title": clause.clause_title,
                        "source_pages": source_pages,
                        "block_ids": block_ids,
                        "evidence_refs": evidence_refs,
                        "contract_type": contract_type,
                        "section_type": clause.section_type.value,
                    },
                }
            )

        if not chunks and evidence_blocks:
            fallback_text = "\n".join(normalize_text(block.text) for block in evidence_blocks[:40]).strip()
            fallback_refs = [
                {
                    "page": block.page,
                    "block_id": block.block_id,
                    "bbox": block.bbox.to_dict(),
                    "confidence": block.confidence,
                    "engine": block.engine,
                }
                for block in evidence_blocks[:40]
            ]
            fallback_block_ids = [block.block_id for block in evidence_blocks[:40]]
            fallback_pages = sorted({block.page for block in evidence_blocks[:40]})
            fallback_quality = self._resolve_quality({block.extract_method for block in evidence_blocks[:40]})

            chunks.append(
                {
                    "id": f"{doc_id}__chunk_0000",
                    "text": fallback_text,
                    "metadata": {
                        "doc_id": doc_id,
                        "chunk_index": 0,
                        "type": ChunkType.OTHER.value,
                        "quality": fallback_quality.value,
                        "searchable": 1 if any(block.searchable for block in evidence_blocks[:40]) else 0,
                        "clause_no": None,
                        "clause_title": None,
                        "source_pages": fallback_pages,
                        "block_ids": fallback_block_ids,
                        "evidence_refs": fallback_refs,
                        "contract_type": contract_type,
                        "section_type": SectionType.MAIN_CONTRACT.value,
                    },
                }
            )
            issues.append(
                ProcessingIssue(
                    severity=ErrorSeverity.REVIEW,
                    reason_code=ReasonCode.UNSTABLE_CLAUSE_SPLIT,
                    message="fallback chunk generated due to empty clause result",
                )
            )

        return ChunkBuildResult(chunks=chunks, issues=issues)

    @staticmethod
    def _build_chunk_text(clause: ClauseUnit) -> str:
        text = normalize_text(clause.text)
        prefix_parts = [part for part in [clause.clause_no, clause.clause_title] if part]
        if clause.clause_no and text.startswith(f"{clause.clause_no} "):
            prefix_parts = [part for part in [clause.clause_title] if part]
        elif clause.clause_no and text == clause.clause_no:
            prefix_parts = [part for part in [clause.clause_title] if part]
        if prefix_parts:
            return f"{' '.join(prefix_parts)} {text}".strip()
        return text

    @staticmethod
    def _resolve_chunk_type(clause: ClauseUnit) -> ChunkType:
        if clause.section_type == SectionType.PREAMBLE:
            return ChunkType.PREAMBLE
        if clause.section_type in {SectionType.APPENDIX, SectionType.FORM, SectionType.INSTRUCTION}:
            return ChunkType.APPENDIX
        if clause.section_type == SectionType.SIGNATURE:
            return ChunkType.OTHER
        if clause.clause_no in {"別紙", "別表"}:
            return ChunkType.APPENDIX
        if clause.clause_no == "附則":
            return ChunkType.SCHEDULE
        if clause.clause_no is None and clause.clause_title == "前文":
            return ChunkType.PREAMBLE
        return ChunkType.CLAUSE

    @staticmethod
    def _resolve_quality(extract_methods: set[ExtractMethod]) -> ExtractMethod:
        if not extract_methods:
            return ExtractMethod.HYBRID
        if extract_methods == {ExtractMethod.NATIVE_TEXT}:
            return ExtractMethod.NATIVE_TEXT
        if extract_methods == {ExtractMethod.OCR}:
            return ExtractMethod.OCR
        return ExtractMethod.HYBRID
