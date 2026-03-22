from __future__ import annotations

from contract_ingest.config import Settings, get_settings
from contract_ingest.domain.models import EvidenceBlock, EvidenceRef, MergeResult, UnifiedBlock


class EvidenceBuilder:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def build(self, merge_result: MergeResult, source_hash: str) -> list[EvidenceBlock]:
        evidence_blocks: list[EvidenceBlock] = []
        for block in merge_result.blocks:
            evidence_blocks.append(self._from_unified_block(block=block, source_hash=source_hash))
        return evidence_blocks

    def build_ref(self, block: EvidenceBlock | UnifiedBlock) -> EvidenceRef:
        return EvidenceRef(
            page=block.page,
            block_id=block.block_id,
            bbox=block.bbox,
            confidence=block.confidence,
            engine=block.engine,
        )

    def _from_unified_block(self, block: UnifiedBlock, source_hash: str) -> EvidenceBlock:
        return EvidenceBlock(
            page=block.page,
            block_id=block.block_id,
            block_type=block.block_type,
            bbox=block.bbox,
            text=block.text,
            engine=block.engine,
            extract_method=block.extract_method,
            confidence=block.confidence,
            searchable=block.searchable,
            reading_order=block.reading_order,
            source_hash=source_hash,
            pipeline_version=self.settings.pipeline_version,
            section_type=block.section_type,
        )
