from __future__ import annotations

from collections import defaultdict

from contract_ingest.config import Settings, get_settings
from contract_ingest.domain.enums import BlockType, ErrorSeverity, ExtractMethod, ReasonCode
from contract_ingest.domain.models import (
    LayoutAnalysisResult,
    MergeResult,
    MergedPage,
    NativeExtractionResult,
    OCRExtractionResult,
    ProcessingIssue,
    UnifiedBlock,
)
from contract_ingest.extract.layout import infer_block_type
from contract_ingest.utils.logging import get_logger
from contract_ingest.utils.text import is_noise_text


class BlockMerger:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.logger = get_logger(__name__)

    def merge(
        self,
        native_result: NativeExtractionResult,
        layout_result: LayoutAnalysisResult,
        ocr_result: OCRExtractionResult,
    ) -> MergeResult:
        warnings: list[ProcessingIssue] = list(layout_result.issues)
        warnings.extend(ocr_result.issues)
        errors: list[ProcessingIssue] = list(native_result.errors)

        native_by_page: dict[int, list] = defaultdict(list)
        for block in native_result.blocks:
            native_by_page[block.page].append(block)

        native_page_metrics = {page.page: page for page in native_result.pages}
        page_layout = {page.page: page for page in layout_result.pages}

        regions_by_page: dict[int, list] = defaultdict(list)
        for page in layout_result.pages:
            regions_by_page[page.page].extend(page.ocr_regions)

        ocr_by_region: dict[str, list] = defaultdict(list)
        for block in ocr_result.blocks:
            ocr_by_region[block.region_id].append(block)

        page_candidates: dict[int, list[dict]] = defaultdict(list)
        consumed_regions: set[str] = set()

        for page_no, native_blocks in native_by_page.items():
            source_regions = regions_by_page.get(page_no, [])
            source_regions_by_block: dict[str, list] = defaultdict(list)
            for region in source_regions:
                if region.source_block_id:
                    source_regions_by_block[region.source_block_id].append(region)

            for block in sorted(native_blocks, key=lambda b: (b.bbox.y0, b.bbox.x0)):
                linked_regions = source_regions_by_block.get(block.block_id, [])
                linked_ocr_blocks = []
                for region in linked_regions:
                    linked_ocr_blocks.extend(ocr_by_region.get(region.region_id, []))

                native_weak = (
                    block.char_count < self.settings.min_block_text_chars
                    or block.garbled_ratio > self.settings.max_garbled_ratio
                    or not block.searchable
                )

                if native_weak and linked_ocr_blocks:
                    for region in linked_regions:
                        consumed_regions.add(region.region_id)
                    for ocr_block in sorted(linked_ocr_blocks, key=lambda o: (o.bbox.y0, o.bbox.x0)):
                        page_candidates[page_no].append(
                            {
                                "bbox": ocr_block.bbox,
                                "text": ocr_block.text,
                                "engine": ocr_block.engine,
                                "extract_method": ExtractMethod.OCR,
                                "confidence": ocr_block.confidence,
                                "block_type": ocr_block.block_type,
                                "source_block_ids": [block.block_id, ocr_block.block_id],
                                "adoption_reason": "ocr_replaced_weak_native",
                            }
                        )
                    continue

                if native_weak and not linked_ocr_blocks:
                    warnings.append(
                        ProcessingIssue(
                            severity=ErrorSeverity.REVIEW,
                            reason_code=ReasonCode.PARTIAL_EXTRACTION_FAILURE,
                            message="weak native block had no OCR fallback result",
                            page=block.page,
                            block_id=block.block_id,
                            details={"garbled_ratio": block.garbled_ratio, "char_count": block.char_count},
                        )
                    )

                page_candidates[page_no].append(
                    {
                        "bbox": block.bbox,
                        "text": block.text,
                        "engine": "native_text",
                        "extract_method": ExtractMethod.NATIVE_TEXT,
                        "confidence": None,
                        "block_type": block.block_type,
                        "source_block_ids": [block.block_id],
                        "adoption_reason": "native_kept",
                    }
                )

        for page_no, regions in regions_by_page.items():
            for region in regions:
                if region.region_id in consumed_regions and region.source_block_id is not None:
                    continue
                ocr_blocks = ocr_by_region.get(region.region_id, [])
                if not ocr_blocks:
                    continue
                for ocr_block in sorted(ocr_blocks, key=lambda o: (o.bbox.y0, o.bbox.x0)):
                    page_candidates[page_no].append(
                        {
                            "bbox": ocr_block.bbox,
                            "text": ocr_block.text,
                            "engine": ocr_block.engine,
                            "extract_method": ExtractMethod.OCR,
                            "confidence": ocr_block.confidence,
                            "block_type": ocr_block.block_type,
                            "source_block_ids": [ocr_block.block_id],
                            "adoption_reason": "ocr_region_added",
                        }
                    )

        merged_blocks: list[UnifiedBlock] = []
        global_reading_order = 1

        for page_no in sorted(page_candidates):
            candidates = sorted(page_candidates[page_no], key=lambda c: (c["bbox"].y0, c["bbox"].x0))

            page_height = self._estimate_page_height(candidates)
            kept: list[dict] = []
            for candidate in candidates:
                if not candidate["text"]:
                    continue
                is_duplicate = False
                for existing in kept:
                    same_text = candidate["text"] == existing["text"]
                    overlap = candidate["bbox"].iou(existing["bbox"]) > 0.90
                    if same_text and overlap:
                        is_duplicate = True
                        break
                if not is_duplicate:
                    kept.append(candidate)

            for idx, candidate in enumerate(kept, start=1):
                text = candidate["text"].strip()
                method = candidate["extract_method"]
                block_type = candidate["block_type"]
                if block_type == BlockType.TEXT:
                    block_type = infer_block_type(text=text, bbox=candidate["bbox"], page_height=page_height)

                searchable = bool(text) and not is_noise_text(text)
                if block_type in {BlockType.SIGNATURE_AREA, BlockType.STAMP_AREA, BlockType.IMAGE}:
                    searchable = False

                block_id = f"p{page_no}_b{idx:03d}"
                unified = UnifiedBlock(
                    page=page_no,
                    block_id=block_id,
                    block_type=block_type,
                    bbox=candidate["bbox"],
                    text=text,
                    engine=str(candidate["engine"]),
                    extract_method=method,
                    confidence=candidate["confidence"],
                    searchable=searchable,
                    reading_order=global_reading_order,
                    source_block_ids=list(candidate["source_block_ids"]),
                    adoption_reason=str(candidate["adoption_reason"]),
                )
                merged_blocks.append(unified)
                global_reading_order += 1

                confidence = unified.confidence
                if confidence is not None and confidence < self.settings.low_confidence_threshold:
                    warnings.append(
                        ProcessingIssue(
                            severity=ErrorSeverity.REVIEW,
                            reason_code=ReasonCode.LOW_CONFIDENCE,
                            message="merged OCR block confidence is below threshold",
                            page=unified.page,
                            block_id=unified.block_id,
                            details={"confidence": confidence},
                        )
                    )

        pages_summary: list[MergedPage] = []
        blocks_by_page: dict[int, list[UnifiedBlock]] = defaultdict(list)
        for block in merged_blocks:
            blocks_by_page[block.page].append(block)

        for page_no in sorted(page_layout):
            page_blocks = blocks_by_page.get(page_no, [])
            page_chars_total = sum(len(block.text) for block in page_blocks)
            page_ocr_chars = sum(
                len(block.text)
                for block in page_blocks
                if block.extract_method in {ExtractMethod.OCR, ExtractMethod.HYBRID}
            )
            if page_chars_total > 0:
                ocr_ratio = page_ocr_chars / page_chars_total
            else:
                ocr_ratio = page_layout[page_no].ocr_ratio

            page_metric = native_page_metrics.get(page_no)
            native_char_count = page_metric.native_text_char_count if page_metric else 0

            pages_summary.append(
                MergedPage(
                    page=page_no,
                    page_kind=page_layout[page_no].page_kind,
                    native_text_char_count=native_char_count,
                    ocr_ratio=min(1.0, ocr_ratio),
                    classification_reason=page_layout[page_no].classification_reason,
                )
            )

        return MergeResult(pages=pages_summary, blocks=merged_blocks, warnings=warnings, errors=errors)

    @staticmethod
    def _estimate_page_height(candidates: list[dict]) -> float:
        if not candidates:
            return 1000.0
        return max(float(candidate["bbox"].y1) for candidate in candidates)
