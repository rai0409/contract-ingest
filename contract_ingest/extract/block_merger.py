from __future__ import annotations

from collections import defaultdict
import re
from typing import Any

from contract_ingest.config import Settings, get_settings
from contract_ingest.domain.enums import BlockType, ErrorSeverity, ExtractMethod, ReasonCode, SectionType
from contract_ingest.domain.models import (
    BBox,
    LayoutAnalysisResult,
    MergeResult,
    MergedPage,
    NativeExtractionResult,
    OCRExtractionResult,
    ProcessingIssue,
    UnifiedBlock,
)
from contract_ingest.extract.layout import LayoutAnalyzer, infer_block_type
from contract_ingest.utils.logging import get_logger
from contract_ingest.utils.text import (
    is_annotation_like_text,
    is_article_heading_text,
    is_fragment_like_text,
    is_noise_text,
    is_page_number_text,
    normalize_text,
    parse_article_number,
)

_ENTITY_TYPE_ONLY_OCR_FRAGMENTS = {
    "株式会社",
    "合同会社",
    "有限会社",
    "公益財団法人",
    "一般社団法人",
    "国立大学法人",
    "学校法人",
}

_FORM_SECTION_RE = re.compile(r"(?:^|\s)(?:様式第?[0-9０-９一二三四五六七八九十]+|通知書|届出書|請求書|実績報告書)")
_INSTRUCTION_SECTION_RE = re.compile(r"(?:記載要領|記入要領|作成要領|取扱要領|記載例)")
_EXECUTION_SIGNATURE_SECTION_RE = re.compile(
    r"(?:上記契約の成立を証するため|本契約(?:締結)?の証として|この契約書は[0-9０-９一二三四五六七八九十]+通作成し|電磁的記録|電子署名)"
)
_TAIL_OPTIONAL_FORM_RE = re.compile(r"(?:必要に応じて追加|任意記載)")


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

        page_height_hints = self._estimate_page_heights(native_by_page, regions_by_page)
        repeated_margin_texts = self._collect_repeated_margin_texts(native_by_page, page_height_hints)

        page_candidates: dict[int, list[dict]] = defaultdict(list)
        consumed_regions: set[str] = set()

        for page_no, native_blocks in native_by_page.items():
            page_height = page_height_hints.get(page_no, 1000.0)
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

                native_kind = self._classify_candidate_kind(
                    text=block.text,
                    bbox=block.bbox,
                    page_height=page_height,
                    block_type=block.block_type,
                    repeated_margin_texts=repeated_margin_texts,
                )
                native_weak = (
                    block.char_count < self.settings.min_block_text_chars
                    or block.garbled_ratio > self.settings.max_garbled_ratio
                    or not block.searchable
                )

                if native_weak and linked_ocr_blocks:
                    accepted_ocr: list[tuple[Any, str]] = []
                    rejected_annotation = 0
                    rejected_low_confidence = 0
                    for ocr_block in sorted(linked_ocr_blocks, key=lambda o: (o.bbox.y0, o.bbox.x0)):
                        ocr_kind = self._classify_candidate_kind(
                            text=ocr_block.text,
                            bbox=ocr_block.bbox,
                            page_height=page_height,
                            block_type=ocr_block.block_type,
                            repeated_margin_texts=repeated_margin_texts,
                        )
                        if ocr_kind in {"annotation", "header_footer"}:
                            rejected_annotation += 1
                            continue
                        if self._is_low_value_ocr_fragment(ocr_block.text, ocr_block.confidence):
                            rejected_low_confidence += 1
                            continue
                        if (
                            native_kind == "body"
                            and ocr_block.confidence is not None
                            and ocr_block.confidence < self.settings.low_confidence_threshold
                        ):
                            rejected_low_confidence += 1
                            continue
                        accepted_ocr.append((ocr_block, ocr_kind))

                    for region in linked_regions:
                        consumed_regions.add(region.region_id)

                    if accepted_ocr:
                        for ocr_block, ocr_kind in accepted_ocr:
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
                                    "candidate_kind": ocr_kind,
                                    "section_type": self._infer_section_type(
                                        text=ocr_block.text,
                                        block_type=ocr_block.block_type,
                                        candidate_kind=ocr_kind,
                                    ),
                                }
                            )
                        continue

                    warnings.append(
                        ProcessingIssue(
                            severity=ErrorSeverity.REVIEW,
                            reason_code=ReasonCode.PARTIAL_EXTRACTION_FAILURE,
                            message="weak native block OCR fallback was rejected",
                            page=block.page,
                            block_id=block.block_id,
                            details={
                                "rejected_annotation_like": rejected_annotation,
                                "rejected_low_confidence": rejected_low_confidence,
                            },
                        )
                    )

                if native_weak and not linked_ocr_blocks and native_kind in {"body", "signature"}:
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

                adoption_reason = "native_kept"
                if native_weak and linked_ocr_blocks:
                    adoption_reason = "native_kept_ocr_rejected"

                page_candidates[page_no].append(
                    {
                        "bbox": block.bbox,
                        "text": block.text,
                        "engine": "native_text",
                        "extract_method": ExtractMethod.NATIVE_TEXT,
                        "confidence": None,
                        "block_type": block.block_type,
                        "source_block_ids": [block.block_id],
                        "adoption_reason": adoption_reason,
                        "candidate_kind": native_kind,
                        "section_type": self._infer_section_type(
                            text=block.text,
                            block_type=block.block_type,
                            candidate_kind=native_kind,
                        ),
                    }
                )

        for page_no, regions in regions_by_page.items():
            page_height = page_height_hints.get(page_no, 1000.0)
            for region in regions:
                if region.region_id in consumed_regions and region.source_block_id is not None:
                    continue
                ocr_blocks = ocr_by_region.get(region.region_id, [])
                if not ocr_blocks:
                    continue
                for ocr_block in sorted(ocr_blocks, key=lambda o: (o.bbox.y0, o.bbox.x0)):
                    ocr_kind = self._classify_candidate_kind(
                        text=ocr_block.text,
                        bbox=ocr_block.bbox,
                        page_height=page_height,
                        block_type=ocr_block.block_type,
                        repeated_margin_texts=repeated_margin_texts,
                    )
                    if ocr_kind in {"annotation", "header_footer"}:
                        continue
                    if self._is_low_value_ocr_fragment(ocr_block.text, ocr_block.confidence):
                        continue
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
                            "candidate_kind": ocr_kind,
                            "section_type": self._infer_section_type(
                                text=ocr_block.text,
                                block_type=ocr_block.block_type,
                                candidate_kind=ocr_kind,
                            ),
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

            kept = self._merge_adjacent_body_candidates(kept)

            for idx, candidate in enumerate(kept, start=1):
                text = str(candidate["text"]).strip()
                if not text:
                    continue
                method = candidate["extract_method"]
                block_type = candidate["block_type"]
                candidate_kind = str(candidate.get("candidate_kind", "body"))
                if candidate_kind == "annotation":
                    block_type = BlockType.OTHER
                elif candidate_kind == "table":
                    block_type = BlockType.TABLE
                elif candidate_kind == "appendix":
                    if block_type in {BlockType.HEADER, BlockType.FOOTER, BlockType.SIGNATURE_AREA}:
                        block_type = BlockType.TEXT
                elif candidate_kind == "signature":
                    if block_type != BlockType.STAMP_AREA:
                        block_type = BlockType.SIGNATURE_AREA
                elif candidate_kind == "header_footer":
                    if candidate["bbox"].y1 <= page_height * 0.10:
                        block_type = BlockType.HEADER
                    else:
                        block_type = BlockType.FOOTER
                elif block_type == BlockType.TEXT:
                    block_type = infer_block_type(text=text, bbox=candidate["bbox"], page_height=page_height)

                searchable = bool(text) and not is_noise_text(text) and candidate_kind == "body"
                if block_type in {
                    BlockType.SIGNATURE_AREA,
                    BlockType.STAMP_AREA,
                    BlockType.IMAGE,
                    BlockType.TABLE,
                    BlockType.HEADER,
                    BlockType.FOOTER,
                }:
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
                    section_type=candidate.get("section_type", SectionType.MAIN_CONTRACT),
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

    @staticmethod
    def _estimate_page_heights(native_by_page: dict[int, list], regions_by_page: dict[int, list]) -> dict[int, float]:
        page_heights: dict[int, float] = {}
        all_pages = set(native_by_page.keys()) | set(regions_by_page.keys())
        for page_no in all_pages:
            max_y = 0.0
            for block in native_by_page.get(page_no, []):
                max_y = max(max_y, float(block.bbox.y1))
            for region in regions_by_page.get(page_no, []):
                max_y = max(max_y, float(region.bbox.y1))
            page_heights[page_no] = max_y if max_y > 0.0 else 1000.0
        return page_heights

    @staticmethod
    def _collect_repeated_margin_texts(
        native_by_page: dict[int, list],
        page_heights: dict[int, float],
    ) -> set[str]:
        counts: dict[str, set[int]] = {}
        for page_no, blocks in native_by_page.items():
            page_height = page_heights.get(page_no, 1000.0)
            for block in blocks:
                text = normalize_text(block.text)
                if not text or len(text) > 80 or is_article_heading_text(text):
                    continue
                in_margin = block.bbox.y1 <= page_height * 0.12 or block.bbox.y0 >= page_height * 0.88
                if not in_margin:
                    continue
                counts.setdefault(text, set()).add(page_no)
        return {text for text, pages in counts.items() if len(pages) >= 2}

    @staticmethod
    def _classify_candidate_kind(
        text: str,
        bbox: BBox,
        page_height: float,
        block_type: BlockType,
        repeated_margin_texts: set[str],
    ) -> str:
        normalized = normalize_text(text)
        if not normalized:
            return "annotation"
        if block_type == BlockType.TABLE:
            return "table"
        if block_type in {BlockType.SIGNATURE_AREA, BlockType.STAMP_AREA}:
            return "signature"
        if block_type in {BlockType.HEADER, BlockType.FOOTER}:
            return "header_footer"
        if LayoutAnalyzer._is_appendix_heading(normalized):
            return "appendix"
        if BlockMerger._is_appendix_like_text(normalized):
            return "appendix"
        if is_article_heading_text(normalized):
            return "body"
        if is_annotation_like_text(normalized):
            return "annotation"
        if LayoutAnalyzer._looks_like_annotation_column(normalized, bbox=bbox, page_height=page_height):
            return "annotation"
        if LayoutAnalyzer._is_signature_like_text(normalized, bbox, page_height):
            return "signature"
        if LayoutAnalyzer._looks_like_header_footer_block(
            normalized,
            bbox=bbox,
            page_height=page_height,
            repeated_margin_texts=repeated_margin_texts,
        ):
            return "header_footer"
        if BlockMerger._is_short_critical_clause_text(normalized):
            return "body"
        if LayoutAnalyzer._is_table_like_block(normalized, bbox=bbox, page_height=page_height):
            return "table"
        if LayoutAnalyzer._is_low_value_fragment_text(normalized):
            return "annotation"
        if (
            len(normalized) <= 8
            and not LayoutAnalyzer._looks_like_sentence_text(normalized)
            and not any(token in normalized for token in ["契約", "条", "甲", "乙", "法", "裁判所"])
        ):
            return "annotation"
        symbol_like = sum(1 for ch in normalized if ch in "[]{}<>|/\\*#@")
        if (
            len(normalized) <= 14
            and symbol_like >= 2
            and not any(token in normalized for token in ["契約", "条", "甲", "乙", "法"])
        ):
            return "annotation"
        if is_fragment_like_text(normalized) and not any(
            token in normalized for token in ["甲", "乙", "株式会社", "合同会社", "有限会社", "第", "条"]
        ):
            return "annotation"
        return "body"

    def _merge_adjacent_body_candidates(self, candidates: list[dict]) -> list[dict]:
        if not candidates:
            return []
        merged: list[dict] = []
        for candidate in candidates:
            if not merged:
                merged.append(candidate)
                continue
            previous = merged[-1]
            prev_kind = previous.get("candidate_kind")
            curr_kind = candidate.get("candidate_kind")
            can_merge = False
            merged_kind = "body"
            if previous.get("section_type", SectionType.MAIN_CONTRACT) != candidate.get(
                "section_type",
                SectionType.MAIN_CONTRACT,
            ):
                merged.append(candidate)
                continue
            if prev_kind == "body" and curr_kind == "body":
                can_merge = self._can_merge_body(previous, candidate)
                if can_merge and self._crosses_article_boundary(previous["text"], candidate["text"]):
                    can_merge = False
                merged_kind = "body"
            elif prev_kind == "appendix" and curr_kind == "appendix":
                can_merge = self._can_merge_appendix(previous, candidate)
                merged_kind = "appendix"
            if not can_merge:
                merged.append(candidate)
                continue
            combined_source_ids = list(previous["source_block_ids"])
            for source_id in candidate["source_block_ids"]:
                if source_id not in combined_source_ids:
                    combined_source_ids.append(source_id)
            merged[-1] = {
                "bbox": self._union_bbox(previous["bbox"], candidate["bbox"]),
                "text": f"{previous['text'].rstrip()}\n{candidate['text'].lstrip()}",
                "engine": previous["engine"],
                "extract_method": previous["extract_method"],
                "confidence": self._merge_confidence(previous["confidence"], candidate["confidence"]),
                "block_type": previous["block_type"],
                "source_block_ids": combined_source_ids,
                "adoption_reason": f"{previous['adoption_reason']}+body_compatible_merge",
                "candidate_kind": merged_kind,
                "section_type": previous.get("section_type", SectionType.MAIN_CONTRACT),
            }
        return merged

    @staticmethod
    def _can_merge_body(previous: dict, current: dict) -> bool:
        if previous.get("candidate_kind") != "body" or current.get("candidate_kind") != "body":
            return False
        if previous.get("block_type") != BlockType.TEXT or current.get("block_type") != BlockType.TEXT:
            return False
        if previous["extract_method"] != current["extract_method"]:
            return False
        prev_text = normalize_text(previous["text"])
        curr_text = normalize_text(current["text"])
        if not prev_text or not curr_text:
            return False
        if is_annotation_like_text(prev_text) or is_annotation_like_text(curr_text):
            return False
        if BlockMerger._is_low_value_ocr_fragment(prev_text, previous.get("confidence")):
            return False
        if BlockMerger._is_low_value_ocr_fragment(curr_text, current.get("confidence")):
            return False
        if is_page_number_text(prev_text) or is_page_number_text(curr_text):
            return False
        if is_article_heading_text(prev_text) or is_article_heading_text(curr_text):
            return False
        if BlockMerger._is_appendix_like_text(prev_text) or BlockMerger._is_appendix_like_text(curr_text):
            return False
        low_value_markers = (
            "契約",
            "条",
            "法",
            "裁判所",
            "甲",
            "乙",
            "株式会社",
            "合意",
            "準拠",
            "適用法",
            "日本法",
            "契約締結日",
            "法人",
            "大学",
            "研究所",
        )
        if previous["extract_method"] == ExtractMethod.OCR and len(prev_text) <= 10:
            if not any(marker in prev_text for marker in low_value_markers):
                return False
        if current["extract_method"] == ExtractMethod.OCR and len(curr_text) <= 10:
            if not any(marker in curr_text for marker in low_value_markers):
                return False
        if len(prev_text) <= 8 and not LayoutAnalyzer._looks_like_sentence_text(prev_text):
            if not any(marker in prev_text for marker in low_value_markers) and not BlockMerger._is_short_critical_clause_text(prev_text):
                return False
        if len(curr_text) <= 8 and not LayoutAnalyzer._looks_like_sentence_text(curr_text):
            if not any(marker in curr_text for marker in low_value_markers) and not BlockMerger._is_short_critical_clause_text(curr_text):
                return False
        if (
            is_fragment_like_text(prev_text)
            and len(prev_text) <= 12
            and not BlockMerger._is_short_critical_clause_text(prev_text)
        ):
            return False
        if (
            is_fragment_like_text(curr_text)
            and len(curr_text) <= 12
            and not BlockMerger._is_short_critical_clause_text(curr_text)
        ):
            return False

        prev_bbox = previous["bbox"]
        curr_bbox = current["bbox"]
        overlap_x = max(0.0, min(prev_bbox.x1, curr_bbox.x1) - max(prev_bbox.x0, curr_bbox.x0))
        min_width = min(prev_bbox.width, curr_bbox.width)
        if min_width <= 0.0 or overlap_x / min_width < 0.60:
            return False
        left_delta = abs(prev_bbox.x0 - curr_bbox.x0)
        if left_delta > max(12.0, min_width * 0.10):
            return False

        vertical_gap = curr_bbox.y0 - prev_bbox.y1
        max_gap = max(14.0, min(prev_bbox.height, curr_bbox.height) * 1.20)
        if len(curr_text) <= 12 or len(prev_text) <= 12:
            max_gap = min(max_gap, 10.0)
        return -2.0 <= vertical_gap <= max_gap

    @staticmethod
    def _can_merge_appendix(previous: dict, current: dict) -> bool:
        if previous.get("candidate_kind") != "appendix" or current.get("candidate_kind") != "appendix":
            return False
        if previous.get("block_type") not in {BlockType.TEXT, BlockType.TABLE}:
            return False
        if current.get("block_type") not in {BlockType.TEXT, BlockType.TABLE}:
            return False
        if previous["extract_method"] != current["extract_method"]:
            return False
        prev_text = normalize_text(previous["text"])
        curr_text = normalize_text(current["text"])
        if not prev_text or not curr_text:
            return False
        if is_annotation_like_text(prev_text) or is_annotation_like_text(curr_text):
            return False
        if is_page_number_text(prev_text) or is_page_number_text(curr_text):
            return False
        prev_bbox = previous["bbox"]
        curr_bbox = current["bbox"]
        overlap_x = max(0.0, min(prev_bbox.x1, curr_bbox.x1) - max(prev_bbox.x0, curr_bbox.x0))
        min_width = min(prev_bbox.width, curr_bbox.width)
        if min_width <= 0.0 or overlap_x / min_width < 0.55:
            return False
        vertical_gap = curr_bbox.y0 - prev_bbox.y1
        return -2.0 <= vertical_gap <= max(24.0, min(prev_bbox.height, curr_bbox.height) * 2.0)

    @staticmethod
    def _union_bbox(lhs: BBox, rhs: BBox) -> BBox:
        return BBox(
            x0=min(lhs.x0, rhs.x0),
            y0=min(lhs.y0, rhs.y0),
            x1=max(lhs.x1, rhs.x1),
            y1=max(lhs.y1, rhs.y1),
        )

    @staticmethod
    def _merge_confidence(lhs: float | None, rhs: float | None) -> float | None:
        if lhs is None:
            return rhs
        if rhs is None:
            return lhs
        return min(lhs, rhs)

    @staticmethod
    def _crosses_article_boundary(previous_text: str, current_text: str) -> bool:
        prev_article = BlockMerger._extract_article_no(previous_text)
        curr_article = BlockMerger._extract_article_no(current_text)
        if prev_article is None or curr_article is None:
            return False
        return prev_article != curr_article

    @staticmethod
    def _extract_article_no(text: str) -> int | None:
        normalized = normalize_text(text)
        if not normalized:
            return None
        match = re.match(r"^\s*(第[0-9０-９一二三四五六七八九十百千〇零]+条)", normalized)
        if not match:
            return None
        return parse_article_number(match.group(1))

    @staticmethod
    def _infer_section_type(text: str, block_type: BlockType, candidate_kind: str) -> SectionType:
        normalized = normalize_text(text)
        if block_type in {BlockType.SIGNATURE_AREA, BlockType.STAMP_AREA} or candidate_kind == "signature":
            return SectionType.SIGNATURE
        if not normalized:
            return SectionType.MAIN_CONTRACT
        if _EXECUTION_SIGNATURE_SECTION_RE.search(normalized):
            return SectionType.SIGNATURE
        if "特記事項" in normalized:
            return SectionType.SPECIAL_PROVISIONS
        if LayoutAnalyzer._is_appendix_heading(normalized) or BlockMerger._is_appendix_like_text(normalized):
            return SectionType.APPENDIX
        if _INSTRUCTION_SECTION_RE.search(normalized):
            return SectionType.INSTRUCTION
        if _FORM_SECTION_RE.search(normalized) or _TAIL_OPTIONAL_FORM_RE.search(normalized):
            return SectionType.FORM
        signature_markers = ("契約締結日", "記名押印", "署名欄", "押印欄", "甲", "乙", "代表者", "氏名", "住所")
        if len(normalized) <= 56 and any(marker in normalized for marker in signature_markers):
            if any(token in normalized for token in ["記名押印", "署名", "押印", "住所", "氏名", "代表者"]):
                return SectionType.SIGNATURE
        return SectionType.MAIN_CONTRACT

    @staticmethod
    def _is_low_value_ocr_fragment(text: str, confidence: float | None) -> bool:
        normalized = normalize_text(text)
        if not normalized:
            return True
        if is_article_heading_text(normalized):
            return False
        if normalized in {"甲", "乙"}:
            return False
        if BlockMerger._is_entity_type_only_ocr_fragment(normalized):
            return True
        if is_annotation_like_text(normalized):
            return True
        if len(normalized) <= 2:
            return True
        if re.search(r"[0０OoＯ]+\s*(?:日間?|週間?|か月|ヶ月|ヵ月|年(?:間)?)", normalized):
            return True
        if len(normalized) <= 4 and not any(token in normalized for token in ["甲", "乙", "第", "条", "法"]):
            return True
        if is_fragment_like_text(normalized) and (confidence is None or confidence < 0.95):
            return True
        if confidence is not None and confidence < 0.55:
            return True
        legal_markers = (
            "契約",
            "条",
            "法",
            "裁判所",
            "甲",
            "乙",
            "株式会社",
            "日本法",
            "契約締結日",
            "法人",
            "大学",
            "研究所",
        )
        if len(normalized) <= 8 and confidence is not None and confidence < 0.70:
            if not any(marker in normalized for marker in legal_markers):
                return True
        return False

    @staticmethod
    def _is_appendix_like_text(text: str) -> bool:
        normalized = normalize_text(text)
        if not normalized:
            return False
        if LayoutAnalyzer._is_appendix_heading(normalized):
            return True
        appendix_markers = ("別紙", "別表", "別添", "付属資料", "仕様書")
        return len(normalized) <= 24 and any(marker in normalized for marker in appendix_markers)

    @staticmethod
    def _is_short_critical_clause_text(text: str) -> bool:
        normalized = normalize_text(text)
        if not normalized:
            return False
        compact = normalize_text(normalized).replace(" ", "")
        if len(compact) > 36:
            return False
        if any(token in compact for token in ["記名押印", "署名欄", "代表者", "氏名", "住所", "㊞"]):
            return False
        japanese_markers = (
            "準拠法",
            "適用法",
            "日本法",
            "日本国法",
            "管轄",
            "裁判所",
            "契約締結日",
            "効力発生日",
            "発効日",
            "発効",
            "効力",
            "履行",
            "解釈",
        )
        if any(marker in compact for marker in japanese_markers):
            return True
        lower = normalized.lower()
        english_phrases = (
            "governed by",
            "laws of",
            "effective date",
            "effective as of",
            "entered into as of",
            "dated as of",
            "from and after",
            "on and after",
            "execution date",
            "date of execution",
            "date first written above",
            "date of last signature",
        )
        return any(phrase in lower for phrase in english_phrases)

    @staticmethod
    def _is_entity_type_only_ocr_fragment(text: str) -> bool:
        return normalize_text(text) in _ENTITY_TYPE_ONLY_OCR_FRAGMENTS
