from __future__ import annotations

from pathlib import Path

import fitz

from contract_ingest.config import Settings, get_settings
from contract_ingest.domain.enums import BlockType, ErrorSeverity, ExtractMethod, ReasonCode
from contract_ingest.domain.models import (
    BBox,
    NativeExtractionResult,
    NativePageMetrics,
    NativeTextBlock,
    ProcessingIssue,
)
from contract_ingest.utils.logging import get_logger
from contract_ingest.utils.text import garbled_ratio, is_noise_text, normalize_text


class NativeTextExtractionError(RuntimeError):
    """Raised when native text extraction fails fatally."""


class NativeTextExtractor:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.logger = get_logger(__name__)

    def extract(self, pdf_path: Path) -> NativeExtractionResult:
        if not pdf_path.exists():
            raise NativeTextExtractionError(f"PDF not found: {pdf_path}")

        warnings: list[ProcessingIssue] = []
        errors: list[ProcessingIssue] = []
        pages: list[NativePageMetrics] = []
        blocks: list[NativeTextBlock] = []

        try:
            doc = fitz.open(pdf_path)
        except Exception as exc:
            raise NativeTextExtractionError(f"Failed to open PDF: {pdf_path}") from exc

        try:
            for page_idx in range(len(doc)):
                page = doc.load_page(page_idx)
                page_no = page_idx + 1
                page_blocks, page_metrics, page_warnings = self._extract_page(page=page, page_no=page_no)
                blocks.extend(page_blocks)
                pages.append(page_metrics)
                warnings.extend(page_warnings)
        except Exception as exc:
            errors.append(
                ProcessingIssue(
                    severity=ErrorSeverity.FATAL,
                    reason_code=ReasonCode.PARTIAL_EXTRACTION_FAILURE,
                    message="native text extraction crashed while iterating pages",
                    details={"error": str(exc)},
                )
            )
            raise NativeTextExtractionError("Native text extraction failed during page processing") from exc
        finally:
            doc.close()

        return NativeExtractionResult(pages=pages, blocks=blocks, warnings=warnings, errors=errors)

    def _extract_page(
        self,
        page: fitz.Page,
        page_no: int,
    ) -> tuple[list[NativeTextBlock], NativePageMetrics, list[ProcessingIssue]]:
        page_dict = page.get_text("dict")
        page_area = max(page.rect.width * page.rect.height, 1.0)

        results: list[NativeTextBlock] = []
        warnings: list[ProcessingIssue] = []
        total_chars = 0
        total_block_area = 0.0
        joined_text_parts: list[str] = []

        text_blocks = [b for b in page_dict.get("blocks", []) if b.get("type") == 0]
        for block_idx, block in enumerate(text_blocks, start=1):
            text = self._extract_text_from_block(block)
            raw_text = text
            normalized = normalize_text(text)
            chars = len(normalized)
            ratio = garbled_ratio(raw_text)
            x0, y0, x1, y1 = block.get("bbox", (0.0, 0.0, 0.0, 0.0))

            if x1 <= x0 or y1 <= y0:
                continue

            bbox = BBox(x0=float(x0), y0=float(y0), x1=float(x1), y1=float(y1))
            block_type = self._infer_block_type(normalized, bbox, page.rect.height)
            searchable = chars > 0 and not is_noise_text(normalized)

            block_id = f"p{page_no}_n{block_idx:03d}"
            native_block = NativeTextBlock(
                page=page_no,
                block_id=block_id,
                bbox=bbox,
                text=normalized,
                raw_text=raw_text,
                char_count=chars,
                garbled_ratio=ratio,
                extract_method=ExtractMethod.NATIVE_TEXT,
                searchable=searchable,
                block_type=block_type,
                metadata={
                    "line_count": len(block.get("lines", [])),
                    "empty": chars == 0,
                },
            )
            results.append(native_block)

            total_chars += chars
            total_block_area += bbox.area
            if normalized:
                joined_text_parts.append(normalized)

            if ratio > self.settings.max_garbled_ratio:
                warnings.append(
                    ProcessingIssue(
                        severity=ErrorSeverity.REVIEW,
                        reason_code=ReasonCode.NATIVE_TEXT_GARBLED,
                        message="native block appears garbled",
                        page=page_no,
                        block_id=block_id,
                        details={"garbled_ratio": ratio},
                    )
                )

        page_text = "\n".join(joined_text_parts)
        page_garbled_ratio = garbled_ratio(page_text)
        metrics = NativePageMetrics(
            page=page_no,
            native_text_char_count=total_chars,
            text_block_count=len(results),
            text_coverage=min(1.0, total_block_area / page_area),
            garbled_ratio=page_garbled_ratio,
            empty=total_chars == 0,
        )

        if total_chars == 0:
            warnings.append(
                ProcessingIssue(
                    severity=ErrorSeverity.REVIEW,
                    reason_code=ReasonCode.EMPTY_PAGE,
                    message="no native text extracted from page",
                    page=page_no,
                    details={"text_blocks": len(text_blocks)},
                )
            )

        return results, metrics, warnings

    @staticmethod
    def _extract_text_from_block(block: dict) -> str:
        parts: list[str] = []
        for line in block.get("lines", []):
            for span in line.get("spans", []):
                chunk = str(span.get("text", ""))
                if chunk:
                    parts.append(chunk)
        return "".join(parts)

    @staticmethod
    def _infer_block_type(text: str, bbox: BBox, page_height: float) -> BlockType:
        lower = text.lower()

        if bbox.y1 <= page_height * 0.10 and len(text) <= 60:
            return BlockType.HEADER
        if bbox.y0 >= page_height * 0.90 and len(text) <= 60:
            return BlockType.FOOTER

        if any(token in text for token in ["署名", "記名押印", "印", "捺印", "署名欄"]):
            return BlockType.SIGNATURE_AREA
        if any(token in text for token in ["別紙", "別表"]) and len(text) <= 20:
            return BlockType.OTHER
        if "|" in text or "\t" in text or "  " in text:
            return BlockType.TABLE
        if not lower:
            return BlockType.OTHER

        return BlockType.TEXT
