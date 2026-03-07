from __future__ import annotations

import inspect
from collections.abc import Sequence
from typing import Any

from contract_ingest.config import Settings, get_settings
from contract_ingest.domain.enums import BlockType, ErrorSeverity, ExtractMethod, ReasonCode
from contract_ingest.domain.models import BBox, OCRBlock, OCRExtractionResult, OCRRequest, ProcessingIssue
from contract_ingest.extract.ocr_base import OCREngineAdapter, OCRInitializationError
from contract_ingest.utils.logging import get_logger
from contract_ingest.utils.text import is_noise_text, normalize_text


class PaddleOCREngine(OCREngineAdapter):
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.logger = get_logger(__name__)
        self._ocr = self._init_engine()

    @property
    def engine_name(self) -> str:
        return self.settings.ocr_engine_name

    def extract_regions(self, requests: Sequence[OCRRequest]) -> OCRExtractionResult:
        blocks: list[OCRBlock] = []
        issues: list[ProcessingIssue] = []

        for request in requests:
            try:
                raw_result = self._ocr.ocr(request.image, cls=self.settings.ocr_use_angle_cls)
            except Exception as exc:
                issues.append(
                    ProcessingIssue(
                        severity=ErrorSeverity.RECOVERABLE,
                        reason_code=ReasonCode.OCR_FAILURE,
                        message="PaddleOCR region processing failed",
                        page=request.page,
                        details={"region_id": request.region_id, "error": str(exc)},
                    )
                )
                continue

            parsed = self._parse_result(request=request, raw_result=raw_result)
            blocks.extend(parsed)

            if not parsed:
                issues.append(
                    ProcessingIssue(
                        severity=ErrorSeverity.REVIEW,
                        reason_code=ReasonCode.PARTIAL_EXTRACTION_FAILURE,
                        message="OCR returned no text for target region",
                        page=request.page,
                        details={"region_id": request.region_id},
                    )
                )

        return OCRExtractionResult(blocks=blocks, issues=issues)

    def _init_engine(self) -> Any:
        try:
            from paddleocr import PaddleOCR
        except Exception as exc:
            raise OCRInitializationError(
                "Failed to import paddleocr. Install dependency to use OCR fallback."
            ) from exc

        kwargs: dict[str, Any] = {
            "use_angle_cls": self.settings.ocr_use_angle_cls,
            "lang": self.settings.ocr_lang,
            "show_log": False,
        }

        signature = inspect.signature(PaddleOCR)
        if "use_gpu" in signature.parameters:
            kwargs["use_gpu"] = self.settings.ocr_use_gpu

        try:
            return PaddleOCR(**kwargs)
        except Exception as exc:
            raise OCRInitializationError("Failed to initialize PaddleOCR engine") from exc

    def _parse_result(self, request: OCRRequest, raw_result: Any) -> list[OCRBlock]:
        lines = self._normalize_lines(raw_result)
        blocks: list[OCRBlock] = []

        for idx, item in enumerate(lines, start=1):
            if not isinstance(item, list) or len(item) < 2:
                continue

            polygon_raw = item[0]
            text_conf = item[1]
            if not isinstance(text_conf, (list, tuple)) or len(text_conf) < 1:
                continue

            text = normalize_text(str(text_conf[0]))
            if not text:
                continue

            confidence = None
            if len(text_conf) >= 2:
                try:
                    confidence = max(0.0, min(float(text_conf[1]), 1.0))
                except (TypeError, ValueError):
                    confidence = None

            bbox_local = self._polygon_to_bbox(polygon_raw)
            if bbox_local is None:
                continue

            bbox = BBox(
                x0=request.bbox.x0 + bbox_local.x0,
                y0=request.bbox.y0 + bbox_local.y0,
                x1=request.bbox.x0 + bbox_local.x1,
                y1=request.bbox.y0 + bbox_local.y1,
            )
            block_id = f"p{request.page}_o_{request.region_id}_{idx:03d}"

            blocks.append(
                OCRBlock(
                    page=request.page,
                    block_id=block_id,
                    region_id=request.region_id,
                    bbox=bbox,
                    text=text,
                    confidence=confidence,
                    engine=self.engine_name,
                    extract_method=ExtractMethod.OCR,
                    block_type=BlockType.TEXT,
                    searchable=not is_noise_text(text),
                    metadata={"region_id": request.region_id},
                )
            )

        return blocks

    @staticmethod
    def _normalize_lines(raw_result: Any) -> list[Any]:
        if raw_result is None:
            return []
        if isinstance(raw_result, list) and raw_result and isinstance(raw_result[0], list):
            first = raw_result[0]
            if first and isinstance(first[0], list) and len(first[0]) >= 2:
                return first
            return raw_result
        return []

    @staticmethod
    def _polygon_to_bbox(polygon_raw: Any) -> BBox | None:
        if not isinstance(polygon_raw, (list, tuple)):
            return None

        points: list[tuple[float, float]] = []
        for point in polygon_raw:
            if not isinstance(point, (list, tuple)) or len(point) < 2:
                continue
            try:
                points.append((float(point[0]), float(point[1])))
            except (TypeError, ValueError):
                continue

        if not points:
            return None

        xs = [pt[0] for pt in points]
        ys = [pt[1] for pt in points]
        x0, y0, x1, y1 = min(xs), min(ys), max(xs), max(ys)
        if x1 <= x0 or y1 <= y0:
            return None
        return BBox(x0=x0, y0=y0, x1=x1, y1=y1)
