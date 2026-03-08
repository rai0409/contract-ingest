from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import fitz

from contract_ingest.config import Settings, get_settings
from contract_ingest.domain.models import BBox
from contract_ingest.utils.image import render_page_to_array
from contract_ingest.utils.logging import get_logger
from contract_ingest.utils.text import normalize_text


@dataclass(frozen=True)
class PPStructureBlock:
    page: int
    bbox: BBox
    label: str
    text: str | None
    score: float | None
    order_hint: int


@dataclass(frozen=True)
class PPStructureLayoutResult:
    blocks: list[PPStructureBlock]
    issues: list[str] = field(default_factory=list)


class PPStructureLayoutAdapter:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.logger = get_logger(__name__)
        self._engine: Any | None = None

    def analyze_pdf(self, pdf_path: Path) -> PPStructureLayoutResult:
        if not pdf_path.exists() or not pdf_path.is_file():
            return PPStructureLayoutResult(blocks=[], issues=[f"pdf not found: {pdf_path}"])

        try:
            engine = self._get_engine()
        except Exception as exc:
            return PPStructureLayoutResult(blocks=[], issues=[f"ppstructure init failed: {exc}"])

        blocks: list[PPStructureBlock] = []
        issues: list[str] = []

        try:
            doc = fitz.open(pdf_path)
        except Exception as exc:
            return PPStructureLayoutResult(blocks=[], issues=[f"failed to open pdf: {exc}"])

        try:
            for page_idx in range(len(doc)):
                page = doc.load_page(page_idx)
                page_no = page_idx + 1
                image = render_page_to_array(page=page, dpi=self.settings.render_dpi)
                try:
                    raw_result = engine(image)
                except Exception as exc:
                    issues.append(f"ppstructure page={page_no} failed: {exc}")
                    continue
                blocks.extend(self._parse_page_output(page_no=page_no, raw_result=raw_result))
        finally:
            doc.close()

        sorted_blocks = sorted(blocks, key=lambda b: (b.page, b.order_hint, b.bbox.y0, b.bbox.x0))
        return PPStructureLayoutResult(blocks=sorted_blocks, issues=issues)

    def _get_engine(self) -> Any:
        if self._engine is not None:
            return self._engine

        try:
            from paddleocr import PPStructure  # type: ignore
        except Exception as exc:
            raise RuntimeError("paddleocr PPStructure is unavailable") from exc

        kwargs: dict[str, Any] = {
            "show_log": False,
            "recovery": True,
            "lang": self.settings.ocr_lang,
        }
        try:
            self._engine = PPStructure(**kwargs)
        except Exception as exc:
            raise RuntimeError("failed to initialize PPStructure") from exc
        return self._engine

    def _parse_page_output(self, page_no: int, raw_result: Any) -> list[PPStructureBlock]:
        entries = raw_result if isinstance(raw_result, list) else []
        blocks: list[PPStructureBlock] = []

        for idx, entry in enumerate(entries, start=1):
            if not isinstance(entry, dict):
                continue
            bbox = self._to_bbox(entry.get("bbox"))
            if bbox is None:
                continue
            label = normalize_text(str(entry.get("type") or entry.get("label") or "unknown"))
            text = self._extract_text(entry)
            score = self._to_score(entry.get("score"))
            blocks.append(
                PPStructureBlock(
                    page=page_no,
                    bbox=bbox,
                    label=label,
                    text=text,
                    score=score,
                    order_hint=idx,
                )
            )
        return blocks

    @staticmethod
    def _extract_text(entry: dict[str, Any]) -> str | None:
        if "text" in entry and entry["text"] is not None:
            text = normalize_text(str(entry["text"]))
            return text if text else None

        res = entry.get("res")
        if not isinstance(res, list):
            return None

        parts: list[str] = []
        for item in res:
            if not isinstance(item, dict):
                continue
            text = item.get("text")
            if text is None:
                continue
            normalized = normalize_text(str(text))
            if normalized:
                parts.append(normalized)

        if not parts:
            return None
        return "\n".join(parts)

    @staticmethod
    def _to_score(raw_score: Any) -> float | None:
        try:
            if raw_score is None:
                return None
            score = float(raw_score)
            if score < 0.0:
                return 0.0
            if score > 1.0:
                return 1.0
            return score
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_bbox(raw_bbox: Any) -> BBox | None:
        if not isinstance(raw_bbox, (list, tuple)):
            return None

        if len(raw_bbox) == 4 and all(isinstance(item, (int, float)) for item in raw_bbox):
            x0, y0, x1, y1 = (float(raw_bbox[0]), float(raw_bbox[1]), float(raw_bbox[2]), float(raw_bbox[3]))
            if x1 <= x0 or y1 <= y0:
                return None
            return BBox(x0=x0, y0=y0, x1=x1, y1=y1)

        points: list[tuple[float, float]] = []
        for point in raw_bbox:
            if not isinstance(point, (list, tuple)) or len(point) < 2:
                continue
            try:
                points.append((float(point[0]), float(point[1])))
            except (TypeError, ValueError):
                continue

        if not points:
            return None
        x0 = min(pt[0] for pt in points)
        y0 = min(pt[1] for pt in points)
        x1 = max(pt[0] for pt in points)
        y1 = max(pt[1] for pt in points)
        if x1 <= x0 or y1 <= y0:
            return None
        return BBox(x0=x0, y0=y0, x1=x1, y1=y1)
