from __future__ import annotations

from typing import Iterable

import fitz
import numpy as np

from contract_ingest.domain.models import BBox



def pixmap_to_array(pixmap: fitz.Pixmap) -> np.ndarray:
    channels = pixmap.n
    data = np.frombuffer(pixmap.samples, dtype=np.uint8)
    array = data.reshape(pixmap.height, pixmap.width, channels)
    if channels >= 3:
        return array[:, :, :3]
    return np.repeat(array, 3, axis=2)



def render_page_to_array(page: fitz.Page, dpi: int = 220) -> np.ndarray:
    zoom = dpi / 72.0
    matrix = fitz.Matrix(zoom, zoom)
    pixmap = page.get_pixmap(matrix=matrix, alpha=False)
    return pixmap_to_array(pixmap)



def estimate_page_image_coverage(page: fitz.Page) -> float:
    page_rect = page.rect
    page_area = max(page_rect.width * page_rect.height, 1.0)
    text_dict = page.get_text("dict")

    total = 0.0
    for block in text_dict.get("blocks", []):
        if block.get("type") != 1:
            continue
        x0, y0, x1, y1 = block.get("bbox", (0.0, 0.0, 0.0, 0.0))
        width = max(0.0, float(x1) - float(x0))
        height = max(0.0, float(y1) - float(y0))
        total += width * height

    return min(1.0, total / page_area)



def clip_bbox_to_rect(bbox: BBox, rect: fitz.Rect) -> BBox:
    x0 = max(bbox.x0, rect.x0)
    y0 = max(bbox.y0, rect.y0)
    x1 = min(bbox.x1, rect.x1)
    y1 = min(bbox.y1, rect.y1)
    if x1 <= x0 or y1 <= y0:
        raise ValueError("bbox is outside page rect")
    return BBox(x0=x0, y0=y0, x1=x1, y1=y1)



def pdf_bbox_to_image_bbox(
    bbox: BBox,
    page_rect: fitz.Rect,
    image_width: int,
    image_height: int,
) -> tuple[int, int, int, int]:
    sx = image_width / max(page_rect.width, 1.0)
    sy = image_height / max(page_rect.height, 1.0)

    x0 = int(max(0, min(image_width, (bbox.x0 - page_rect.x0) * sx)))
    y0 = int(max(0, min(image_height, (bbox.y0 - page_rect.y0) * sy)))
    x1 = int(max(0, min(image_width, (bbox.x1 - page_rect.x0) * sx)))
    y1 = int(max(0, min(image_height, (bbox.y1 - page_rect.y0) * sy)))

    if x1 <= x0 or y1 <= y0:
        raise ValueError("converted image bbox is empty")
    return x0, y0, x1, y1



def crop_image_by_pdf_bbox(page_image: np.ndarray, page_rect: fitz.Rect, bbox: BBox) -> np.ndarray:
    h, w = page_image.shape[:2]
    x0, y0, x1, y1 = pdf_bbox_to_image_bbox(bbox=bbox, page_rect=page_rect, image_width=w, image_height=h)
    return page_image[y0:y1, x0:x1].copy()



def merge_bboxes(bboxes: Iterable[BBox]) -> BBox:
    items = list(bboxes)
    if not items:
        raise ValueError("cannot merge empty bbox list")
    return BBox(
        x0=min(b.x0 for b in items),
        y0=min(b.y0 for b in items),
        x1=max(b.x1 for b in items),
        y1=max(b.y1 for b in items),
    )
