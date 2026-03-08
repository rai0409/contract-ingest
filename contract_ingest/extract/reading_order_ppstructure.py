from __future__ import annotations

from dataclasses import dataclass

from contract_ingest.domain.models import BBox
from contract_ingest.extract.layout_ppstructure import PPStructureLayoutResult


@dataclass(frozen=True)
class PPStructureReadingItem:
    page: int
    order: int
    bbox: BBox
    label: str
    text: str | None


class PPStructureReadingOrderAdapter:
    def build(self, layout_result: PPStructureLayoutResult) -> list[PPStructureReadingItem]:
        ordered_blocks = sorted(
            layout_result.blocks,
            key=lambda block: (block.page, block.bbox.y0, block.bbox.x0, block.order_hint),
        )

        items: list[PPStructureReadingItem] = []
        for idx, block in enumerate(ordered_blocks, start=1):
            items.append(
                PPStructureReadingItem(
                    page=block.page,
                    order=idx,
                    bbox=block.bbox,
                    label=block.label,
                    text=block.text,
                )
            )
        return items
