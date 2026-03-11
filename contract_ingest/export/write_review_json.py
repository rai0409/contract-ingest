from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from contract_ingest.config import Settings, get_settings
from contract_ingest.domain.schemas import ReviewSchema


class ReviewWriteError(RuntimeError):
    """Raised when review.json validation or output fails."""


class ReviewJsonWriter:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def write(self, output_dir: Path, payload: dict[str, Any]) -> Path:
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "review.json"
        normalized_payload = self._normalize_payload(payload)

        try:
            validated = ReviewSchema.model_validate(normalized_payload)
        except ValidationError as exc:
            raise ReviewWriteError("review.json validation failed") from exc

        output_path.write_text(
            json.dumps(
                validated.model_dump(mode="json"),
                ensure_ascii=False,
                indent=self.settings.output_indent,
            ),
            encoding="utf-8",
        )
        return output_path

    @staticmethod
    def _normalize_payload(payload: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(payload)
        items = normalized.get("items")
        if not isinstance(items, list):
            return normalized
        normalized_items: list[dict[str, Any]] = []
        for raw_item in items:
            if not isinstance(raw_item, dict):
                continue
            item = dict(raw_item)
            reason_codes = item.get("reason_codes")
            if reason_codes is None and isinstance(item.get("code"), str):
                reason_codes = [item["code"]]
            elif isinstance(reason_codes, str):
                reason_codes = [reason_codes]
            elif isinstance(reason_codes, list):
                reason_codes = [str(code) for code in reason_codes if code]
            else:
                reason_codes = []
            item["reason_codes"] = reason_codes
            item.pop("code", None)
            normalized_items.append(item)
        normalized["items"] = normalized_items
        return normalized
