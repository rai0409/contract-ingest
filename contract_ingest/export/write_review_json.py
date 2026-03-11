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

        try:
            validated = ReviewSchema.model_validate(payload)
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
