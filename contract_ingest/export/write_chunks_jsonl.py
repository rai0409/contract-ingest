from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from contract_ingest.domain.schemas import ChunkSchema


class ChunksWriteError(RuntimeError):
    """Raised when chunks.jsonl validation or output fails."""


class ChunksJsonlWriter:
    def write(self, output_dir: Path, chunks: list[dict[str, Any]]) -> Path:
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "chunks.jsonl"

        lines: list[str] = []
        for idx, chunk in enumerate(chunks):
            try:
                validated = ChunkSchema.model_validate(chunk)
            except ValidationError as exc:
                raise ChunksWriteError(f"chunks.jsonl validation failed at chunk index {idx}") from exc

            lines.append(json.dumps(validated.model_dump(mode="json"), ensure_ascii=False))

        output_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
        return output_path
