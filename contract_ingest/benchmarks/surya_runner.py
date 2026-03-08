from __future__ import annotations

from dataclasses import dataclass, field
import importlib
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SuryaRunResult:
    success: bool
    payload: Any | None = None
    issues: list[str] = field(default_factory=list)


class SuryaRunner:
    """Benchmark-only adapter. Not used in production ingestion flow."""

    def run_layout(self, pdf_path: Path) -> SuryaRunResult:
        if not pdf_path.exists() or not pdf_path.is_file():
            return SuryaRunResult(success=False, issues=[f"pdf not found: {pdf_path}"])

        module, issue = self._load_surya_module()
        if module is None:
            return SuryaRunResult(success=False, issues=[issue])

        for fn_name in ["run_layout", "layout", "predict_layout"]:
            fn = getattr(module, fn_name, None)
            if callable(fn):
                try:
                    payload = fn(str(pdf_path))
                    return SuryaRunResult(success=True, payload=payload)
                except Exception as exc:
                    return SuryaRunResult(success=False, issues=[f"surya {fn_name} failed: {exc}"])

        return SuryaRunResult(success=False, issues=["surya layout API not found"])

    def run_reading_order(self, pdf_path: Path) -> SuryaRunResult:
        if not pdf_path.exists() or not pdf_path.is_file():
            return SuryaRunResult(success=False, issues=[f"pdf not found: {pdf_path}"])

        module, issue = self._load_surya_module()
        if module is None:
            return SuryaRunResult(success=False, issues=[issue])

        for fn_name in ["run_reading_order", "reading_order", "predict_reading_order"]:
            fn = getattr(module, fn_name, None)
            if callable(fn):
                try:
                    payload = fn(str(pdf_path))
                    return SuryaRunResult(success=True, payload=payload)
                except Exception as exc:
                    return SuryaRunResult(success=False, issues=[f"surya {fn_name} failed: {exc}"])

        return SuryaRunResult(success=False, issues=["surya reading-order API not found"])

    @staticmethod
    def _load_surya_module() -> tuple[Any | None, str]:
        try:
            module = importlib.import_module("surya")
            return module, ""
        except Exception as exc:
            return None, f"surya unavailable: {exc}"
