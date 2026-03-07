from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    pipeline_version: str = "0.1.0"
    ocr_engine_name: str = "paddleocr"
    ocr_lang: str = "japan"
    ocr_use_angle_cls: bool = True
    ocr_use_gpu: bool = False

    low_confidence_threshold: float = 0.65
    high_ocr_ratio_threshold: float = 0.40
    min_native_text_chars: int = 25
    min_block_text_chars: int = 8
    max_garbled_ratio: float = 0.20
    min_clause_text_chars: int = 20

    output_indent: int = 2
    render_dpi: int = 220

    default_output_dir: Path = Path("./output")



def _bool_from_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    return value in {"1", "true", "yes", "y", "on"}



def _float_from_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default



def _int_from_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings(
        pipeline_version=os.getenv("PIPELINE_VERSION", Settings.pipeline_version),
        ocr_engine_name=os.getenv("OCR_ENGINE_NAME", Settings.ocr_engine_name),
        ocr_lang=os.getenv("OCR_LANG", Settings.ocr_lang),
        ocr_use_angle_cls=_bool_from_env("OCR_USE_ANGLE_CLS", Settings.ocr_use_angle_cls),
        ocr_use_gpu=_bool_from_env("OCR_USE_GPU", Settings.ocr_use_gpu),
        low_confidence_threshold=_float_from_env(
            "LOW_CONFIDENCE_THRESHOLD", Settings.low_confidence_threshold
        ),
        high_ocr_ratio_threshold=_float_from_env(
            "HIGH_OCR_RATIO_THRESHOLD", Settings.high_ocr_ratio_threshold
        ),
        min_native_text_chars=_int_from_env(
            "MIN_NATIVE_TEXT_CHARS", Settings.min_native_text_chars
        ),
        min_block_text_chars=_int_from_env("MIN_BLOCK_TEXT_CHARS", Settings.min_block_text_chars),
        max_garbled_ratio=_float_from_env("MAX_GARBLED_RATIO", Settings.max_garbled_ratio),
        min_clause_text_chars=_int_from_env("MIN_CLAUSE_TEXT_CHARS", Settings.min_clause_text_chars),
        output_indent=_int_from_env("OUTPUT_INDENT", Settings.output_indent),
        render_dpi=_int_from_env("RENDER_DPI", Settings.render_dpi),
        default_output_dir=Path(os.getenv("DEFAULT_OUTPUT_DIR", str(Settings.default_output_dir))),
    )
