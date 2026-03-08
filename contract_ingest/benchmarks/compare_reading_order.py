from __future__ import annotations

import argparse
import json
from pathlib import Path

from contract_ingest.config import get_settings
from contract_ingest.extract.layout_ppstructure import PPStructureLayoutAdapter
from contract_ingest.extract.native_text import NativeTextExtractor
from contract_ingest.extract.reading_order_ppstructure import PPStructureReadingOrderAdapter

from contract_ingest.benchmarks.surya_runner import SuryaRunner


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare reading order across engines")
    parser.add_argument("--input", required=True, type=Path, help="input PDF path")
    parser.add_argument("--output", required=False, type=Path, default=None, help="optional JSON output path")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = get_settings()

    pdf_path: Path = args.input
    if not pdf_path.exists() or not pdf_path.is_file():
        print(json.dumps({"error": f"pdf not found: {pdf_path}"}, ensure_ascii=False))
        return 1

    native_result = NativeTextExtractor(settings).extract(pdf_path)
    native_order = [
        {
            "page": block.page,
            "block_id": block.block_id,
            "y0": block.bbox.y0,
            "x0": block.bbox.x0,
        }
        for block in sorted(native_result.blocks, key=lambda b: (b.page, b.bbox.y0, b.bbox.x0))
    ]

    pp_layout = PPStructureLayoutAdapter(settings).analyze_pdf(pdf_path)
    pp_order = PPStructureReadingOrderAdapter().build(pp_layout)
    pp_order_payload = [
        {
            "page": item.page,
            "order": item.order,
            "label": item.label,
            "y0": item.bbox.y0,
            "x0": item.bbox.x0,
        }
        for item in pp_order
    ]

    surya_result = SuryaRunner().run_reading_order(pdf_path)

    payload = {
        "pdf": str(pdf_path),
        "current": {
            "count": len(native_order),
            "items": native_order,
        },
        "ppstructure": {
            "count": len(pp_order_payload),
            "issues": pp_layout.issues,
            "items": pp_order_payload,
        },
        "surya": {
            "success": surya_result.success,
            "issues": surya_result.issues,
            "payload": surya_result.payload,
        },
    }

    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
