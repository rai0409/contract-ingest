from __future__ import annotations

import argparse
import json
from pathlib import Path

from contract_ingest.config import get_settings
from contract_ingest.extract.layout import LayoutAnalyzer
from contract_ingest.extract.layout_ppstructure import PPStructureLayoutAdapter
from contract_ingest.extract.native_text import NativeTextExtractor
from contract_ingest.extract.pdf_classifier import PDFClassifier

from contract_ingest.benchmarks.surya_runner import SuryaRunner


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare layout extraction across engines")
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

    classifier = PDFClassifier(settings)
    native_extractor = NativeTextExtractor(settings)
    layout_analyzer = LayoutAnalyzer(settings)

    classification = classifier.classify(pdf_path)
    native_result = native_extractor.extract(pdf_path)
    current_layout = layout_analyzer.analyze(pdf_path=pdf_path, classification=classification, native_result=native_result)

    pp_adapter = PPStructureLayoutAdapter(settings)
    pp_result = pp_adapter.analyze_pdf(pdf_path)

    surya_result = SuryaRunner().run_layout(pdf_path)

    payload = {
        "pdf": str(pdf_path),
        "current": {
            "page_count": len(current_layout.pages),
            "region_count": sum(len(page.ocr_regions) for page in current_layout.pages),
            "issue_count": len(current_layout.issues),
        },
        "ppstructure": {
            "block_count": len(pp_result.blocks),
            "issue_count": len(pp_result.issues),
            "issues": pp_result.issues,
        },
        "surya": {
            "success": surya_result.success,
            "issue_count": len(surya_result.issues),
            "issues": surya_result.issues,
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
