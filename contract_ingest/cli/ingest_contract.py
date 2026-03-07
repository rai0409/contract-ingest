from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from contract_ingest.config import get_settings
from contract_ingest.domain.models import OCRExtractionResult, ProcessingIssue
from contract_ingest.export.write_chunks_jsonl import ChunksJsonlWriter, ChunksWriteError
from contract_ingest.export.write_document_json import DocumentJsonWriter, DocumentWriteError
from contract_ingest.export.write_review_json import ReviewJsonWriter, ReviewWriteError
from contract_ingest.extract.block_merger import BlockMerger
from contract_ingest.extract.layout import LayoutAnalyzer, LayoutAnalyzerError
from contract_ingest.extract.native_text import NativeTextExtractionError, NativeTextExtractor
from contract_ingest.extract.ocr_base import OCRInitializationError
from contract_ingest.extract.ocr_paddle import PaddleOCREngine
from contract_ingest.extract.pdf_classifier import PDFClassificationError, PDFClassifier
from contract_ingest.normalize.chunk_builder import ChunkBuilder
from contract_ingest.normalize.clause_splitter import ClauseSplitter
from contract_ingest.normalize.evidence_builder import EvidenceBuilder
from contract_ingest.normalize.field_extractor import ContractFieldExtractor
from contract_ingest.review.review_queue import ReviewQueueBuilder
from contract_ingest.review.scorer import ReviewScorer
from contract_ingest.utils.hash import sha256_file
from contract_ingest.utils.logging import configure_logging, get_logger


class PipelineFatalError(RuntimeError):
    """Raised when pipeline cannot continue and must exit with code 1."""



def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Contract ingestion CLI")
    parser.add_argument("--input", required=True, type=Path, help="input PDF path")
    parser.add_argument("--output-dir", required=True, type=Path, help="output directory path")
    parser.add_argument("--doc-id", required=False, type=str, default=None, help="optional document id")
    parser.add_argument("--log-level", required=False, type=str, default="INFO", help="log level")
    return parser.parse_args(argv)



def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = get_settings()

    configure_logging(args.log_level)

    input_pdf: Path = args.input
    output_dir: Path = args.output_dir

    if not input_pdf.exists() or not input_pdf.is_file():
        logger = get_logger(__name__)
        logger.error("input file does not exist", extra={"path": str(input_pdf)})
        return 1

    doc_id = args.doc_id or _build_doc_id(input_pdf)
    logger = get_logger(__name__, doc_id=doc_id)

    try:
        source_hash = sha256_file(input_pdf)

        classifier = PDFClassifier(settings)
        native_extractor = NativeTextExtractor(settings)
        layout_analyzer = LayoutAnalyzer(settings)
        merger = BlockMerger(settings)

        evidence_builder = EvidenceBuilder(settings)
        clause_splitter = ClauseSplitter(settings)
        field_extractor = ContractFieldExtractor(settings)
        chunk_builder = ChunkBuilder(settings)

        review_scorer = ReviewScorer(settings)
        review_queue_builder = ReviewQueueBuilder()

        document_writer = DocumentJsonWriter(settings)
        chunks_writer = ChunksJsonlWriter()
        review_writer = ReviewJsonWriter(settings)

        classification = classifier.classify(input_pdf)
        native_result = native_extractor.extract(input_pdf)

        layout_result = layout_analyzer.analyze(
            pdf_path=input_pdf,
            classification=classification,
            native_result=native_result,
        )

        ocr_requests, request_issues = layout_analyzer.build_ocr_requests(
            pdf_path=input_pdf,
            layout_result=layout_result,
        )

        if ocr_requests:
            ocr_engine = PaddleOCREngine(settings)
            raw_ocr_result = ocr_engine.extract_regions(ocr_requests)
            ocr_result = OCRExtractionResult(
                blocks=raw_ocr_result.blocks,
                issues=raw_ocr_result.issues + request_issues,
            )
        else:
            ocr_result = OCRExtractionResult(blocks=[], issues=request_issues)

        merge_result = merger.merge(
            native_result=native_result,
            layout_result=layout_result,
            ocr_result=ocr_result,
        )

        evidence_blocks = evidence_builder.build(
            merge_result=merge_result,
            source_hash=source_hash,
        )

        clause_result = clause_splitter.split(evidence_blocks)
        field_result = field_extractor.extract(evidence_blocks, clause_result.clauses)

        chunk_result = chunk_builder.build(
            doc_id=doc_id,
            clauses=clause_result.clauses,
            evidence_blocks=evidence_blocks,
            fields=field_result.fields,
        )

        all_issues = _collect_issues(
            classification_warnings=classification.warnings,
            native_warnings=native_result.warnings,
            native_errors=native_result.errors,
            layout_issues=layout_result.issues,
            ocr_issues=ocr_result.issues,
            merge_warnings=merge_result.warnings,
            merge_errors=merge_result.errors,
            clause_issues=clause_result.issues,
            field_issues=field_result.issues,
            chunk_issues=chunk_result.issues,
        )

        review_assessment = review_scorer.score(
            issues=all_issues,
            merged_pages=merge_result.pages,
            fields=field_result.fields,
        )
        review_queue = review_queue_builder.build(doc_id=doc_id, assessment=review_assessment)

        document_path = document_writer.write(
            output_dir=output_dir,
            doc_id=doc_id,
            source_file=input_pdf.name,
            document_kind=classification.document_kind,
            source_hash=source_hash,
            merge_result=merge_result,
            evidence_blocks=evidence_blocks,
            clause_result=clause_result,
            field_result=field_result,
            issues=all_issues,
        )
        chunks_path = chunks_writer.write(output_dir=output_dir, chunks=chunk_result.chunks)
        review_path = review_writer.write(output_dir=output_dir, payload=review_queue.payload)

        logger.info(
            "ingestion completed",
            extra={
                "document_path": str(document_path),
                "chunks_path": str(chunks_path),
                "review_path": str(review_path),
                "review_required": review_assessment.review_required,
                "signal_count": len(review_assessment.signals),
            },
        )
        return 0

    except OCRInitializationError as exc:
        logger.error("fatal OCR initialization failure", extra={"error": str(exc)})
        return 1
    except (
        PDFClassificationError,
        NativeTextExtractionError,
        LayoutAnalyzerError,
        DocumentWriteError,
        ChunksWriteError,
        ReviewWriteError,
        PipelineFatalError,
    ) as exc:
        logger.error("fatal pipeline failure", extra={"error": str(exc)})
        return 1
    except Exception as exc:
        logger.error("unexpected fatal pipeline failure", extra={"error": str(exc)})
        return 1



def _build_doc_id(input_pdf: Path) -> str:
    source_hash = sha256_file(input_pdf).replace("sha256:", "")[:12]
    stem = input_pdf.stem.replace(" ", "_")
    return f"{stem}_{source_hash}"



def _collect_issues(
    classification_warnings: list[ProcessingIssue],
    native_warnings: list[ProcessingIssue],
    native_errors: list[ProcessingIssue],
    layout_issues: list[ProcessingIssue],
    ocr_issues: list[ProcessingIssue],
    merge_warnings: list[ProcessingIssue],
    merge_errors: list[ProcessingIssue],
    clause_issues: list[ProcessingIssue],
    field_issues: list[ProcessingIssue],
    chunk_issues: list[ProcessingIssue],
) -> list[ProcessingIssue]:
    issues = [
        *classification_warnings,
        *native_warnings,
        *native_errors,
        *layout_issues,
        *ocr_issues,
        *merge_warnings,
        *merge_errors,
        *clause_issues,
        *field_issues,
        *chunk_issues,
    ]

    deduped: list[ProcessingIssue] = []
    seen: set[str] = set()

    for issue in issues:
        reason_code = issue.reason_code.value if hasattr(issue.reason_code, "value") else str(issue.reason_code)
        key = json.dumps(
            {
                "severity": issue.severity.value,
                "reason_code": reason_code,
                "message": issue.message,
                "page": issue.page,
                "block_id": issue.block_id,
                "details": issue.details,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(issue)

    return deduped



def main() -> None:
    exit_code = run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
