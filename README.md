# contract-ingest

Python core for contract PDF ingestion with native-text-first parsing and OCR fallback.

This project is built for document-heavy workflows where scanned PDFs, layout variance, and extraction precision matter. It focuses on structured extraction from contracts and business documents, especially Japanese-language materials.

## What it does

- Parses contract PDFs with native text extraction first
- Falls back to OCR when necessary
- Recovers document structure for downstream extraction workflows
- Produces outputs suitable for review support and structured processing

## Typical use cases

- Contract and business document ingestion
- OCR fallback for scanned PDFs
- Structured extraction to JSON / CSV
- Internal review support workflows
- Japanese document processing pipelines

## Stack

Python, Pydantic, PyMuPDF, NumPy, PaddleOCR, PaddlePaddle

## Why this repo matters

Many document workflows break on scanned PDFs, inconsistent layouts, or mixed native/OCR text. This project is designed to make document ingestion more reliable and easier to integrate into downstream review and automation systems.

## Quick start

```bash
uv sync
uv run pytest
```

Add your project-specific ingestion command or sample script here.

## Notes

This repository is a good fit for teams that need:
- contract ingestion
- OCR-backed PDF workflows
- structure-aware extraction
- review support before manual approval
