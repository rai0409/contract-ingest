# Contract PDF Extraction for Japanese Business Documents

Extract structured JSON / CSV from business PDFs with native-text-first parsing and OCR fallback.

## What this solves

Many document workflows break on scanned PDFs, mixed native/OCR text, unstable layouts, and weak clause recovery.

This project focuses on making extraction from contracts and business PDFs more stable and easier to integrate into downstream review and automation workflows.

## Best fit use cases

- contract review support
- PDF-to-JSON / CSV extraction
- OCR-backed business document pipelines
- structure-aware preprocessing for internal tools
- Japanese document processing workflows

## What it does

- parses contract PDFs with native text extraction first
- falls back to OCR when necessary
- recovers document structure for downstream extraction
- improves clause and field stability for review-oriented processing
- produces outputs suitable for JSON / CSV workflows

## Demo

![Input PDF to structured output](docs/images/input_to_output.png)

## Sample output

```json
{
  "document_type": "nda",
  "parties": [
    "Company A",
    "Company B"
  ],
  "clauses": [
    {
      "title": "Confidential Information",
      "text": "..."
    },
    {
      "title": "Use Restrictions",
      "text": "..."
    }
  ]
}
```

## Quick start

```bash
uv sync
uv run pytest
```

## Notes

This repository is a good fit for teams that need:

- contract ingestion
- OCR-backed PDF workflows
- structure-aware extraction
- review support before manual approval

## Stack

Python, Pydantic, PyMuPDF, NumPy, PaddleOCR, PaddlePaddle

## License

This repository is source-available for personal study, research, and evaluation.  
Commercial use requires prior written permission and a separate paid license.  
See `LICENSE` for details.
