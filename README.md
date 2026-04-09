# Contract PDF Extraction for Japanese Business Documents

Structure-aware contract ingestion for Japanese business PDFs, with native-text-first parsing and OCR fallback.

## What this repository is

`contract-ingest` is a review-oriented extraction pipeline for turning contract PDFs into structured outputs with evidence references.

It is designed for:

- contract ingestion support,
- legal/business review support,
- internal document automation,
- PDF-to-JSON / CSV preprocessing.

## Current strengths

- Clause/section recovery that is more robust than naive page-text parsing.
- Native text first, with OCR fallback for scanned or mixed-quality PDFs.
- Review-aware field extraction (the pipeline surfaces ambiguity instead of forcing overconfident outputs).
- Evidence-friendly outputs (`evidence_refs`, issue details) for downstream audit and triage.

## Extracted field coverage

Current normalized extraction includes:

- contract type
- counterparties
- effective date
- expiration date
- auto-renewal
- termination notice period
- governing law
- jurisdiction

## Review philosophy

This project prioritizes safe reviewability over optimistic automation.

Typical review signals include:

- missing required fields,
- low-quality field candidates,
- low confidence,
- unstable clause split,
- section boundary uncertainty.

## Baseline evaluation foundation

Baseline artifacts are included for reproducible evaluation setup:

- manifest schema: `data/baseline_manifest.jsonl`
- baseline evaluator: `scripts/eval_baseline_contracts.py`
- metric definitions: `docs/evaluation/baseline_metrics.md`
- failure taxonomy: `docs/evaluation/failure_taxonomy.md`

Run baseline manifest validation:

```bash
source .venv/bin/activate
python scripts/eval_baseline_contracts.py --manifest data/baseline_manifest.jsonl
```

## Current limitations

- Document coverage is strongest for Japanese business contracts; broader document classes may require additional tuning.
- Ambiguous cases still require human review.
- Public, adjudicated benchmark coverage is still limited (foundation is present; dataset growth is ongoing).
- The project is actively evolving; behavior can improve as layout and extraction heuristics are refined.

## Quick start

```bash
uv sync
uv run pytest
```

## Demo

![Input PDF to structured output](docs/images/input_to_output.png)

## Stack

Python, Pydantic, PyMuPDF, NumPy, PaddleOCR, PaddlePaddle

## License

This repository is source-available for personal study, research, and evaluation.  
Commercial use requires prior written permission and a separate paid license.  
See `LICENSE` for details.
