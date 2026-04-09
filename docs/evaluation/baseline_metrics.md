# Baseline Metrics

## Purpose

This baseline exists to make extraction quality changes measurable over time for Japanese contract PDFs.  
The goal is not a single accuracy number. The goal is reproducible comparison of:

- structure quality (can clauses/sections be trusted?),
- field quality (are key fields usable?),
- review quality (did the system correctly request human review when needed?).

## Reproducible Entry Point

Use the manifest + evaluator script:

```bash
python scripts/eval_baseline_contracts.py --manifest data/baseline_manifest.jsonl
```

The script separates three states:

1. Manifest/schema validity
2. Extraction execution availability (PDF path readiness)
3. Evaluation availability (whether extracted outputs are present for comparison)

No benchmark score should be reported unless all three are satisfied.

## Metric Categories

### Structural

- `clause_count_error`: absolute difference between expected and extracted clause count.
- `reversed_clause_number_count`: count of `REVERSED_CLAUSE_NUMBER` review signals.
- `section_boundary_uncertain_count`: count of `SECTION_BOUNDARY_UNCERTAIN` review signals.
- `header_footer_leakage_count`: blocks likely leaked from page furniture into clause/body outputs.
- `signature_leakage_count`: signature-area content leaking into main clause/field interpretation.
- `appendix_form_contamination_count`: appendix/form/instruction content contaminating main-contract extraction.

Structural quality is upstream quality. Poor structure directly degrades field precision.

### Field

Tracked fields:

- `governing_law`
- `jurisdiction`
- `effective_date`
- `expiration_date`
- `counterparties`

Comparison buckets:

- `exact`: extracted value matches expected value after normalization.
- `partial`: semantically related but incomplete or weakly aligned match.
- `placeholder` (date fields): unresolved placeholder date text (for example `令和○○年○○月○○日`, `年 月 日`).
- `missing`: no usable value.

For `counterparties`, set-level overlap is treated as partial when not exact.

### Review Quality

- `review_item_count`: total review issues emitted by the pipeline.
- `true_review_needed_count`: review emitted for a case that actually required review.
- `false_clean_count`: pipeline emitted no review where review was actually needed.

`false_clean_count` is the highest-risk outcome in legal extraction because it hides uncertainty.

## Why False Clean Matters More Than Naive Recall

In review-oriented legal extraction, conservative escalation is intentional.  
A model that extracts more fields but silently misses ambiguity is riskier than one that emits review flags.

Operational priority is:

1. avoid false clean,
2. preserve evidence and review context,
3. improve exact match rate without suppressing valid review signals.

## Structural vs Field Interaction

A field mismatch can be downstream evidence of structural failure:

- boundary drift can move governing law/jurisdiction into wrong scope,
- signature/form contamination can produce false dates or parties,
- unstable clause split can reduce retrieval quality for clause-aware ranking.

Baseline analysis should inspect structural and field metrics together, not separately.

## Current Public Baseline Status

This repository now includes a baseline foundation (manifest schema + evaluator).  
Public fixtures are available for dry-run extraction readiness checks, but adjudicated ground truth coverage is still limited.
