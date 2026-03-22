#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "usage: bash scripts/check_quality_outputs.sh <output-dir> [<output-dir> ...]" >&2
  exit 1
fi

for output_dir in "$@"; do
  document_json="$output_dir/document.json"
  review_json="$output_dir/review.json"

  if [ ! -f "$document_json" ]; then
    echo "[error] missing document.json: $document_json" >&2
    exit 1
  fi
  if [ ! -f "$review_json" ]; then
    echo "[error] missing review.json: $review_json" >&2
    exit 1
  fi

  echo "== $output_dir =="
  echo "-- review.json.items --"
  jq '.items' "$review_json"
  echo "-- document.json warnings/errors/fields --"
  jq '{
    warnings: .warnings,
    errors: .errors,
    fields: {
      effective_date: .fields.effective_date,
      governing_law: .fields.governing_law,
      jurisdiction: .fields.jurisdiction,
      counterparties: .fields.counterparties
    }
  }' "$document_json"
done
