from __future__ import annotations

from pathlib import Path

from contract_ingest.cli.ingest_contract import parse_args


def test_layout_engine_default_is_current() -> None:
    args = parse_args(["--input", "in.pdf", "--output-dir", "out"])

    assert args.layout_engine == "current"


def test_layout_engine_accepts_compare_mode() -> None:
    args = parse_args(
        [
            "--input",
            str(Path("in.pdf")),
            "--output-dir",
            str(Path("out")),
            "--layout-engine",
            "compare",
        ]
    )

    assert args.layout_engine == "compare"
