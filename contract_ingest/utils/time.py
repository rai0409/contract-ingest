from __future__ import annotations

from datetime import date, datetime, timezone
import re

from contract_ingest.utils.text import normalize_digits

_DATE_RE = re.compile(r"(?P<y>[0-9]{4})年\s*(?P<m>[0-9]{1,2})月\s*(?P<d>[0-9]{1,2})日")



def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()



def parse_japanese_date(text: str) -> date | None:
    normalized = normalize_digits(text)
    match = _DATE_RE.search(normalized)
    if not match:
        return None
    year = int(match.group("y"))
    month = int(match.group("m"))
    day = int(match.group("d"))
    try:
        return date(year, month, day)
    except ValueError:
        return None



def to_iso_date(text: str) -> str | None:
    parsed = parse_japanese_date(text)
    if parsed is None:
        return None
    return parsed.isoformat()
