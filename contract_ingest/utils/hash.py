from __future__ import annotations

import hashlib
from pathlib import Path



def sha256_bytes(data: bytes) -> str:
    digest = hashlib.sha256(data).hexdigest()
    return f"sha256:{digest}"



def sha256_text(text: str, encoding: str = "utf-8") -> str:
    return sha256_bytes(text.encode(encoding))



def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as fp:
        while True:
            chunk = fp.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return f"sha256:{hasher.hexdigest()}"
