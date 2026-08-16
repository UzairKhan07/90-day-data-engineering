"""
Row hashing helpers for idempotent loads.
"""

from __future__ import annotations

import hashlib
from typing import Any, Iterable


def row_hash(values: Iterable[Any]) -> str:
    """
    Create a stable MD5 hash from a sequence of values.
    None is treated as the string 'NULL'.
    """
    parts = []
    for v in values:
        if v is None:
            parts.append("NULL")
        else:
            parts.append(str(v).strip())
    payload = "||".join(parts)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()