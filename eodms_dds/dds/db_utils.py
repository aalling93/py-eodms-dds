from __future__ import annotations
from pathlib import Path
from datetime import datetime
import hashlib
import json
from typing import Any, Optional


def to_iso(x: Any) -> Optional[str]:
    if x is None:
        return None
    if isinstance(x, str):
        return x
    if isinstance(x, datetime):
        return x.isoformat()
    return None


def md5_file(path: Path) -> Optional[str]:
    try:
        h = hashlib.md5()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None


def json_dumps_safe(obj: Any) -> str:
    try:
        return json.dumps(obj)
    except Exception:
        return "{}"
