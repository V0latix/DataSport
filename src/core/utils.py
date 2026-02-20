from __future__ import annotations

import hashlib
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


SLUG_PATTERN = re.compile(r"[^a-z0-9]+")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def utc_now_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def slugify(value: str) -> str:
    lowered = value.strip().lower()
    slug = SLUG_PATTERN.sub("-", lowered).strip("-")
    return slug or "unknown"


def stable_sha1(*parts: object) -> str:
    payload = "|".join(str(part) for part in parts)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def stable_id(prefix: str, *parts: object) -> str:
    return f"{prefix}_{stable_sha1(*parts)}"


def safe_mkdir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def git_short_hash() -> Optional[str]:
    try:
        output = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        return None
    return output or None


def optional_iso2_to_iso3(iso2: Optional[str]) -> Optional[str]:
    if not iso2:
        return None
    code = iso2.strip().upper()
    if len(code) != 2:
        return None
    try:
        import pycountry

        country = pycountry.countries.get(alpha_2=code)
        return country.alpha_3 if country else None
    except Exception:
        return None

