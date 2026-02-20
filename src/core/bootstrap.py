from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd

from .utils import slugify, utc_now_iso


DEFAULT_COUNTRIES_FALLBACK = [
    ("FRA", "FR", "France"),
    ("USA", "US", "United States"),
    ("CAN", "CA", "Canada"),
    ("GBR", "GB", "United Kingdom"),
    ("DEU", "DE", "Germany"),
    ("ESP", "ES", "Spain"),
    ("ITA", "IT", "Italy"),
    ("BRA", "BR", "Brazil"),
    ("JPN", "JP", "Japan"),
    ("CHN", "CN", "China"),
    ("AUS", "AU", "Australia"),
    ("IND", "IN", "India"),
]

KNOWN_SPORTS = {
    "athletics",
    "swimming",
    "wrestling",
    "football",
    "basketball",
    "cycling",
    "judo",
    "boxing",
    "tennis",
    "rowing",
    "volleyball",
    "handball",
    "rugby",
    "gymnastics",
    "biathlon",
    "skiing",
    "fencing",
    "weightlifting",
    "taekwondo",
}

HEURISTIC_RULES: list[tuple[re.Pattern[str], str, float]] = [
    (re.compile(r"\b(\d{2,4}m|marathon|hurdles|relay|steeplechase)\b", re.I), "Athletics", 0.90),
    (re.compile(r"\b(freestyle|butterfly|breaststroke|backstroke|medley)\b", re.I), "Swimming", 0.90),
    (re.compile(r"\b(greco-roman|freestyle wrestling|wrestling)\b", re.I), "Wrestling", 0.90),
    (re.compile(r"\b(road race|time trial|bmx|track sprint)\b", re.I), "Cycling", 0.85),
    (re.compile(r"\b(single sculls|double sculls|coxless)\b", re.I), "Rowing", 0.85),
    (re.compile(r"\b(sabre|foil|epee)\b", re.I), "Fencing", 0.85),
]


def load_seed_entries(seed_path: Path) -> list[str]:
    entries: list[str] = []
    for raw in seed_path.read_text(encoding="utf-8").splitlines():
        value = raw.strip()
        if value and not value.startswith("#"):
            entries.append(value)
    return entries


def load_mapping_overrides(mapping_path: Path) -> dict[str, Any]:
    if not mapping_path.exists():
        return {"explicit_sports": [], "overrides": {}}
    raw_text = mapping_path.read_text(encoding="utf-8")

    try:
        import yaml

        data = yaml.safe_load(raw_text) or {}
    except Exception:
        data = _parse_simple_mapping_yaml(raw_text)

    explicit = data.get("explicit_sports", []) or []
    overrides = data.get("overrides", {}) or {}
    normalized_overrides = {str(key).strip().lower(): str(value).strip() for key, value in overrides.items()}
    normalized_explicit = [str(item).strip() for item in explicit if str(item).strip()]
    return {"explicit_sports": normalized_explicit, "overrides": normalized_overrides}


def _parse_simple_mapping_yaml(raw_text: str) -> dict[str, Any]:
    data: dict[str, Any] = {"explicit_sports": [], "overrides": {}}
    mode: str | None = None
    for raw_line in raw_text.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped == "explicit_sports:":
            mode = "explicit_sports"
            continue
        if stripped == "overrides:":
            mode = "overrides"
            continue
        if mode == "explicit_sports" and stripped.startswith("- "):
            data["explicit_sports"].append(stripped[2:].strip())
            continue
        if mode == "overrides" and ":" in stripped:
            key, value = stripped.split(":", 1)
            data["overrides"][key.strip()] = value.strip()
    return data


def build_countries_dimension() -> tuple[pd.DataFrame, str]:
    rows: list[dict[str, Any]] = []
    try:
        import pycountry

        for country in pycountry.countries:
            iso3 = getattr(country, "alpha_3", None)
            if not iso3:
                continue
            rows.append(
                {
                    "country_id": iso3,
                    "iso2": getattr(country, "alpha_2", None),
                    "iso3": iso3,
                    "name_en": getattr(country, "name", iso3),
                    "name_fr": None,
                }
            )
        note = f"Loaded {len(rows)} countries from pycountry."
    except Exception:
        for iso3, iso2, name in DEFAULT_COUNTRIES_FALLBACK:
            rows.append(
                {
                    "country_id": iso3,
                    "iso2": iso2,
                    "iso3": iso3,
                    "name_en": name,
                    "name_fr": None,
                }
            )
        note = "pycountry unavailable, loaded fallback country subset."

    frame = pd.DataFrame(rows).drop_duplicates(subset=["country_id"]).sort_values("country_id")
    return frame.reset_index(drop=True), note


def _infer_mapping(entry: str, explicit_sports: set[str], overrides: dict[str, str]) -> tuple[str, str, float, str]:
    normalized = entry.strip().lower()
    if normalized in overrides:
        return "discipline", overrides[normalized], 0.99, "yaml_override"

    if normalized in explicit_sports or normalized in KNOWN_SPORTS:
        sport_title = " ".join(token.capitalize() for token in normalized.split())
        return "sport", sport_title, 1.00, "exact_sport"

    for pattern, sport_name, confidence in HEURISTIC_RULES:
        if pattern.search(entry):
            return "discipline", sport_name, confidence, "heuristic_regex"

    tokens = normalized.split()
    for token in tokens:
        if token in KNOWN_SPORTS:
            sport_title = " ".join(part.capitalize() for part in token.split())
            return "discipline", sport_title, 0.70, "heuristic_token"

    return "sport", entry.strip(), 0.55, "fallback_assume_sport"


def build_sports_and_disciplines(
    seed_entries: list[str],
    mapping_config: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    timestamp = utc_now_iso()
    explicit_sports = {value.lower() for value in mapping_config.get("explicit_sports", [])}
    overrides = mapping_config.get("overrides", {})

    sport_rows: list[dict[str, Any]] = []
    discipline_rows: list[dict[str, Any]] = []
    audit_rows: list[dict[str, Any]] = []

    for entry in seed_entries:
        row_type, mapped_sport, confidence, mapping_source = _infer_mapping(entry, explicit_sports, overrides)
        sport_name = mapped_sport.strip()
        sport_id = slugify(sport_name)

        sport_rows.append(
            {
                "sport_id": sport_id,
                "sport_name": sport_name,
                "sport_slug": sport_id,
                "created_at_utc": timestamp,
            }
        )

        if row_type == "discipline":
            discipline_name = entry.strip()
            discipline_id = slugify(discipline_name)
            discipline_rows.append(
                {
                    "discipline_id": discipline_id,
                    "discipline_name": discipline_name,
                    "discipline_slug": discipline_id,
                    "sport_id": sport_id,
                    "confidence": confidence,
                    "mapping_source": mapping_source,
                    "created_at_utc": timestamp,
                }
            )

        audit_rows.append(
            {
                "seed_entry": entry,
                "resolved_type": row_type,
                "mapped_sport": sport_name,
                "sport_id": sport_id,
                "confidence": confidence,
                "mapping_source": mapping_source,
            }
        )

    sports_df = (
        pd.DataFrame(sport_rows)
        .drop_duplicates(subset=["sport_id"])
        .sort_values("sport_name")
        .reset_index(drop=True)
    )
    disciplines_df = (
        pd.DataFrame(discipline_rows)
        .drop_duplicates(subset=["discipline_id"])
        .sort_values(["sport_id", "discipline_name"])
        .reset_index(drop=True)
    )
    audit_df = pd.DataFrame(audit_rows).sort_values("seed_entry").reset_index(drop=True)
    return sports_df, disciplines_df, audit_df
