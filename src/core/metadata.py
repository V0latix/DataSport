from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .db import SQLiteDB
from .utils import git_short_hash, safe_mkdir, utc_now_iso


DATA_DICTIONARY: dict[str, list[tuple[str, str]]] = {
    "countries": [
        ("country_id", "ISO3 code used as stable country key"),
        ("iso2", "ISO2 code"),
        ("iso3", "ISO3 code duplicate for convenience"),
        ("name_en", "Country name in English"),
        ("name_fr", "Country name in French if available"),
    ],
    "sports": [
        ("sport_id", "Slug identifier for sport"),
        ("sport_name", "Human-readable sport name"),
        ("sport_slug", "Slug version of sport name"),
        ("created_at_utc", "UTC timestamp for insertion"),
    ],
    "disciplines": [
        ("discipline_id", "Slug identifier for discipline"),
        ("discipline_name", "Human-readable discipline/event name"),
        ("discipline_slug", "Slug version of discipline name"),
        ("sport_id", "Parent sport foreign key"),
        ("confidence", "Mapping confidence score"),
        ("mapping_source", "Mapping origin (heuristic/override)"),
        ("created_at_utc", "UTC timestamp for insertion"),
    ],
    "competitions": [
        ("competition_id", "Deterministic hashed ID"),
        ("sport_id", "Sport foreign key"),
        ("name", "Competition name"),
        ("season_year", "Season year"),
        ("level", "Competition level/category"),
        ("start_date", "Competition start date"),
        ("end_date", "Competition end date"),
        ("source_id", "Source foreign key"),
    ],
    "events": [
        ("event_id", "Deterministic hashed ID"),
        ("competition_id", "Competition foreign key"),
        ("discipline_id", "Discipline foreign key"),
        ("gender", "Event gender category"),
        ("event_class", "Class/stage of event"),
        ("event_date", "Event date"),
    ],
    "participants": [
        ("participant_id", "Deterministic hashed ID"),
        ("type", "athlete/team/pair"),
        ("display_name", "Display name"),
        ("country_id", "Country foreign key"),
    ],
    "results": [
        ("event_id", "Event foreign key"),
        ("participant_id", "Participant foreign key"),
        ("rank", "Rank/position"),
        ("medal", "Medal label"),
        ("score_raw", "Raw score payload"),
        ("points_awarded", "Points allocated in normalized model"),
    ],
    "sources": [
        ("source_id", "Source key"),
        ("source_name", "Source display name"),
        ("source_type", "API/SPARQL/seed type"),
        ("license_notes", "License context"),
        ("base_url", "Base endpoint"),
    ],
    "raw_imports": [
        ("import_id", "Import operation key"),
        ("source_id", "Source key"),
        ("fetched_at_utc", "Import timestamp UTC"),
        ("raw_path", "Snapshot folder path"),
        ("status", "success/skipped/error"),
        ("error", "Error message if any"),
    ],
    "sport_federations": [
        ("sport_id", "Sport key"),
        ("federation_qid", "Wikidata federation QID"),
        ("federation_name", "Federation label"),
    ],
}


def write_build_meta(db: SQLiteDB, output_path: Path, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    safe_mkdir(output_path.parent)
    payload: dict[str, Any] = {
        "generated_at_utc": utc_now_iso(),
        "git_hash": git_short_hash(),
        "db_path": str(db.db_path),
        "row_counts": db.table_row_counts(),
    }
    if extra:
        payload.update(extra)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return payload


def write_data_dictionary(output_path: Path) -> None:
    safe_mkdir(output_path.parent)
    lines: list[str] = []
    lines.append("# Data Dictionary")
    lines.append("")
    for table_name, columns in DATA_DICTIONARY.items():
        lines.append(f"## {table_name}")
        lines.append("")
        lines.append("| column | description |")
        lines.append("|---|---|")
        for column_name, description in columns:
            lines.append(f"| {column_name} | {description} |")
        lines.append("")
    output_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")

