from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from .db import SQLiteDB
from .utils import safe_mkdir, utc_now_iso


@dataclass
class DatabaseArchitecture:
    root_dir: Path

    @property
    def master_db(self) -> Path:
        return self.root_dir / "sports_nations.db"

    @property
    def bases_dir(self) -> Path:
        return self.root_dir / "databases"

    @property
    def reference_base(self) -> Path:
        return self.bases_dir / "reference"

    @property
    def competition_base(self) -> Path:
        return self.bases_dir / "competition"

    @property
    def lineage_base(self) -> Path:
        return self.bases_dir / "lineage"


def _cleanup_legacy_db_files(architecture: DatabaseArchitecture) -> None:
    for name in ("reference.db", "competition.db", "lineage.db"):
        legacy_path = architecture.bases_dir / name
        if legacy_path.exists():
            legacy_path.unlink()


def _export_tables_to_csv_base(master: SQLiteDB, base_dir: Path, tables: Iterable[str]) -> dict[str, int]:
    safe_mkdir(base_dir)
    table_counts: dict[str, int] = {}
    for table in tables:
        frame = master.read_table(table)
        frame.to_csv(base_dir / f"{table}.csv", index=False)
        table_counts[table] = len(frame)
    return table_counts


def build_multi_database_architecture(processed_dir: Path, master_db_path: Path | None = None) -> dict[str, object]:
    processed_dir = processed_dir.resolve()
    safe_mkdir(processed_dir)
    architecture = DatabaseArchitecture(root_dir=processed_dir)
    safe_mkdir(architecture.bases_dir)
    _cleanup_legacy_db_files(architecture)

    master_path = master_db_path.resolve() if master_db_path else architecture.master_db
    master = SQLiteDB(master_path)
    master.create_schema()

    reference_tables = ["countries", "sports", "disciplines", "sources", "sport_federations"]
    competition_tables = [
        "countries",
        "sports",
        "disciplines",
        "sources",
        "competitions",
        "events",
        "participants",
        "results",
    ]
    lineage_tables = ["sources", "raw_imports"]

    payload = {
        "generated_at_utc": utc_now_iso(),
        "master_db": str(master_path),
        "format": "csv",
        "databases": {
            "reference": {
                "path": str(architecture.reference_base),
                "tables": reference_tables,
                "rows_synced": _export_tables_to_csv_base(master, architecture.reference_base, reference_tables),
            },
            "competition": {
                "path": str(architecture.competition_base),
                "tables": competition_tables,
                "rows_synced": _export_tables_to_csv_base(master, architecture.competition_base, competition_tables),
            },
            "lineage": {
                "path": str(architecture.lineage_base),
                "tables": lineage_tables,
                "rows_synced": _export_tables_to_csv_base(master, architecture.lineage_base, lineage_tables),
            },
        },
    }
    return payload


def export_architecture_csv(payload: dict[str, object], output_dir: Path) -> Path:
    safe_mkdir(output_dir)
    rows: list[dict[str, object]] = []
    for db_name, db_data in payload["databases"].items():
        base_path = db_data["path"]
        row_counts = db_data["rows_synced"]
        for table_name, count in row_counts.items():
            rows.append(
                {
                    "database": db_name,
                    "base_path": base_path,
                    "table_name": table_name,
                    "rows_synced": count,
                }
            )
    frame = pd.DataFrame(rows).sort_values(["database", "table_name"])
    out_path = output_dir / "database_architecture.csv"
    frame.to_csv(out_path, index=False)
    return out_path


def write_architecture_json(payload: dict[str, object], output_path: Path) -> None:
    safe_mkdir(output_path.parent)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
