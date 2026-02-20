from __future__ import annotations

import argparse
from pathlib import Path

from src.core.bootstrap import (
    build_countries_dimension,
    build_sports_and_disciplines,
    load_mapping_overrides,
    load_seed_entries,
)
from src.core.db import SQLiteDB
from src.core.metadata import write_build_meta, write_data_dictionary
from src.core.utils import safe_mkdir, stable_id, utc_now_iso
from src.core.validation import run_all_checks


ROOT_DIR = Path(__file__).resolve().parents[2]


def _write_exports(db: SQLiteDB, exports_dir: Path) -> None:
    safe_mkdir(exports_dir)

    for table in ("countries", "sports", "disciplines", "sources", "raw_imports"):
        frame = db.read_table(table)
        if frame.empty:
            continue
        frame.to_csv(exports_dir / f"{table}.csv", index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap countries/sports/disciplines dimensions.")
    parser.add_argument("--db-path", default=str(ROOT_DIR / "data/processed/sports_nations.db"))
    parser.add_argument("--seed-path", default=str(ROOT_DIR / "data/raw/sport_name_seed.txt"))
    parser.add_argument("--mapping-path", default=str(ROOT_DIR / "data/raw/sport_mapping.yaml"))
    parser.add_argument("--exports-dir", default=str(ROOT_DIR / "exports/bootstrap_dimensions"))
    args = parser.parse_args()

    db_path = Path(args.db_path)
    seed_path = Path(args.seed_path)
    mapping_path = Path(args.mapping_path)
    exports_dir = Path(args.exports_dir)
    meta_dir = ROOT_DIR / "meta"

    db = SQLiteDB(db_path)
    db.create_schema()

    countries_df, countries_note = build_countries_dimension()
    mapping = load_mapping_overrides(mapping_path)
    seed_entries = load_seed_entries(seed_path)
    sports_df, disciplines_df, audit_df = build_sports_and_disciplines(seed_entries, mapping)

    db.ensure_source(
        {
            "source_id": "local_seed",
            "source_name": "Local sport_name seed list",
            "source_type": "seed",
            "license_notes": "User-provided seed list for normalization bootstrap.",
            "base_url": str(seed_path),
        }
    )
    db.log_raw_import(
        {
            "import_id": stable_id("import", "local_seed", utc_now_iso()),
            "source_id": "local_seed",
            "fetched_at_utc": utc_now_iso(),
            "raw_path": str(seed_path),
            "status": "success",
            "error": None,
        }
    )

    db.upsert_dataframe("countries", countries_df, ["country_id"])
    db.upsert_dataframe("sports", sports_df, ["sport_id"])
    db.upsert_dataframe("disciplines", disciplines_df, ["discipline_id"])

    _write_exports(db, exports_dir)
    audit_df.to_csv(exports_dir / "discipline_mapping_audit.csv", index=False)

    checks = run_all_checks(db.db_path)
    write_build_meta(
        db,
        meta_dir / "build_meta.json",
        extra={
            "pipeline": "bootstrap_dimensions",
            "countries_note": countries_note,
            "validation_passed": checks["passed"],
        },
    )
    write_data_dictionary(meta_dir / "data_dictionary.md")

    print(f"[bootstrap] DB: {db.db_path}")
    print(f"[bootstrap] {countries_note}")
    print(
        f"[bootstrap] Inserted/updated dimensions: countries={len(countries_df)} "
        f"sports={len(sports_df)} disciplines={len(disciplines_df)}"
    )
    print(f"[bootstrap] Audit: {exports_dir / 'discipline_mapping_audit.csv'}")
    print(f"[bootstrap] Validation passed: {checks['passed']}")


if __name__ == "__main__":
    main()
