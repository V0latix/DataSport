from __future__ import annotations

import argparse
from pathlib import Path

from src.connectors.base import MissingCredentialError
from src.connectors.registry import build_connector
from src.core.db import SQLiteDB
from src.core.metadata import write_build_meta, write_data_dictionary
from src.core.utils import safe_mkdir, stable_id, utc_now_compact, utc_now_iso
from src.core.validation import run_all_checks


ROOT_DIR = Path(__file__).resolve().parents[2]


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest one connector for one season/year.")
    parser.add_argument(
        "--connector",
        required=True,
        help=(
            "wikidata | football_data | balldontlie_nba | fiba_ranking_history | "
            "fiba_basketball_world_cup_history | fifa_ranking_history | fifa_women_ranking_history | "
            "fifa_women_world_cup_history | world_rugby_ranking_history | rugby_world_cup_history | "
            "ihf_handball_world_championship_history | icc_team_ranking_history | "
            "icc_cricket_world_cup_history | world_cup_history | "
            "paris_2024_summer_olympics | olympics_keith_history"
        ),
    )
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--db-path", default=str(ROOT_DIR / "data/processed/sports_nations.db"))
    args = parser.parse_args()

    connector = build_connector(args.connector)
    db = SQLiteDB(Path(args.db_path))
    db.create_schema()
    db.ensure_source(connector.source_row())

    timestamp = utc_now_compact()
    raw_dir = safe_mkdir(ROOT_DIR / "data" / "raw" / connector.id / timestamp)
    export_dir = safe_mkdir(ROOT_DIR / "exports" / connector.id / f"year={args.year}")
    import_id = stable_id("import", connector.id, timestamp, args.year)
    status = "success"
    error_text = None

    try:
        raw_paths = connector.fetch(args.year, raw_dir)
        payload = connector.parse(raw_paths, args.year)
        connector.upsert(db, payload)
        connector.export(payload, export_dir)
    except MissingCredentialError as exc:
        raw_paths = []
        status = "skipped"
        error_text = str(exc)
        print(f"[ingest] {error_text}")
    except Exception as exc:
        raw_paths = []
        status = "error"
        error_text = str(exc)
        print(f"[ingest] ERROR: {error_text}")
        raise
    finally:
        db.log_raw_import(
            {
                "import_id": import_id,
                "source_id": connector.id,
                "fetched_at_utc": utc_now_iso(),
                "raw_path": str(raw_dir),
                "status": status,
                "error": error_text,
            }
        )

    checks = run_all_checks(db.db_path)
    write_build_meta(
        db,
        ROOT_DIR / "meta" / "build_meta.json",
        extra={
            "pipeline": "ingest",
            "connector": connector.id,
            "year": args.year,
            "status": status,
            "validation_passed": checks["passed"],
        },
    )
    write_data_dictionary(ROOT_DIR / "meta" / "data_dictionary.md")

    print(f"[ingest] connector={connector.id} year={args.year} status={status}")
    print(f"[ingest] raw snapshots: {raw_dir}")
    print(f"[ingest] exports: {export_dir}")
    print(f"[ingest] validation passed: {checks['passed']}")


if __name__ == "__main__":
    main()
