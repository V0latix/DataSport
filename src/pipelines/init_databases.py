from __future__ import annotations

import argparse
from pathlib import Path

from src.core.multi_db import (
    build_multi_database_architecture,
    export_architecture_csv,
    write_architecture_json,
)


ROOT_DIR = Path(__file__).resolve().parents[2]


def main() -> None:
    parser = argparse.ArgumentParser(description="Create and sync multi-base CSV architecture from master DB.")
    parser.add_argument("--processed-dir", default=str(ROOT_DIR / "data/processed"))
    parser.add_argument("--master-db", default=str(ROOT_DIR / "data/processed/sports_nations.db"))
    parser.add_argument("--meta-dir", default=str(ROOT_DIR / "meta"))
    parser.add_argument("--exports-dir", default=str(ROOT_DIR / "exports" / "architecture"))
    args = parser.parse_args()

    processed_dir = Path(args.processed_dir)
    master_db = Path(args.master_db)
    meta_dir = Path(args.meta_dir)
    exports_dir = Path(args.exports_dir)

    payload = build_multi_database_architecture(processed_dir, master_db_path=master_db)
    write_architecture_json(payload, meta_dir / "database_architecture.json")
    csv_path = export_architecture_csv(payload, exports_dir)

    print("[init_databases] multi-base CSV architecture ready")
    print(f"[init_databases] master: {payload['master_db']}")
    print(f"[init_databases] competition csv base: {payload['databases']['competition']['path']}")
    print(f"[init_databases] lineage csv base: {payload['databases']['lineage']['path']}")
    print(f"[init_databases] architecture csv: {csv_path}")


if __name__ == "__main__":
    main()
