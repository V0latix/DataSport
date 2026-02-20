from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.core.validation import run_all_checks


ROOT_DIR = Path(__file__).resolve().parents[2]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run FK and sanity checks on SQLite database.")
    parser.add_argument("--db-path", default=str(ROOT_DIR / "data/processed/sports_nations.db"))
    args = parser.parse_args()

    report = run_all_checks(Path(args.db_path))
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

