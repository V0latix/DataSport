from __future__ import annotations

from pathlib import Path

import pandas as pd

from .db import SQLiteDB


def _single_count(db: SQLiteDB, query: str) -> int:
    with db.connect() as conn:
        cursor = conn.execute(query)
        return int(cursor.fetchone()[0])


def run_fk_integrity_checks(db: SQLiteDB) -> list[dict[str, object]]:
    checks: list[tuple[str, str]] = [
        (
            "disciplines.sport_id exists",
            """
            SELECT COUNT(*)
            FROM disciplines d
            LEFT JOIN sports s ON s.sport_id = d.sport_id
            WHERE s.sport_id IS NULL
            """,
        ),
        (
            "competitions.sport_id exists",
            """
            SELECT COUNT(*)
            FROM competitions c
            LEFT JOIN sports s ON s.sport_id = c.sport_id
            WHERE s.sport_id IS NULL
            """,
        ),
        (
            "events.competition_id exists",
            """
            SELECT COUNT(*)
            FROM events e
            LEFT JOIN competitions c ON c.competition_id = e.competition_id
            WHERE c.competition_id IS NULL
            """,
        ),
        (
            "results.event_id exists",
            """
            SELECT COUNT(*)
            FROM results r
            LEFT JOIN events e ON e.event_id = r.event_id
            WHERE e.event_id IS NULL
            """,
        ),
        (
            "results.participant_id exists",
            """
            SELECT COUNT(*)
            FROM results r
            LEFT JOIN participants p ON p.participant_id = r.participant_id
            WHERE p.participant_id IS NULL
            """,
        ),
    ]
    output: list[dict[str, object]] = []
    for name, query in checks:
        invalid_rows = _single_count(db, query)
        output.append({"check": name, "invalid_rows": invalid_rows, "ok": invalid_rows == 0})
    return output


def run_sanity_checks(db: SQLiteDB) -> list[dict[str, object]]:
    checks: list[tuple[str, str]] = [
        ("results.rank >= 1 or null", "SELECT COUNT(*) FROM results WHERE rank IS NOT NULL AND rank < 1"),
        (
            "participants.country_id exists or null",
            """
            SELECT COUNT(*)
            FROM participants p
            LEFT JOIN countries c ON c.country_id = p.country_id
            WHERE p.country_id IS NOT NULL AND c.country_id IS NULL
            """,
        ),
    ]
    output: list[dict[str, object]] = []
    for name, query in checks:
        invalid_rows = _single_count(db, query)
        output.append({"check": name, "invalid_rows": invalid_rows, "ok": invalid_rows == 0})
    return output


def run_all_checks(db_path: Path) -> dict[str, object]:
    db = SQLiteDB(db_path)
    fk_checks = run_fk_integrity_checks(db)
    sanity_checks = run_sanity_checks(db)
    all_checks = fk_checks + sanity_checks
    failed = [check for check in all_checks if not check["ok"]]
    return {
        "db_path": str(db_path),
        "checks_total": len(all_checks),
        "checks_failed": len(failed),
        "passed": len(failed) == 0,
        "checks": all_checks,
    }


def checks_as_frame(check_results: dict[str, object]) -> pd.DataFrame:
    return pd.DataFrame(check_results["checks"])

