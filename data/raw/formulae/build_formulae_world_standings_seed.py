from __future__ import annotations

import argparse
import re
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests


SEED_PATH = Path(__file__).resolve().parent / "formulae_world_standings_top10_seed.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder)"}

DRIVERS_COMPETITION_ID = "formulae_drivers_world_championship"
TEAMS_COMPETITION_ID = "formulae_teams_world_championship"
DISCIPLINE_ID = "formula-e"
DISCIPLINE_NAME = "Formula E"

SEASON_PAGES: list[dict[str, Any]] = [
    {
        "season_label": "2014-2015",
        "year_end": 2015,
        "url": "https://en.wikipedia.org/wiki/2014%E2%80%9315_Formula_E_Championship",
    },
    {
        "season_label": "2015-2016",
        "year_end": 2016,
        "url": "https://en.wikipedia.org/wiki/2015%E2%80%9316_Formula_E_Championship",
    },
    {
        "season_label": "2016-2017",
        "year_end": 2017,
        "url": "https://en.wikipedia.org/wiki/2016%E2%80%9317_Formula_E_Championship",
    },
    {
        "season_label": "2017-2018",
        "year_end": 2018,
        "url": "https://en.wikipedia.org/wiki/2017%E2%80%9318_Formula_E_Championship",
    },
    {
        "season_label": "2018-2019",
        "year_end": 2019,
        "url": "https://en.wikipedia.org/wiki/2018%E2%80%9319_Formula_E_Championship",
    },
    {
        "season_label": "2019-2020",
        "year_end": 2020,
        "url": "https://en.wikipedia.org/wiki/2019%E2%80%9320_Formula_E_Championship",
    },
    {
        "season_label": "2020-2021",
        "year_end": 2021,
        "url": "https://en.wikipedia.org/wiki/2020%E2%80%9321_Formula_E_World_Championship",
    },
    {
        "season_label": "2021-2022",
        "year_end": 2022,
        "url": "https://en.wikipedia.org/wiki/2021%E2%80%9322_Formula_E_World_Championship",
    },
    {
        "season_label": "2022-2023",
        "year_end": 2023,
        "url": "https://en.wikipedia.org/wiki/2022%E2%80%9323_Formula_E_World_Championship",
    },
    {
        "season_label": "2023-2024",
        "year_end": 2024,
        "url": "https://en.wikipedia.org/wiki/2023%E2%80%9324_Formula_E_World_Championship",
    },
    {
        "season_label": "2024-2025",
        "year_end": 2025,
        "url": "https://en.wikipedia.org/wiki/2024%E2%80%9325_Formula_E_World_Championship",
    },
]


def _clean_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    if text.lower() in {"nan", "none"}:
        return ""
    return text


def _team_key(value: Any) -> str:
    text = _clean_text(value).lower()
    text = re.sub(r"\bformula\s*e\s*team\b", "", text)
    text = re.sub(r"\bformula\s*e\b", "", text)
    text = re.sub(r"\bteam\b", "", text)
    text = re.sub(r"[^a-z0-9]+", "", text)
    return text


def _normalize_position(value: Any) -> int | None:
    text = _clean_text(value)
    if not text:
        return None
    match = re.search(r"\d+", text)
    if not match:
        return None
    try:
        pos = int(match.group(0))
        return pos if pos >= 1 else None
    except Exception:
        return None


def _normalize_points(value: Any) -> float | None:
    text = _clean_text(value).replace(",", ".")
    if not text:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except Exception:
        return None


def _find_points_column(columns: list[str]) -> str | None:
    for candidate in columns:
        low = str(candidate).strip().lower()
        if low in {"pts", "points"}:
            return candidate
    return None


def _extract_tables(url: str) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    html = requests.get(url, headers=HEADERS, timeout=60)
    html.raise_for_status()
    tables = pd.read_html(StringIO(html.text))

    drivers_table: pd.DataFrame | None = None
    teams_table: pd.DataFrame | None = None
    entrants_teams: list[str] = []

    for table in tables:
        columns = [str(col) for col in table.columns]
        if "Team" in columns and "Drivers" in columns:
            teams = [_clean_text(value) for value in table["Team"].tolist()]
            entrants_teams.extend([team for team in teams if team and not team.lower().startswith("source:")])
        has_position = any(str(col).startswith("Pos") for col in columns)
        points_col = _find_points_column(columns)
        if not has_position or points_col is None:
            continue

        if "Driver" in columns and drivers_table is None:
            drivers_table = table.copy()
        if "Team" in columns and teams_table is None:
            teams_table = table.copy()

    if drivers_table is None or teams_table is None:
        raise RuntimeError(f"Could not locate drivers/teams standings tables in {url}")
    entrants_teams = list(dict.fromkeys(entrants_teams))
    return drivers_table, teams_table, entrants_teams


def _build_driver_rows(season: dict[str, Any], table: pd.DataFrame) -> list[dict[str, Any]]:
    columns = [str(col) for col in table.columns]
    pos_col = next(col for col in columns if str(col).startswith("Pos"))
    points_col = _find_points_column(columns)
    assert points_col is not None

    rows: list[dict[str, Any]] = []
    for rec in table.to_dict(orient="records"):
        rank = _normalize_position(rec.get(pos_col))
        if rank is None or rank > 10:
            continue
        driver_name = _clean_text(rec.get("Driver"))
        if not driver_name:
            continue
        points = _normalize_points(rec.get(points_col))
        rows.append(
            {
                "competition_id": DRIVERS_COMPETITION_ID,
                "competition_name": "Formula E Drivers' Championship",
                "year": int(season["year_end"]),
                "event_date": f"{int(season['year_end'])}-12-31",
                "discipline_key": DISCIPLINE_ID,
                "discipline_name": DISCIPLINE_NAME,
                "gender": "mixed",
                "rank": rank,
                "participant_type": "athlete",
                "participant_ref": "",
                "participant_name": driver_name,
                "country_code": "",
                "country_name": "",
                "nationality": "",
                "points": points,
                "wins": None,
                "team_name": "",
                "round": "",
                "season_label": str(season["season_label"]),
                "source_url": str(season["url"]),
            }
        )
    return rows


def _build_team_rows(season: dict[str, Any], table: pd.DataFrame, entrants_teams: list[str]) -> list[dict[str, Any]]:
    columns = [str(col) for col in table.columns]
    pos_col = next(col for col in columns if str(col).startswith("Pos"))
    points_col = _find_points_column(columns)
    assert points_col is not None

    normalized_rows: list[dict[str, Any]] = []
    for rec in table.to_dict(orient="records"):
        rank = _normalize_position(rec.get(pos_col))
        if rank is None:
            continue
        team_name = _clean_text(rec.get("Team"))
        if not team_name:
            continue
        points = _normalize_points(rec.get(points_col))
        normalized_rows.append(
            {
                "rank": rank,
                "team_name": team_name,
                "points": points,
            }
        )

    if not normalized_rows:
        return []

    team_df = pd.DataFrame(normalized_rows)
    team_df = (
        team_df.sort_values(["rank", "points", "team_name"], ascending=[True, False, True], na_position="last")
        .drop_duplicates(subset=["team_name"], keep="first")
        .sort_values(["rank", "points", "team_name"], ascending=[True, False, True], na_position="last")
    )
    team_df = team_df.loc[team_df["rank"] <= 10].head(10).copy()

    # Some historical pages publish only classified teams (e.g. no-point teams omitted).
    # Complete to top 10 with entrant teams not yet listed, points=0.
    if len(team_df) < 10 and entrants_teams:
        existing = {_team_key(name) for name in team_df["team_name"].tolist()}
        fallback_teams = [name for name in entrants_teams if _team_key(name) and _team_key(name) not in existing]
        next_rank = int(team_df["rank"].max()) + 1 if not team_df.empty else 1
        additions: list[dict[str, Any]] = []
        for team_name in fallback_teams:
            if next_rank > 10:
                break
            additions.append({"rank": next_rank, "team_name": team_name, "points": 0.0})
            next_rank += 1
        if additions:
            team_df = pd.concat([team_df, pd.DataFrame(additions)], ignore_index=True)
            team_df = team_df.sort_values(["rank", "points", "team_name"], ascending=[True, False, True]).head(10)

    rows: list[dict[str, Any]] = []
    for row in team_df.itertuples(index=False):
        rows.append(
            {
                "competition_id": TEAMS_COMPETITION_ID,
                "competition_name": "Formula E Teams' Championship",
                "year": int(season["year_end"]),
                "event_date": f"{int(season['year_end'])}-12-31",
                "discipline_key": DISCIPLINE_ID,
                "discipline_name": DISCIPLINE_NAME,
                "gender": "mixed",
                "rank": int(row.rank),
                "participant_type": "team",
                "participant_ref": "",
                "participant_name": str(row.team_name),
                "country_code": "",
                "country_name": "",
                "nationality": "",
                "points": row.points,
                "wins": None,
                "team_name": str(row.team_name),
                "round": "",
                "season_label": str(season["season_label"]),
                "source_url": str(season["url"]),
            }
        )
    return rows


def build_seed(max_year: int, output: Path) -> pd.DataFrame:
    selected_seasons = [season for season in SEASON_PAGES if int(season["year_end"]) <= int(max_year)]
    rows: list[dict[str, Any]] = []
    for season in selected_seasons:
        drivers_table, teams_table, entrants_teams = _extract_tables(str(season["url"]))
        rows.extend(_build_driver_rows(season, drivers_table))
        rows.extend(_build_team_rows(season, teams_table, entrants_teams))

    frame = pd.DataFrame(rows)
    if frame.empty:
        raise RuntimeError("Formula E standings seed extraction produced no rows.")

    frame = frame.drop_duplicates(
        subset=["competition_id", "year", "rank", "participant_type", "participant_name"],
        keep="first",
    )
    frame = frame.sort_values(
        ["competition_id", "year", "rank", "participant_name"]
    ).reset_index(drop=True)

    profiles = (
        frame.groupby(["competition_id", "year"])["rank"]
        .apply(lambda s: tuple(sorted(int(v) for v in s.tolist())))
        .to_dict()
    )
    expected_profile = tuple(range(1, 11))
    bad_profiles = {key: value for key, value in profiles.items() if value != expected_profile}
    if bad_profiles:
        sample = dict(list(bad_profiles.items())[:10])
        raise RuntimeError(f"Unexpected Formula E rank profiles (expected 1..10): {sample}")

    output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output, index=False)
    return frame


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Formula E top-10 annual standings seed (drivers + teams).")
    parser.add_argument("--start-year", type=int, default=2015)
    parser.add_argument(
        "--max-year",
        type=int,
        default=max(2015, datetime.now(timezone.utc).year - 1),
    )
    parser.add_argument("--output", type=Path, default=SEED_PATH)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    max_year = int(args.max_year)
    start_year = int(args.start_year)
    if max_year < start_year:
        raise RuntimeError(f"max_year ({max_year}) must be >= start_year ({start_year})")
    seed = build_seed(max_year=max_year, output=Path(args.output))
    seed = seed.loc[seed["year"] >= start_year].copy()
    if seed.empty:
        raise RuntimeError("Formula E seed became empty after start_year filtering.")
    seed.to_csv(Path(args.output), index=False)
    print(
        f"[formulae-seed] wrote {args.output} rows={len(seed)} "
        f"years={seed['year'].min()}-{seed['year'].max()}"
    )
    print("[formulae-seed] rows by competition:")
    print(seed.groupby("competition_id").size())


if __name__ == "__main__":
    main()
