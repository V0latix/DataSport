from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

BASE_URL = "https://dataride.uci.org"
SEASONS_ENDPOINT = "/iframe/GetDisciplineSeasons/"
RANKINGS_DISCIPLINE_ENDPOINT = "/iframe/RankingsDiscipline/"
OBJECT_RANKINGS_ENDPOINT = "/iframe/ObjectRankings/"

DISCIPLINE_ID = 10  # Road cycling
CATEGORY_ID = 22  # Men Elite
RACE_TYPE_ID = 0  # All
WORLD_GROUP_ID = 1
NATION_RANKING_TYPE_ID = 3

SEED_PATH = Path(__file__).resolve().parent / "uci_road_nation_rankings_history_seed.csv"


@dataclass(frozen=True)
class Season:
    year: int
    season_id: int


def parse_dotnet_date(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text.startswith("/Date("):
        return None
    try:
        millis = int(text[6:].split(")", 1)[0])
    except Exception:
        return None
    dt = datetime.fromtimestamp(millis / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def post_json(path: str, data: dict[str, Any], timeout: int = 45) -> Any:
    response = requests.post(
        f"{BASE_URL}{path}",
        data=data,
        headers={
            "User-Agent": "DataSportPipeline/0.1 (UCI road nation rankings seed)",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        },
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def get_json(path: str, params: dict[str, Any], timeout: int = 45) -> Any:
    response = requests.get(
        f"{BASE_URL}{path}",
        params=params,
        headers={"User-Agent": "DataSportPipeline/0.1 (UCI road nation rankings seed)"},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def fetch_seasons(min_year: int, max_year: int) -> list[Season]:
    payload = get_json(SEASONS_ENDPOINT, {"disciplineId": DISCIPLINE_ID})
    seasons: list[Season] = []
    for row in payload:
        year = int(row.get("Year") or 0)
        season_id = int(row.get("Id") or 0)
        if year < min_year or year > max_year or season_id <= 0:
            continue
        seasons.append(Season(year=year, season_id=season_id))
    seasons.sort(key=lambda s: s.year)
    return seasons


def fetch_rankings_for_season(season_id: int) -> list[dict[str, Any]]:
    return post_json(
        RANKINGS_DISCIPLINE_ENDPOINT,
        {
            "disciplineId": DISCIPLINE_ID,
            "take": 40,
            "skip": 0,
            "page": 1,
            "pageSize": 40,
            "filter[filters][0][field]": "RaceTypeId",
            "filter[filters][0][value]": RACE_TYPE_ID,
            "filter[filters][1][field]": "CategoryId",
            "filter[filters][1][value]": CATEGORY_ID,
            "filter[filters][2][field]": "SeasonId",
            "filter[filters][2][value]": season_id,
        },
    )


def find_world_nation_ranking(rankings_payload: list[dict[str, Any]]) -> dict[str, Any] | None:
    for group in rankings_payload:
        group_id = int(group.get("GroupId") or 0)
        group_name = str(group.get("GroupName") or "").strip()
        if group_id != WORLD_GROUP_ID or group_name != "World Ranking":
            continue

        for ranking in group.get("Rankings") or []:
            if int(ranking.get("RankingTypeId") or 0) != NATION_RANKING_TYPE_ID:
                continue
            return {
                "group_id": group_id,
                "group_name": group_name,
                "ranking_id": int(ranking.get("Id") or 0),
                "moment_id": int(ranking.get("MomentId") or 0),
                "ranking_name": str(ranking.get("RankingName") or "").strip(),
            }
    return None


def fetch_object_rankings(ranking_id: int, season_id: int, moment_id: int) -> list[dict[str, Any]]:
    payload = post_json(
        OBJECT_RANKINGS_ENDPOINT,
        {
            "rankingId": ranking_id,
            "disciplineId": DISCIPLINE_ID,
            "rankingTypeId": NATION_RANKING_TYPE_ID,
            "take": 40,
            "skip": 0,
            "page": 1,
            "pageSize": 40,
            "filter[filters][0][field]": "RaceTypeId",
            "filter[filters][0][value]": RACE_TYPE_ID,
            "filter[filters][1][field]": "CategoryId",
            "filter[filters][1][value]": CATEGORY_ID,
            "filter[filters][2][field]": "SeasonId",
            "filter[filters][2][value]": season_id,
            "filter[filters][3][field]": "MomentId",
            "filter[filters][3][value]": moment_id,
            "filter[filters][4][field]": "CountryId",
            "filter[filters][4][value]": 0,
            "filter[filters][5][field]": "IndividualName",
            "filter[filters][5][value]": "",
            "filter[filters][6][field]": "TeamName",
            "filter[filters][6][value]": "",
        },
    )
    if isinstance(payload, dict):
        return list(payload.get("data") or [])
    return []


def build_rows(min_year: int, max_year: int) -> tuple[list[dict[str, Any]], list[int]]:
    seasons = fetch_seasons(min_year=min_year, max_year=max_year)
    rows: list[dict[str, Any]] = []
    missing_years: list[int] = []

    for season in seasons:
        rankings_payload = fetch_rankings_for_season(season.season_id)
        selected = find_world_nation_ranking(rankings_payload)
        if not selected:
            missing_years.append(season.year)
            continue

        ranking_rows = fetch_object_rankings(
            ranking_id=selected["ranking_id"],
            season_id=season.season_id,
            moment_id=selected["moment_id"],
        )
        ranking_rows = sorted(
            ranking_rows,
            key=lambda row: (
                int(row.get("Rank") or 9999),
                str(row.get("NationFullName") or row.get("FullName") or ""),
            ),
        )

        top10 = [row for row in ranking_rows if int(row.get("Rank") or 0) > 0][:10]
        if len(top10) < 10:
            missing_years.append(season.year)
            continue

        for row in top10:
            country_name = str(row.get("NationFullName") or row.get("FullName") or "").strip()
            country_code = str(row.get("NationName") or "").strip().upper()
            effective_date = parse_dotnet_date(row.get("ComputationDate"))
            rows.append(
                {
                    "requested_year": season.year,
                    "discipline_season_id": season.season_id,
                    "ranking_id": selected["ranking_id"],
                    "group_id": selected["group_id"],
                    "group_name": selected["group_name"],
                    "ranking_name": selected["ranking_name"],
                    "moment_id": selected["moment_id"],
                    "effective_date": effective_date,
                    "country_name": country_name,
                    "country_code": country_code,
                    "source_rank": int(row.get("Rank") or 0),
                    "points": float(row.get("Points") or 0.0),
                    "source_url": (
                        f"{BASE_URL}/iframe/RankingDetails/{selected['ranking_id']}"
                        f"?disciplineId={DISCIPLINE_ID}"
                        f"&groupId={selected['group_id']}"
                        f"&momentId={selected['moment_id']}"
                        f"&disciplineSeasonId={season.season_id}"
                        f"&rankingTypeId={NATION_RANKING_TYPE_ID}"
                        f"&categoryId={CATEGORY_ID}"
                        f"&raceTypeId={RACE_TYPE_ID}"
                    ),
                }
            )

    rows.sort(key=lambda r: (int(r["requested_year"]), int(r["source_rank"]), str(r["country_code"])))
    return rows, sorted(set(missing_years))


def write_rows(rows: list[dict[str, Any]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    headers = [
        "requested_year",
        "discipline_season_id",
        "ranking_id",
        "group_id",
        "group_name",
        "ranking_name",
        "moment_id",
        "effective_date",
        "country_name",
        "country_code",
        "source_rank",
        "points",
        "source_url",
    ]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build UCI road world nation ranking top10 seed.")
    parser.add_argument("--min-year", type=int, default=2000)
    parser.add_argument("--max-year", type=int, default=datetime.now(timezone.utc).year)
    parser.add_argument("--out", type=Path, default=SEED_PATH)
    args = parser.parse_args()

    rows, missing = build_rows(min_year=args.min_year, max_year=args.max_year)
    if not rows:
        raise RuntimeError("No rows produced for UCI road world nation rankings seed.")
    write_rows(rows, args.out)

    available_years = sorted({int(row["requested_year"]) for row in rows})
    print(f"[uci-road-nation-seed] rows={len(rows)} years={available_years[0]}-{available_years[-1]} out={args.out}")
    if missing:
        print(f"[uci-road-nation-seed] missing_world_nation_ranking_years={missing}")


if __name__ == "__main__":
    main()
