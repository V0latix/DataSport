from __future__ import annotations

from io import StringIO
from pathlib import Path
import re

import pandas as pd
import requests


SOURCE_URL = "https://en.wikipedia.org/wiki/FIFA_Beach_Soccer_World_Cup"
OUT_FILE = Path(__file__).resolve().with_name("fifa_beach_soccer_world_cup_top4_seed.csv")
COMPETITION_ID = "fifa_beach_soccer_world_cup"
COMPETITION_NAME = "FIFA Beach Soccer World Cup"
DISCIPLINE_KEY = "beach-soccer"
DISCIPLINE_NAME = "Beach soccer"
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze", 4: ""}
COUNTRY_CODE_OVERRIDES = {
    "Belarus": "BLR",
    "Brazil": "BRA",
    "El Salvador": "SLV",
    "France": "FRA",
    "Iran": "IRN",
    "Italy": "ITA",
    "Japan": "JPN",
    "Mexico": "MEX",
    "Portugal": "PRT",
    "Russia": "RUS",
    "Senegal": "SEN",
    "Spain": "ESP",
    "Switzerland": "SUI",
    "Tahiti": "TAH",
    "Uruguay": "URU",
}


def clean_team(value: object) -> str:
    text = str(value).strip()
    text = re.sub(r"\[[^\]]+\]", "", text).strip()
    if text == "RFU":
        return "Russia"
    return text


def country_code(country_name: str) -> str:
    if country_name in COUNTRY_CODE_OVERRIDES:
        return COUNTRY_CODE_OVERRIDES[country_name]
    try:
        import pycountry

        return pycountry.countries.lookup(country_name).alpha_3
    except Exception as exc:
        raise RuntimeError(f"Missing country code mapping for {country_name!r}") from exc


def clean_year(value: object) -> int | None:
    text = str(value).strip()
    match = re.search(r"\d{4}", text)
    if not match:
        return None
    return int(match.group(0))


def editions_table(html: str) -> pd.DataFrame:
    for frame in pd.read_html(StringIO(html)):
        flat_columns = [
            " ".join(str(part).strip() for part in column if str(part) != "nan")
            if isinstance(column, tuple)
            else str(column)
            for column in frame.columns
        ]
        if {
            "Year Year",
            "Final Champions",
            "Final Runners-up",
            "Third place play-off Third place",
            "Third place play-off Fourth place",
        }.issubset(set(flat_columns)):
            frame = frame.copy()
            frame.columns = flat_columns
            return frame
    raise RuntimeError("Could not find FIFA Beach Soccer World Cup editions table.")


def extract_ranked_teams(record: dict[str, object]) -> list[tuple[int, str]]:
    year = clean_year(record["Year Year"])
    if year is not None and year >= 2015:
        champion = record["Unnamed: 3_level_0 Unnamed: 3_level_1"]
        runner_up = record["Final Score"]
        third_place = record["Final Runners-up"]
        fourth_place = record["Third place play-off Third place"]
    else:
        champion = record["Final Champions"]
        runner_up = record["Final Runners-up"]
        third_place = record["Third place play-off Third place"]
        fourth_place = record["Third place play-off Fourth place"]
    return [
        (1, clean_team(champion)),
        (2, clean_team(runner_up)),
        (3, clean_team(third_place)),
        (4, clean_team(fourth_place)),
    ]


def build_seed() -> pd.DataFrame:
    response = requests.get(SOURCE_URL, headers={"User-Agent": "DataSport seed builder"}, timeout=60)
    response.raise_for_status()
    table = editions_table(response.text)

    rows: list[dict[str, object]] = []
    for record in table.to_dict("records"):
        year = clean_year(record["Year Year"])
        if year is None or year <= 2000:
            continue

        for rank, team_name in extract_ranked_teams(record):
            if not team_name or team_name.lower() in {"nan", "tbd", "-"} or team_name == "–":
                continue
            rows.append(
                {
                    "competition_id": COMPETITION_ID,
                    "competition_name": COMPETITION_NAME,
                    "year": year,
                    "event_date": f"{year}-12-31",
                    "discipline_key": DISCIPLINE_KEY,
                    "discipline_name": DISCIPLINE_NAME,
                    "gender": "men",
                    "rank": rank,
                    "medal": RANK_TO_MEDAL[rank],
                    "participant_type": "team",
                    "participant_name": team_name,
                    "country_name": team_name,
                    "country_code": country_code(team_name),
                    "source_url": SOURCE_URL,
                }
            )

    frame = pd.DataFrame(rows)
    if frame.empty:
        raise RuntimeError("No FIFA Beach Soccer World Cup top4 rows extracted.")
    frame = frame.sort_values(["year", "rank", "country_code"]).reset_index(drop=True)
    profiles = frame.groupby("year")["rank"].apply(lambda values: tuple(sorted(values.tolist()))).to_dict()
    bad_profiles = {year: profile for year, profile in profiles.items() if profile != (1, 2, 3, 4)}
    if bad_profiles:
        raise RuntimeError(f"Unexpected FIFA Beach Soccer World Cup rank profiles: {bad_profiles}")
    return frame


def main() -> None:
    frame = build_seed()
    frame.to_csv(OUT_FILE, index=False)
    print(f"Wrote {len(frame)} rows to {OUT_FILE}")
    print(frame.groupby("competition_id")["year"].agg(["min", "max", "nunique"]).to_string())


if __name__ == "__main__":
    main()
