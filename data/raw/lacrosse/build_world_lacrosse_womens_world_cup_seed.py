from __future__ import annotations

import re
from io import StringIO
from pathlib import Path

import pandas as pd
import requests


SOURCE_URL = "https://en.wikipedia.org/wiki/World_Lacrosse_Women%27s_Championship"
OUT_FILE = Path(__file__).resolve().with_name("world_lacrosse_womens_world_cup_top4_seed.csv")
COMPETITION_ID = "world_lacrosse_womens_world_cup"
COMPETITION_NAME = "Women's Lacrosse World Cup"
DISCIPLINE_KEY = "field-lacrosse"
DISCIPLINE_NAME = "Field lacrosse"
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze", 4: ""}
COUNTRY_CODE_OVERRIDES = {
    "Australia": "AUS",
    "Canada": "CAN",
    "England": "ENG",
    "Haudenosaunee": "HAU",
    "Israel": "ISR",
    "United States": "USA",
}


def clean_text(value: object) -> str:
    value = re.sub(r"\[[^\]]+\]", "", str(value))
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def country_code(country_name: str) -> str:
    if country_name in COUNTRY_CODE_OVERRIDES:
        return COUNTRY_CODE_OVERRIDES[country_name]
    try:
        import pycountry

        return pycountry.countries.lookup(country_name).alpha_3
    except Exception as exc:
        raise RuntimeError(f"Missing country code mapping for {country_name!r}") from exc


def past_results_table(html: str) -> pd.DataFrame:
    for frame in pd.read_html(StringIO(html)):
        if "Team" in frame.columns and any(str(column).startswith("2022") for column in frame.columns):
            return frame
    raise RuntimeError("Could not find World Lacrosse Women's Championship past results table.")


def build_seed() -> pd.DataFrame:
    response = requests.get(SOURCE_URL, headers={"User-Agent": "DataSport seed builder"}, timeout=60)
    response.raise_for_status()
    table = past_results_table(response.text)

    rows: list[dict[str, object]] = []
    for column in table.columns:
        match = re.match(r"^(20\d{2})", str(column))
        if not match:
            continue
        year = int(match.group(1))
        if year <= 2000:
            continue

        for record in table[["Team", column]].to_dict("records"):
            team_name = clean_text(record["Team"])
            finish = clean_text(record[column])
            rank_match = re.fullmatch(r"([1-4])(?:st|nd|rd|th)", finish)
            if rank_match is None:
                continue
            rank = int(rank_match.group(1))
            rows.append(
                {
                    "competition_id": COMPETITION_ID,
                    "competition_name": COMPETITION_NAME,
                    "year": year,
                    "event_date": f"{year}-12-31",
                    "discipline_key": DISCIPLINE_KEY,
                    "discipline_name": DISCIPLINE_NAME,
                    "gender": "women",
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
        raise RuntimeError("No Women's Lacrosse World Cup top4 rows extracted.")
    frame = frame.sort_values(["year", "rank", "country_code"]).reset_index(drop=True)
    profiles = frame.groupby("year")["rank"].apply(lambda values: tuple(sorted(values.tolist()))).to_dict()
    bad_profiles = {key: value for key, value in profiles.items() if value != (1, 2, 3, 4)}
    if bad_profiles:
        sample = dict(list(bad_profiles.items())[:20])
        raise RuntimeError(f"Unexpected Women's Lacrosse World Cup rank profiles: {sample}")
    return frame


def main() -> None:
    frame = build_seed()
    frame.to_csv(OUT_FILE, index=False)
    print(f"Wrote {len(frame)} rows to {OUT_FILE}")
    print(frame.groupby("competition_id")["year"].agg(["min", "max", "nunique"]).to_string())


if __name__ == "__main__":
    main()
