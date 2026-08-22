from __future__ import annotations

import re
from io import StringIO
from pathlib import Path

import pandas as pd
import requests


YEARS = [2001, 2005, 2009, 2013, 2017, 2022, 2025]
OUT_FILE = Path(__file__).resolve().with_name("world_games_medal_table_top10_seed.csv")
COMPETITION_ID = "world_games"
COMPETITION_NAME = "World Games"
DISCIPLINE_KEY = "world-games-overall-medal-table"
DISCIPLINE_NAME = "World Games overall medal table"
COUNTRY_CODE_OVERRIDES = {
    "Australia": "AUS",
    "Belgium": "BEL",
    "China": "CHN",
    "Chinese Taipei": "TPE",
    "Colombia": "COL",
    "Denmark": "DEN",
    "France": "FRA",
    "Germany": "GER",
    "Great Britain": "GBR",
    "Hungary": "HUN",
    "Italy": "ITA",
    "Japan": "JPN",
    "Netherlands": "NED",
    "Russia": "RUS",
    "South Korea": "KOR",
    "Spain": "ESP",
    "Ukraine": "UKR",
    "United States": "USA",
}


def clean_text(value: object) -> str:
    value = re.sub(r"\[[^\]]+\]", "", str(value))
    value = re.sub(r"\*", "", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def split_nation(value: object) -> tuple[str, str]:
    text = clean_text(value)
    code_match = re.search(r"\(([A-Z]{3})\)", text)
    if code_match:
        code = code_match.group(1)
        name = clean_text(re.sub(r"\([A-Z]{3}\)", "", text))
        return name, code
    if text in COUNTRY_CODE_OVERRIDES:
        return text, COUNTRY_CODE_OVERRIDES[text]
    try:
        import pycountry

        country = pycountry.countries.lookup(text)
        return text, country.alpha_3
    except Exception as exc:
        raise RuntimeError(f"Missing country code mapping for {text!r}") from exc


def medal_table(html: str) -> pd.DataFrame:
    candidates: list[pd.DataFrame] = []
    for frame in pd.read_html(StringIO(html)):
        columns = {str(column) for column in frame.columns}
        if {"Rank", "Nation", "Gold", "Silver", "Bronze", "Total"}.issubset(columns):
            candidates.append(frame)
    if not candidates:
        raise RuntimeError("Could not find World Games medal table.")
    return max(candidates, key=len)


def build_seed() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for year in YEARS:
        source_url = f"https://en.wikipedia.org/wiki/{year}_World_Games"
        response = requests.get(source_url, headers={"User-Agent": "DataSport seed builder"}, timeout=60)
        response.raise_for_status()
        table = medal_table(response.text)
        table = table.copy()
        table["Rank"] = pd.to_numeric(table["Rank"], errors="coerce")
        table = table.dropna(subset=["Rank"]).copy()
        table["Rank"] = table["Rank"].astype(int)
        table = table.loc[table["Rank"].between(1, 10)].copy()

        for record in table.to_dict("records"):
            nation_name, country_code = split_nation(record["Nation"])
            gold = int(record["Gold"])
            silver = int(record["Silver"])
            bronze = int(record["Bronze"])
            total = int(record["Total"])
            rows.append(
                {
                    "competition_id": COMPETITION_ID,
                    "competition_name": COMPETITION_NAME,
                    "year": year,
                    "event_date": f"{year}-12-31",
                    "discipline_key": DISCIPLINE_KEY,
                    "discipline_name": DISCIPLINE_NAME,
                    "gender": "mixed",
                    "rank": int(record["Rank"]),
                    "participant_type": "team",
                    "participant_name": nation_name,
                    "country_name": nation_name,
                    "country_code": country_code,
                    "gold": gold,
                    "silver": silver,
                    "bronze": bronze,
                    "total": total,
                    "source_url": source_url,
                }
            )

    frame = pd.DataFrame(rows).sort_values(["year", "rank", "country_code"]).reset_index(drop=True)
    if frame.empty:
        raise RuntimeError("No World Games medal table rows extracted.")
    profiles = frame.groupby("year")["rank"].apply(lambda values: tuple(sorted(values.tolist()))).to_dict()
    bad_profiles = {year: profile for year, profile in profiles.items() if profile != tuple(range(1, 11))}
    if bad_profiles:
        raise RuntimeError(f"Unexpected World Games top10 rank profiles: {bad_profiles}")
    return frame


def main() -> None:
    frame = build_seed()
    frame.to_csv(OUT_FILE, index=False)
    print(f"Wrote {len(frame)} rows to {OUT_FILE}")
    print(frame.groupby("competition_id")["year"].agg(["min", "max", "nunique"]).to_string())


if __name__ == "__main__":
    main()
