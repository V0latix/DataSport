from __future__ import annotations

import re
from io import StringIO
from pathlib import Path

import pandas as pd
import requests


SOURCE_URL = "https://en.wikipedia.org/wiki/List_of_Davis_Cup_champions"
OUT_FILE = Path(__file__).resolve().with_name("davis_cup_finalists_seed.csv")
RANK_TO_MEDAL = {1: "gold", 2: "silver"}
COUNTRY_CODE_OVERRIDES = {
    "Argentina": "ARG",
    "Australia": "AUS",
    "Belgium": "BEL",
    "Canada": "CAN",
    "Croatia": "CRO",
    "Czech Republic": "CZE",
    "France": "FRA",
    "Great Britain": "GBR",
    "Italy": "ITA",
    "Netherlands": "NLD",
    "RTF": "RTF",
    "Russia": "RUS",
    "Serbia": "SRB",
    "Slovakia": "SVK",
    "Spain": "ESP",
    "Switzerland": "SUI",
    "United States": "USA",
}


def clean_text(value: object) -> str:
    value = re.sub(r"\[[^\]]+\]", "", str(value))
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def season_year(value: object) -> int | None:
    text = clean_text(value)
    if not text:
        return None
    match = re.fullmatch(r"(\d{4})[-–](\d{2})", text)
    if match:
        century = int(match.group(1)[:2]) * 100
        return century + int(match.group(2))
    match = re.search(r"\d{4}", text)
    return int(match.group(0)) if match else None


def country_code(country_name: str) -> str:
    if country_name in COUNTRY_CODE_OVERRIDES:
        return COUNTRY_CODE_OVERRIDES[country_name]
    try:
        import pycountry

        return pycountry.countries.lookup(country_name).alpha_3
    except Exception as exc:
        raise RuntimeError(f"Missing country code mapping for {country_name!r}") from exc


def build_seed() -> pd.DataFrame:
    response = requests.get(SOURCE_URL, headers={"User-Agent": "DataSport seed builder"}, timeout=60)
    response.raise_for_status()
    tables = pd.read_html(StringIO(response.text))
    table = next(
        frame
        for frame in tables
        if {"Year", "Winner", "Runner-up", "Score"}.issubset(set(map(str, frame.columns)))
    )

    rows: list[dict[str, object]] = []
    for record in table.to_dict("records"):
        year = season_year(record["Year"])
        if year is None or year <= 2000:
            continue

        entries = [(1, clean_text(record["Winner"])), (2, clean_text(record["Runner-up"]))]
        if any(not name for _, name in entries):
            continue
        score = clean_text(record.get("Score", ""))
        original_year = clean_text(record["Year"])
        for rank, team_name in entries:
            rows.append(
                {
                    "competition_id": "davis_cup",
                    "competition_name": "Davis Cup",
                    "year": year,
                    "source_year": original_year,
                    "event_date": f"{year}-12-31",
                    "discipline_key": "tennis",
                    "discipline_name": "Tennis",
                    "gender": "men",
                    "rank": rank,
                    "medal": RANK_TO_MEDAL[rank],
                    "participant_type": "team",
                    "participant_name": team_name,
                    "country_name": team_name,
                    "country_code": country_code(team_name),
                    "score": score,
                    "source_url": SOURCE_URL,
                }
            )

    frame = pd.DataFrame(rows)
    if frame.empty:
        raise RuntimeError("No Davis Cup finalist rows extracted.")
    frame = frame.sort_values(["year", "rank"]).reset_index(drop=True)
    return frame


def main() -> None:
    frame = build_seed()
    profiles = frame.groupby("year")["rank"].apply(lambda values: tuple(sorted(values.tolist()))).to_dict()
    bad_profiles = {key: value for key, value in profiles.items() if value != (1, 2)}
    if bad_profiles:
        sample = dict(list(bad_profiles.items())[:20])
        raise RuntimeError(f"Unexpected Davis Cup rank profiles: {sample}")
    frame.to_csv(OUT_FILE, index=False)
    print(f"Wrote {len(frame)} rows to {OUT_FILE}")
    print(frame.groupby("competition_id")["year"].agg(["min", "max", "nunique"]).to_string())


if __name__ == "__main__":
    main()
