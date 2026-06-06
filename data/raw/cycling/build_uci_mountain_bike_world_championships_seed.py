from __future__ import annotations

import argparse
import io
import re
from pathlib import Path
from typing import Any

import pandas as pd
import requests


BASE_DIR = Path(__file__).resolve().parent
SEED_PATH = BASE_DIR / "uci_mountain_bike_world_championships_top3_seed.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder)"}

COMPETITION_ID = "uci_mountain_bike_world_championships"
COMPETITION_NAME = "UCI Mountain Bike World Championships"
START_YEAR = 2001
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}

PODIUM_PAGES = {
    ("men", "xco"): {
        "source_url": "https://en.wikipedia.org/wiki/UCI_Mountain_Bike_%26_Trials_World_Championships_%E2%80%93_Men%27s_cross-country",
        "discipline_id": "cycling-mountain-bike-cross-country",
        "discipline_name": "Mountain Bike Cross-country",
        "event_name": "Men Elite Cross-country",
    },
    ("women", "xco"): {
        "source_url": "https://en.wikipedia.org/wiki/UCI_Mountain_Bike_%26_Trials_World_Championships_%E2%80%93_Women%27s_cross-country",
        "discipline_id": "cycling-mountain-bike-cross-country",
        "discipline_name": "Mountain Bike Cross-country",
        "event_name": "Women Elite Cross-country",
    },
    ("men", "downhill"): {
        "source_url": "https://en.wikipedia.org/wiki/UCI_Mountain_Bike_%26_Trials_World_Championships_%E2%80%93_Men%27s_downhill",
        "discipline_id": "cycling-mountain-bike-downhill",
        "discipline_name": "Mountain Bike Downhill",
        "event_name": "Men Elite Downhill",
    },
    ("women", "downhill"): {
        "source_url": "https://en.wikipedia.org/wiki/UCI_Mountain_Bike_%26_Trials_World_Championships_%E2%80%93_Women%27s_downhill",
        "discipline_id": "cycling-mountain-bike-downhill",
        "discipline_name": "Mountain Bike Downhill",
        "event_name": "Women Elite Downhill",
    },
}

COUNTRY_CODE_ALIASES = {
    "Great Britain": "GBR",
    "Russia": "RUS",
    "Czech Republic": "CZE",
    "Iran": "IRN",
    "South Korea": "KOR",
    "United States": "USA",
    "United States of America": "USA",
}


def clean_text(value: object) -> str:
    text = str(value or "")
    text = text.replace("\xa0", " ")
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def country_aliases() -> dict[str, str]:
    aliases = dict(COUNTRY_CODE_ALIASES)
    try:
        import pycountry

        for country in pycountry.countries:
            aliases[str(country.name)] = str(country.alpha_3)
            official_name = getattr(country, "official_name", None)
            if official_name:
                aliases[str(official_name)] = str(country.alpha_3)
            common_name = getattr(country, "common_name", None)
            if common_name:
                aliases[str(common_name)] = str(country.alpha_3)
    except Exception:
        pass
    return aliases


COUNTRY_ALIASES = country_aliases()
COUNTRY_NAMES_BY_LENGTH = sorted(COUNTRY_ALIASES, key=len, reverse=True)


def parse_year(championships_value: object) -> int:
    text = clean_text(championships_value)
    match = re.search(r"(?:19|20)\d{2}", text)
    if not match:
        raise RuntimeError(f"Could not parse championship year: {text!r}")
    return int(match.group(0))


def parse_rider(value: object) -> tuple[str, str, str]:
    text = clean_text(value)
    for country_name in COUNTRY_NAMES_BY_LENGTH:
        if text == country_name or text.endswith(f" {country_name}"):
            rider_name = clean_text(text[: -len(country_name)])
            country_code = COUNTRY_ALIASES[country_name]
            if rider_name and re.fullmatch(r"[A-Z]{3}", country_code):
                return rider_name, country_name, country_code
    raise RuntimeError(f"Could not parse rider/country value: {text!r}")


def fetch_podium_table(source_url: str) -> pd.DataFrame:
    response = requests.get(source_url, headers=HEADERS, timeout=45)
    response.raise_for_status()
    tables = pd.read_html(io.StringIO(response.text))
    for table in tables:
        if {"Championships", "Gold", "Silver", "Bronze"}.issubset(set(table.columns)):
            return table[["Championships", "Gold", "Silver", "Bronze"]].copy()
    raise RuntimeError(f"No podium table found at {source_url}")


def build_rows(max_year: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (gender, event_key), config in PODIUM_PAGES.items():
        source_url = str(config["source_url"])
        table = fetch_podium_table(source_url)
        table["year"] = table["Championships"].map(parse_year)
        table = table.loc[(table["year"] >= START_YEAR) & (table["year"] <= int(max_year))].copy()

        for row in table.itertuples(index=False):
            year = int(getattr(row, "year"))
            for rank, medal_column in ((1, "Gold"), (2, "Silver"), (3, "Bronze")):
                rider_name, country_name, country_code = parse_rider(getattr(row, medal_column))
                rows.append(
                    {
                        "competition_id": COMPETITION_ID,
                        "competition_name": COMPETITION_NAME,
                        "year": year,
                        "event_date": f"{year}-09-01",
                        "discipline_key": config["discipline_id"],
                        "discipline_name": config["discipline_name"],
                        "event_key": event_key,
                        "event_name": config["event_name"],
                        "gender": gender,
                        "rank": rank,
                        "medal": RANK_TO_MEDAL[rank],
                        "participant_type": "athlete",
                        "participant_name": rider_name,
                        "country_name": country_name,
                        "country_code": country_code,
                        "source_url": source_url,
                    }
                )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Build UCI Mountain Bike World Championships elite podium seed.")
    parser.add_argument("--max-year", type=int, default=2026)
    parser.add_argument("--out", type=Path, default=SEED_PATH)
    args = parser.parse_args()

    rows = build_rows(max(START_YEAR, int(args.max_year)))
    if not rows:
        raise RuntimeError("No UCI Mountain Bike World Championships rows extracted.")

    frame = pd.DataFrame(rows)
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype(int)
    frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce").astype(int)
    frame = frame.loc[frame["year"] > 2000].copy()
    frame = frame.sort_values(["year", "event_key", "gender", "rank", "participant_name"]).reset_index(drop=True)

    profiles = (
        frame.groupby(["year", "event_key", "gender"])["rank"]
        .apply(lambda values: tuple(sorted(int(value) for value in values.tolist())))
        .to_dict()
    )
    invalid_profiles = {key: value for key, value in profiles.items() if value != (1, 2, 3)}
    if invalid_profiles:
        sample = dict(list(invalid_profiles.items())[:30])
        raise RuntimeError(f"Unexpected UCI Mountain Bike rank profile(s): {sample}")

    duplicates = frame.loc[
        frame.duplicated(subset=["year", "event_key", "gender", "participant_name", "country_code"], keep=False)
    ]
    if not duplicates.empty:
        sample = duplicates[["year", "event_key", "gender", "participant_name", "country_code"]].head(20).to_dict(
            "records"
        )
        raise RuntimeError(f"Duplicate rider in same UCI Mountain Bike event: {sample}")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(args.out, index=False)

    years = sorted(int(year) for year in frame["year"].unique().tolist())
    event_count = frame[["year", "event_key", "gender"]].drop_duplicates().shape[0]
    print(
        f"[seed] uci_mountain_bike_world_championships rows={len(frame)} "
        f"years={years[0]}-{years[-1]} events={event_count} out={args.out}"
    )


if __name__ == "__main__":
    main()
