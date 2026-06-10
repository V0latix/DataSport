from __future__ import annotations

import argparse
import io
import re
from pathlib import Path
from typing import Any

import pandas as pd
import requests


BASE_DIR = Path(__file__).resolve().parent
SEED_PATH = BASE_DIR / "world_archery_championships_top3_seed.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder)"}

COMPETITION_ID = "world_archery_championships"
COMPETITION_NAME = "World Archery Championships"
START_YEAR = 2001
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}
YEARS = [2001, 2003, 2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019, 2021, 2023, 2025]

DISCIPLINES = ("recurve", "compound")
EVENT_CONFIG = {
    "Men's individual": ("individual", "men", "athlete"),
    "Women's individual": ("individual", "women", "athlete"),
    "Men's team": ("team", "men", "team"),
    "Women's team": ("team", "women", "team"),
    "Mixed team": ("team", "mixed", "team"),
}
COUNTRY_NAME_ALIASES = {
    "Great Britain": "GBR",
    "United Kingdom": "GBR",
    "United States": "USA",
    "United States of America": "USA",
    "South Korea": "KOR",
    "North Korea": "PRK",
    "Chinese Taipei": "TPE",
    "Czech Republic": "CZE",
    "Turkey": "TUR",
    "Russia": "RUS",
    "Russian Archery Federation": "RAF",
    "Iran": "IRN",
    "Venezuela": "VEN",
    "Moldova": "MDA",
    "Netherlands": "NLD",
    "Vietnam": "VNM",
    "Syria": "SYR",
}
COUNTRY_NAME_OVERRIDES = {
    "GBR": "United Kingdom",
    "RAF": "Russian Archery Federation",
    "TPE": "Chinese Taipei",
}


def clean_text(value: object) -> str:
    text = str(value or "")
    text = text.replace("\xa0", " ")
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = text.replace("*", "")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def country_aliases() -> dict[str, str]:
    aliases = dict(COUNTRY_NAME_ALIASES)
    try:
        import pycountry

        for country in pycountry.countries:
            aliases[str(country.name)] = str(country.alpha_3)
            common_name = getattr(country, "common_name", None)
            if common_name:
                aliases[str(common_name)] = str(country.alpha_3)
    except Exception:
        pass
    return aliases


COUNTRY_ALIASES = country_aliases()
COUNTRY_NAMES_BY_LENGTH = sorted(COUNTRY_ALIASES, key=len, reverse=True)


def country_name(country_code: str) -> str:
    code = str(country_code).upper().strip()
    override = COUNTRY_NAME_OVERRIDES.get(code)
    if override:
        return override
    try:
        import pycountry

        country = pycountry.countries.get(alpha_3=code)
        if country is not None:
            return str(getattr(country, "name"))
    except Exception:
        pass
    return code


def source_url(year: int) -> str:
    return f"https://en.wikipedia.org/wiki/{year}_World_Archery_Championships"


def event_name(raw_event: object) -> str:
    text = clean_text(raw_event)
    text = text.replace(" details", "").replace("details", "")
    return clean_text(text)


def parse_entry(value: object) -> tuple[str, str, str]:
    text = clean_text(value)
    candidates: list[tuple[int, str, str]] = []
    for candidate_country in COUNTRY_NAMES_BY_LENGTH:
        if text.startswith(f"{candidate_country} "):
            candidates.append((len(candidate_country), candidate_country, clean_text(text[len(candidate_country) :])))
        if text.endswith(f" {candidate_country}"):
            candidates.append((len(candidate_country), candidate_country, clean_text(text[: -len(candidate_country)])))
    candidates = [candidate for candidate in candidates if candidate[2]]
    if not candidates:
        raise RuntimeError(f"Could not parse archery entry/country value: {text!r}")

    _, matched_country, participant_name = sorted(candidates, reverse=True)[0]
    code = COUNTRY_ALIASES[matched_country]
    return participant_name, country_name(code), code


def fetch_medal_tables(year: int) -> list[pd.DataFrame]:
    url = source_url(year)
    response = requests.get(url, headers=HEADERS, timeout=45)
    response.raise_for_status()
    tables = [
        table[["Event", "Gold", "Silver", "Bronze"]].copy()
        for table in pd.read_html(io.StringIO(response.text))
        if {"Event", "Gold", "Silver", "Bronze"}.issubset(set(table.columns))
    ]
    if len(tables) != len(DISCIPLINES):
        raise RuntimeError(f"Expected {len(DISCIPLINES)} medal tables for {year}, found {len(tables)} at {url}")
    return tables


def build_rows(max_year: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for year in YEARS:
        if year < START_YEAR or year > int(max_year):
            continue
        for discipline_key, table in zip(DISCIPLINES, fetch_medal_tables(year)):
            for row in table.itertuples(index=False):
                raw_event_name = event_name(getattr(row, "Event"))
                if raw_event_name not in EVENT_CONFIG:
                    raise RuntimeError(f"Unsupported World Archery event name for {year}: {raw_event_name!r}")

                event_key, gender, participant_type = EVENT_CONFIG[raw_event_name]
                for rank, medal_column in ((1, "Gold"), (2, "Silver"), (3, "Bronze")):
                    participant_name, resolved_country_name, country_code = parse_entry(getattr(row, medal_column))
                    rows.append(
                        {
                            "competition_id": COMPETITION_ID,
                            "competition_name": COMPETITION_NAME,
                            "year": year,
                            "event_date": f"{year}-12-31",
                            "discipline_key": f"archery-{discipline_key}",
                            "discipline_name": discipline_key.title(),
                            "event_key": event_key,
                            "event_name": raw_event_name,
                            "gender": gender,
                            "rank": rank,
                            "medal": RANK_TO_MEDAL[rank],
                            "participant_type": participant_type,
                            "participant_name": participant_name,
                            "country_name": resolved_country_name,
                            "country_code": country_code,
                            "source_url": source_url(year),
                        }
                    )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Build World Archery Championships podium seed.")
    parser.add_argument("--max-year", type=int, default=2026)
    parser.add_argument("--out", type=Path, default=SEED_PATH)
    args = parser.parse_args()

    rows = build_rows(max(START_YEAR, int(args.max_year)))
    if not rows:
        raise RuntimeError("No World Archery Championships rows extracted.")

    frame = pd.DataFrame(rows)
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype(int)
    frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce").astype(int)
    frame = frame.loc[frame["year"] > 2000].copy()
    frame = frame.sort_values(
        ["year", "discipline_key", "event_key", "gender", "rank", "participant_name"]
    ).reset_index(drop=True)

    profiles = (
        frame.groupby(["year", "discipline_key", "event_key", "gender"])["rank"]
        .apply(lambda values: tuple(sorted(int(value) for value in values.tolist())))
        .to_dict()
    )
    invalid_profiles = {key: value for key, value in profiles.items() if value != (1, 2, 3)}
    if invalid_profiles:
        sample = dict(list(invalid_profiles.items())[:30])
        raise RuntimeError(f"Unexpected World Archery rank profile(s): {sample}")

    duplicates = frame.loc[
        frame.duplicated(
            subset=["year", "discipline_key", "event_key", "gender", "participant_name", "country_code"],
            keep=False,
        )
    ]
    if not duplicates.empty:
        sample = duplicates[
            ["year", "discipline_key", "event_key", "gender", "participant_name", "country_code"]
        ].head(20).to_dict("records")
        raise RuntimeError(f"Duplicate entry in same World Archery event: {sample}")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(args.out, index=False)

    years = sorted(int(year) for year in frame["year"].unique().tolist())
    event_count = frame[["year", "discipline_key", "event_key", "gender"]].drop_duplicates().shape[0]
    print(
        f"[seed] world_archery_championships rows={len(frame)} "
        f"years={years[0]}-{years[-1]} events={event_count} out={args.out}"
    )


if __name__ == "__main__":
    main()
