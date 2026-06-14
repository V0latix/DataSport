from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag


BASE_DIR = Path(__file__).resolve().parent
SEED_PATH = BASE_DIR / "world_amateur_team_championships_top3_seed.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder)"}

COMPETITION_ID = "world_amateur_team_championships"
COMPETITION_NAME = "World Amateur Team Championships"
START_YEAR = 2001
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}


@dataclass(frozen=True)
class TrophyConfig:
    source_url: str
    trophy_name: str
    gender: str
    medal_columns: tuple[str, str, str]


TROPHIES = {
    "eisenhower_trophy": TrophyConfig(
        source_url="https://en.wikipedia.org/wiki/Eisenhower_Trophy",
        trophy_name="Eisenhower Trophy",
        gender="men",
        medal_columns=("Winners", "Silver medalists", "Bronze medalists"),
    ),
    "espirito_santo_trophy": TrophyConfig(
        source_url="https://en.wikipedia.org/wiki/Espirito_Santo_Trophy",
        trophy_name="Espirito Santo Trophy",
        gender="women",
        medal_columns=("Winners", "Runners-up", "Third place"),
    ),
}

COUNTRY_CODE_NORMALIZATION = {
    "DEN": "DNK",
    "GER": "DEU",
    "KOR": "KOR",
    "NED": "NLD",
    "SUI": "CHE",
}
COUNTRY_NAME_ALIASES = {
    "Australia": "AUS",
    "Austria": "AUT",
    "Canada": "CAN",
    "Chinese Taipei": "TPE",
    "Colombia": "COL",
    "Denmark": "DNK",
    "England": "ENG",
    "Finland": "FIN",
    "France": "FRA",
    "Germany": "DEU",
    "Ireland": "IRL",
    "Italy": "ITA",
    "Japan": "JPN",
    "Korea": "KOR",
    "Mexico": "MEX",
    "Netherlands": "NLD",
    "Norway": "NOR",
    "Portugal": "PRT",
    "Scotland": "SCO",
    "South Africa": "ZAF",
    "South Korea": "KOR",
    "Spain": "ESP",
    "Sweden": "SWE",
    "Switzerland": "CHE",
    "Thailand": "THA",
    "United States": "USA",
}
COUNTRY_NAME_OVERRIDES = {
    "ENG": "England",
    "GBR": "United Kingdom",
    "KOR": "South Korea",
    "SCO": "Scotland",
    "TPE": "Chinese Taipei",
}


def clean_text(value: object) -> str:
    text = str(value or "")
    text = text.replace("\xa0", " ")
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = text.replace("*", "")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_country_code(code: str) -> str:
    value = clean_text(code).upper()
    return COUNTRY_CODE_NORMALIZATION.get(value, value)


def country_aliases() -> dict[str, str]:
    aliases = dict(COUNTRY_NAME_ALIASES)
    try:
        import pycountry

        for country in pycountry.countries:
            aliases[str(country.name)] = normalize_country_code(str(country.alpha_3))
            common_name = getattr(country, "common_name", None)
            if common_name:
                aliases[str(common_name)] = normalize_country_code(str(country.alpha_3))
    except Exception:
        pass
    return aliases


COUNTRY_ALIASES = country_aliases()
COUNTRY_NAMES_BY_LENGTH = sorted(COUNTRY_ALIASES, key=len, reverse=True)


def country_name(country_code: str) -> str:
    code = normalize_country_code(country_code)
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


def fetch_soup(source_url: str) -> BeautifulSoup:
    response = requests.get(source_url, headers=HEADERS, timeout=45)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def result_table(soup: BeautifulSoup, medal_columns: tuple[str, str, str]) -> Tag:
    expected = {"Year", *medal_columns}
    for table in soup.find_all("table", class_=lambda value: value and "wikitable" in value):
        first_row = table.find("tr")
        if first_row is None:
            continue
        headers = [clean_text(cell.get_text(" ", strip=True)) for cell in first_row.find_all(["th", "td"])]
        if expected.issubset(set(headers)):
            return table
    raise RuntimeError(f"Could not find WATC result table with columns {sorted(expected)!r}")


def column_index_map(table: Tag) -> dict[str, int]:
    first_row = table.find("tr")
    if first_row is None:
        raise RuntimeError("Missing result table header.")
    return {clean_text(cell.get_text(" ", strip=True)): idx for idx, cell in enumerate(first_row.find_all(["th", "td"]))}


def is_country_link(link: Tag) -> tuple[str, str] | None:
    label = clean_text(link.get_text(" ", strip=True))
    title = clean_text(link.get("title") or label)
    for candidate in (label, title):
        if candidate in COUNTRY_ALIASES:
            code = normalize_country_code(COUNTRY_ALIASES[candidate])
            return country_name(code), code
    return None


def countries_from_cell(cell: Tag) -> list[tuple[str, str]]:
    text = clean_text(cell.get_text(" ", strip=True))
    lowered = text.lower()
    if "canceled" in lowered or "cancelled" in lowered or "no bronze" in lowered:
        return []

    countries: list[tuple[str, str]] = []
    seen: set[str] = set()
    for link in cell.find_all("a"):
        parsed = is_country_link(link)
        if parsed is None:
            continue
        name, code = parsed
        if code not in seen:
            countries.append((name, code))
            seen.add(code)

    if countries:
        return countries

    for candidate_country in COUNTRY_NAMES_BY_LENGTH:
        if text.startswith(f"{candidate_country} ") or text == candidate_country:
            code = normalize_country_code(COUNTRY_ALIASES[candidate_country])
            return [(country_name(code), code)]

    return []


def parse_year(value: object) -> int | None:
    text = clean_text(value)
    match = re.search(r"\d{4}", text)
    if not match:
        return None
    return int(match.group(0))


def rows_for_trophy(trophy_key: str, config: TrophyConfig, max_year: int) -> list[dict[str, Any]]:
    soup = fetch_soup(config.source_url)
    table = result_table(soup, config.medal_columns)
    column_map = column_index_map(table)
    rows: list[dict[str, Any]] = []
    for tr in table.find_all("tr")[1:]:
        cells = tr.find_all(["th", "td"], recursive=False)
        if not cells:
            continue
        if len(cells) <= max(column_map.values()):
            continue
        year = parse_year(cells[column_map["Year"]].get_text(" ", strip=True))
        if year is None or year < START_YEAR or year > int(max_year):
            continue

        medal_cells = [
            (1, cells[column_map[config.medal_columns[0]]]),
            (2, cells[column_map[config.medal_columns[1]]]),
            (3, cells[column_map[config.medal_columns[2]]]),
        ]
        if any("canceled" in clean_text(cell.get_text(" ", strip=True)).lower() for _, cell in medal_cells):
            continue

        for rank, cell in medal_cells:
            countries = countries_from_cell(cell)
            if not countries:
                continue
            for resolved_country_name, country_code in countries:
                rows.append(
                    {
                        "competition_id": COMPETITION_ID,
                        "competition_name": COMPETITION_NAME,
                        "year": year,
                        "event_date": f"{year}-12-31",
                        "discipline_key": "golf",
                        "discipline_name": "Golf",
                        "event_key": trophy_key,
                        "event_name": config.trophy_name,
                        "gender": config.gender,
                        "rank": rank,
                        "medal": RANK_TO_MEDAL[rank],
                        "participant_type": "team",
                        "participant_name": resolved_country_name,
                        "country_name": resolved_country_name,
                        "country_code": country_code,
                        "source_url": config.source_url,
                    }
                )
    return rows


def build_rows(max_year: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for trophy_key, config in TROPHIES.items():
        rows.extend(rows_for_trophy(trophy_key, config, max_year))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Build World Amateur Team Championships golf podium seed.")
    parser.add_argument("--max-year", type=int, default=2026)
    parser.add_argument("--out", type=Path, default=SEED_PATH)
    args = parser.parse_args()

    rows = build_rows(max(START_YEAR, int(args.max_year)))
    if not rows:
        raise RuntimeError("No World Amateur Team Championships rows extracted.")

    frame = pd.DataFrame(rows)
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype(int)
    frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce").astype(int)
    frame = frame.loc[frame["year"] > 2000].copy()
    frame = frame.sort_values(["year", "gender", "rank", "country_code"]).reset_index(drop=True)

    allowed_profiles = {(1, 2, 3), (1, 2, 2), (1, 2, 3, 3), (1, 2, 3, 3, 3)}
    profiles = (
        frame.groupby(["year", "event_key", "gender"])["rank"]
        .apply(lambda values: tuple(sorted(int(value) for value in values.tolist())))
        .to_dict()
    )
    invalid_profiles = {key: value for key, value in profiles.items() if value not in allowed_profiles}
    if invalid_profiles:
        sample = dict(list(invalid_profiles.items())[:20])
        raise RuntimeError(f"Unexpected WATC rank profile(s): {sample}")

    duplicates = frame.loc[
        frame.duplicated(subset=["year", "event_key", "gender", "country_code"], keep=False)
    ]
    if not duplicates.empty:
        sample = duplicates[["year", "event_key", "gender", "country_code"]].head(20).to_dict("records")
        raise RuntimeError(f"Duplicate WATC country in same event: {sample}")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(args.out, index=False)

    years = sorted(int(year) for year in frame["year"].unique().tolist())
    event_count = frame[["year", "event_key", "gender"]].drop_duplicates().shape[0]
    print(
        f"[seed] world_amateur_team_championships rows={len(frame)} "
        f"years={years[0]}-{years[-1]} events={event_count} out={args.out}"
    )


if __name__ == "__main__":
    main()
