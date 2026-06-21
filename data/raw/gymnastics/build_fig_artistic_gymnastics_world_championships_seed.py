from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag


BASE_DIR = Path(__file__).resolve().parent
SEED_PATH = BASE_DIR / "fig_artistic_gymnastics_world_championships_top3_seed.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder)"}

COMPETITION_ID = "fig_artistic_gymnastics_world_championships"
COMPETITION_NAME = "FIG Artistic Gymnastics World Championships"
START_YEAR = 2001
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}

EVENT_NAME_ALIASES = {
    "All-around": "Individual all-around",
    "Floor": "Floor exercise",
    "High bar": "Horizontal bar",
    "Rings": "Still rings",
    "Team all-around": "Team",
}
COUNTRY_ALIASES = {
    "AIN": ("Individual Neutral Athletes", "AIN"),
    "BUL": ("Bulgaria", "BGR"),
    "CRO": ("Croatia", "HRV"),
    "GER": ("Germany", "DEU"),
    "Individual Neutral Athletes": ("Individual Neutral Athletes", "AIN"),
    "Great Britain": ("Great Britain", "GBR"),
    "GRE": ("Greece", "GRC"),
    "United Kingdom": ("Great Britain", "GBR"),
    "Hong Kong": ("Hong Kong", "HKG"),
    "NED": ("Netherlands", "NLD"),
    "North Korea": ("North Korea", "PRK"),
    "Russia": ("Russia", "RUS"),
    "Republic of Ireland": ("Ireland", "IRL"),
    "Russian Federation": ("Russia", "RUS"),
    "Russian Gymnastics Federation": ("Russian Gymnastics Federation", "RGF"),
    "South Korea": ("South Korea", "KOR"),
    "SLO": ("Slovenia", "SVN"),
    "SUI": ("Switzerland", "CHE"),
    "Chinese Taipei": ("Chinese Taipei", "TPE"),
    "Turkey": ("Turkey", "TUR"),
    "Viet Nam": ("Vietnam", "VIE"),
    "Vietnam": ("Vietnam", "VIE"),
}
COUNTRY_NAME_PREFIXES: list[tuple[str, Country]] | None = None
SOURCE_YEARS = [
    2001,
    2002,
    2003,
    2005,
    2006,
    2007,
    2009,
    2010,
    2011,
    2013,
    2014,
    2015,
    2017,
    2018,
    2019,
    2021,
    2022,
    2023,
    2025,
    2026,
]


@dataclass(frozen=True)
class Country:
    name: str
    code: str


def clean_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = text.replace("–", "-")
    return text.strip()


def slugify(value: str) -> str:
    text = clean_text(value).lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-") or "unknown"


def country_from_label(label: str) -> Country | None:
    text = clean_text(label)
    if not text:
        return None
    alias = COUNTRY_ALIASES.get(text)
    if alias:
        return Country(alias[0], alias[1])

    if re.fullmatch(r"[A-Z]{3}", text):
        country = country_from_code(text)
        if country is not None:
            return country

    try:
        import pycountry

        country = pycountry.countries.lookup(text)
        code = getattr(country, "alpha_3", None)
        if code:
            return Country(text, str(code).upper())
    except Exception:
        return None

    return None


def country_from_code(code: str) -> Country | None:
    normalized = clean_text(code).upper()
    alias = COUNTRY_ALIASES.get(normalized)
    if alias:
        return Country(alias[0], alias[1])
    for _label, (name, alias_code) in COUNTRY_ALIASES.items():
        if normalized == alias_code:
            return Country(name, alias_code)

    try:
        import pycountry

        country = pycountry.countries.lookup(normalized)
        alpha_3 = getattr(country, "alpha_3", None)
        name = clean_text(getattr(country, "name", ""))
        if alpha_3 and name:
            return Country(name, str(alpha_3).upper())
    except Exception:
        return None

    return None


def country_name_prefixes() -> list[tuple[str, Country]]:
    global COUNTRY_NAME_PREFIXES
    if COUNTRY_NAME_PREFIXES is not None:
        return COUNTRY_NAME_PREFIXES

    countries: dict[str, Country] = {}
    for label in COUNTRY_ALIASES:
        country = country_from_label(label)
        if country is not None:
            countries[label] = country

    try:
        import pycountry

        for country in pycountry.countries:
            code = getattr(country, "alpha_3", "")
            if not code:
                continue
            for attr in ("name", "official_name", "common_name"):
                name = clean_text(getattr(country, attr, ""))
                if name:
                    countries.setdefault(name, Country(name, str(code).upper()))
    except Exception:
        pass

    COUNTRY_NAME_PREFIXES = sorted(countries.items(), key=lambda item: len(item[0]), reverse=True)
    return COUNTRY_NAME_PREFIXES


def country_from_cell_text(cell: Tag) -> Country | None:
    text = clean_text(cell.get_text(" ", strip=True))
    if not text:
        return None
    for label, country in country_name_prefixes():
        if text == label or text.startswith(f"{label} "):
            return country
    return None


def is_country_link(link: Tag) -> Country | None:
    candidates = [
        clean_text(link.get("title") or ""),
        clean_text(link.get_text(" ", strip=True)),
    ]
    for candidate in candidates:
        country = country_from_label(candidate)
        if country is not None:
            return country
    return None


def event_name(raw_event: str) -> str:
    text = clean_text(raw_event)
    text = re.sub(r"\s+details$", "", text, flags=re.IGNORECASE).strip()
    return EVENT_NAME_ALIASES.get(text, text)


def discipline_key(name: str) -> str:
    return slugify(name)


def discipline_name(name: str) -> str:
    return f"Artistic Gymnastics - {name}"


def source_url(year: int) -> str:
    return f"https://en.wikipedia.org/wiki/{year}_World_Artistic_Gymnastics_Championships"


def fetch_soup(year: int) -> BeautifulSoup:
    response = requests.get(source_url(year), headers=HEADERS, timeout=60)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def medal_table(soup: BeautifulSoup, year: int) -> Tag:
    for table in soup.find_all("table"):
        first_row = table.find("tr")
        if first_row is None:
            continue
        headers = [clean_text(cell.get_text(" ", strip=True)) for cell in first_row.find_all(["th", "td"], recursive=False)]
        if headers[:4] == ["Event", "Gold", "Silver", "Bronze"]:
            return table
    raise RuntimeError(f"Could not find medalist table for FIG artistic gymnastics {year}.")


def medalists_from_cell(cell: Tag, event: str) -> list[tuple[str, str, str]]:
    countries_and_links: list[tuple[Country | None, Tag]] = []
    for link in cell.find_all("a"):
        if clean_text(link.get_text(" ", strip=True)).lower() == "details":
            continue
        countries_and_links.append((is_country_link(link), link))

    if "team" in event.lower():
        for country, _link in countries_and_links:
            if country is not None:
                return [(country.name, country.code, country.name)]
        inferred_country = country_from_cell_text(cell)
        if inferred_country is not None:
            return [(inferred_country.name, inferred_country.code, inferred_country.name)]
        return []

    medalists: list[tuple[str, str, str]] = []
    current_country: Country | None = None
    athlete_links = [
        link
        for country, link in countries_and_links
        if country is None and clean_text(link.get_text(" ", strip=True)).lower() != "details"
    ]
    inline_codes = re.findall(r"\(\s*([A-Z]{3})\s*\)", clean_text(cell.get_text(" ", strip=True)))
    if athlete_links and inline_codes and len(athlete_links) == len(inline_codes):
        for link, code in zip(athlete_links, inline_codes):
            country = country_from_code(code)
            athlete_name = clean_text(link.get_text(" ", strip=True))
            if country is not None and athlete_name:
                medalists.append((country.name, country.code, athlete_name))
        if medalists:
            return medalists

    for country, link in countries_and_links:
        if country is not None:
            current_country = country
            continue
        athlete_name = clean_text(link.get_text(" ", strip=True))
        title = clean_text(link.get("title") or "")
        if not athlete_name:
            athlete_name = re.sub(r"\s*\(.*?\)$", "", title).strip()
        if current_country is not None and athlete_name:
            medalists.append((current_country.name, current_country.code, athlete_name))
            current_country = None
    if medalists:
        return medalists

    for index, (country, link) in enumerate(countries_and_links[:-1]):
        next_country, _next_link = countries_and_links[index + 1]
        athlete_name = clean_text(link.get_text(" ", strip=True))
        if country is None and next_country is not None and athlete_name:
            medalists.append((next_country.name, next_country.code, athlete_name))

    return medalists


def rows_for_year(year: int) -> list[dict[str, Any]]:
    soup = fetch_soup(year)
    table = medal_table(soup, year)
    rows: list[dict[str, Any]] = []
    current_gender = ""
    source = source_url(year)

    for tr in table.find_all("tr")[1:]:
        cells = tr.find_all(["th", "td"], recursive=False)
        texts = [clean_text(cell.get_text(" ", strip=True)) for cell in cells]
        if not texts:
            continue
        if len(cells) == 1 and texts[0].lower() in {"men", "women"}:
            current_gender = texts[0].lower()
            continue
        if len(cells) < 4 or current_gender not in {"men", "women"}:
            continue

        event = event_name(texts[0])
        if not event:
            continue

        medal_columns = [(1, cells[1]), (2, cells[2]), (3, cells[3])]
        for rank, medal_cell in medal_columns:
            for country_name, country_code, participant_name in medalists_from_cell(medal_cell, event):
                participant_type = "team" if "team" in event.lower() else "athlete"
                rows.append(
                    {
                        "competition_id": COMPETITION_ID,
                        "competition_name": COMPETITION_NAME,
                        "year": year,
                        "event_date": f"{year}-12-31",
                        "discipline_key": discipline_key(event),
                        "discipline_name": discipline_name(event),
                        "event_name": event,
                        "gender": current_gender,
                        "rank": rank,
                        "medal": RANK_TO_MEDAL[rank],
                        "participant_type": participant_type,
                        "participant_name": participant_name,
                        "country_name": country_name,
                        "country_code": country_code,
                        "source_url": source,
                    }
                )

    return rows


def build_seed(start_year: int, max_year: int, output: Path) -> pd.DataFrame:
    all_rows: list[dict[str, Any]] = []
    empty_years: list[int] = []
    target_years = [year for year in SOURCE_YEARS if start_year <= year <= max_year]
    for year in target_years:
        year_rows = rows_for_year(year)
        if not year_rows:
            empty_years.append(year)
            continue
        all_rows.extend(year_rows)

    frame = pd.DataFrame(all_rows)
    if frame.empty:
        raise RuntimeError("FIG artistic gymnastics seed extraction produced no rows.")

    frame = frame.loc[frame["year"].astype(int) > 2000].copy()
    frame = frame.drop_duplicates(
        subset=[
            "competition_id",
            "year",
            "gender",
            "discipline_key",
            "rank",
            "participant_type",
            "participant_name",
            "country_code",
        ]
    )
    frame = frame.sort_values(["year", "gender", "discipline_key", "rank", "country_code", "participant_name"])
    frame = frame.reset_index(drop=True)

    profiles = frame.groupby(["year", "gender", "discipline_key"])["rank"].apply(
        lambda values: tuple(sorted(int(value) for value in values.tolist()))
    )
    allowed_profiles = {
        (1, 2, 3),
        (1, 2, 3, 3),
        (1, 1, 3),
        (1, 2, 2),
        (1, 1, 3, 3),
        (1, 1, 1, 1),
    }
    bad_profiles = {key: value for key, value in profiles.items() if value not in allowed_profiles}
    if bad_profiles:
        sample = dict(list(bad_profiles.items())[:20])
        raise RuntimeError(f"Unexpected FIG rank profiles in seed: {sample}")

    output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output, index=False)
    if empty_years:
        print(f"[fig-artistic-seed] skipped empty/incomplete years: {empty_years}")
    return frame


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build FIG Artistic Gymnastics World Championships podium seed.")
    parser.add_argument("--start-year", type=int, default=START_YEAR)
    parser.add_argument("--max-year", type=int, default=datetime.now(timezone.utc).year)
    parser.add_argument("--output", type=Path, default=SEED_PATH)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    seed = build_seed(start_year=int(args.start_year), max_year=int(args.max_year), output=Path(args.output))
    print(f"[fig-artistic-seed] wrote {args.output} rows={len(seed)} years={seed.year.min()}-{seed.year.max()}")
    print(f"[fig-artistic-seed] events={seed.groupby(['year', 'gender', 'discipline_key']).ngroups}")
    print(f"[fig-artistic-seed] rows by year:\n{seed.groupby('year').size()}")


if __name__ == "__main__":
    main()
