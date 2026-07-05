from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import pycountry
import requests
from bs4 import BeautifulSoup, Tag


ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.core.utils import slugify


OUTPUT_FILE = Path(__file__).with_name("biathlon_world_championships_top3_seed.csv")
SOURCE_URL = "https://en.wikipedia.org/wiki/Biathlon_World_Championships"
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder)"}

COMPETITION_ID = "biathlon_world_championships"
COMPETITION_NAME = "Biathlon World Championships"
DISCIPLINE_KEY = "biathlon"
DISCIPLINE_NAME = "Biathlon"
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}
ALLOWED_PROFILES = {(1, 2, 3), (1, 3)}
COUNTRY_ALIASES = {
    "Belarus": ("Belarus", "BLR"),
    "Czech Republic": ("Czech Republic", "CZE"),
    "Germany": ("Germany", "GER"),
    "Russia": ("Russia", "RUS"),
    "Slovenia": ("Slovenia", "SLO"),
    "South Korea": ("South Korea", "KOR"),
    "United States": ("United States", "USA"),
}
CODE_NAME_OVERRIDES = {
    "BLR": "Belarus",
    "CZE": "Czech Republic",
    "GER": "Germany",
    "RUS": "Russia",
    "SLO": "Slovenia",
}


@dataclass(frozen=True)
class Country:
    name: str
    code: str


def clean_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text.replace("–", "-").strip()


def country_from_code(code: str, title: str = "") -> Country:
    country_code = clean_text(code).upper()
    country_name = clean_text(title) or CODE_NAME_OVERRIDES.get(country_code) or country_code
    country_name = re.sub(r"\s+national.*$", "", country_name, flags=re.IGNORECASE).strip()
    if country_code in CODE_NAME_OVERRIDES:
        return Country(CODE_NAME_OVERRIDES[country_code], country_code)
    country_obj = pycountry.countries.get(alpha_3=country_code)
    if country_obj is not None:
        return Country(getattr(country_obj, "name", country_name), country_code)
    return Country(country_name, country_code)


def country_from_name(name: str) -> Country | None:
    cleaned = clean_text(name)
    if not cleaned:
        return None
    if cleaned in COUNTRY_ALIASES:
        country_name, country_code = COUNTRY_ALIASES[cleaned]
        return Country(country_name, country_code)
    try:
        country_obj = pycountry.countries.lookup(cleaned)
        return Country(getattr(country_obj, "name", cleaned), country_obj.alpha_3)
    except LookupError:
        return None


def is_medal_table(table: Tag) -> bool:
    first_row = table.find("tr")
    if first_row is None:
        return False
    headers = [clean_text(cell.get_text(" ", strip=True)) for cell in first_row.find_all(["th", "td"])]
    return headers[:4] == ["Season", "Winner", "Runner-up", "Third"]


def iter_medal_tables(soup: BeautifulSoup):
    current_h2 = ""
    current_h3 = ""
    for node in soup.select("h2,h3,table.wikitable"):
        if node.name == "h2":
            current_h2 = clean_text(node.get_text(" ", strip=True).replace("[edit]", ""))
            current_h3 = ""
        elif node.name == "h3":
            current_h3 = clean_text(node.get_text(" ", strip=True).replace("[edit]", ""))
        elif is_medal_table(node):
            if current_h2 in {"Men", "Women", "Mixed"} and current_h3:
                yield current_h2, current_h3, node


def event_spec(gender_label: str, event_label: str) -> tuple[str, str, str]:
    gender = gender_label.lower()
    normalized = clean_text(event_label)
    event_key = slugify(normalized)
    participant_type = "team" if re.search(r"\b(relay|team)\b", normalized, flags=re.IGNORECASE) else "athlete"
    return gender, event_key, participant_type


def year_from_cell(cell: Tag) -> int | None:
    match = re.search(r"\b(19\d{2}|20\d{2})\b", clean_text(cell.get_text(" ", strip=True)))
    return int(match.group(1)) if match else None


def country_from_medal_cell(cell: Tag) -> Country | None:
    abbr = cell.find("abbr")
    if abbr is not None:
        return country_from_code(abbr.get_text(" ", strip=True), abbr.get("title") or "")

    for link in cell.find_all("a"):
        country = country_from_name(link.get("title") or link.get_text(" ", strip=True))
        if country is not None:
            return country

    text = clean_text(cell.get_text(" ", strip=True))
    tokens = text.split()
    for size in range(min(4, len(tokens)), 0, -1):
        country = country_from_name(" ".join(tokens[:size]))
        if country is not None:
            return country
    return None


def medal_cell_entry(cell: Tag, participant_type: str) -> tuple[str, Country, str]:
    if clean_text(cell.get_text(" ", strip=True)).lower() in {"none awarded", "not awarded"}:
        raise ValueError("medal_not_awarded")

    country = country_from_medal_cell(cell)
    if country is None:
        raise RuntimeError(f"Could not resolve country from medal cell: {clean_text(cell.get_text(' ', strip=True))!r}")

    athlete_names: list[str] = []
    for link in cell.find_all("a"):
        text = clean_text(link.get_text(" ", strip=True))
        title = clean_text(link.get("title") or "")
        if not text:
            continue
        if country_from_name(title or text) is not None:
            continue
        athlete_names.append(re.sub(r"\s+\(\d+\)$", "", text).strip())

    if participant_type == "team":
        participant_name = country.name
        team_members = " / ".join(athlete_names)
    else:
        if not athlete_names:
            text = clean_text(cell.get_text(" ", strip=True))
            raise RuntimeError(f"Could not resolve athlete from medal cell: {text!r}")
        participant_name = athlete_names[0]
        team_members = ""

    return participant_name, country, team_members


def parse_table(gender_label: str, event_label: str, table: Tag) -> list[dict[str, Any]]:
    gender, event_key, participant_type = event_spec(gender_label, event_label)
    rows: list[dict[str, Any]] = []
    for tr in table.find_all("tr")[1:]:
        cells = tr.find_all(["th", "td"], recursive=False)
        if len(cells) < 4:
            continue
        year = year_from_cell(cells[0])
        if year is None or year <= 2000:
            continue

        for rank, cell in zip([1, 2, 3], cells[1:4]):
            try:
                participant_name, country, team_members = medal_cell_entry(cell, participant_type)
            except ValueError as exc:
                if str(exc) == "medal_not_awarded":
                    continue
                raise
            rows.append(
                {
                    "competition_id": COMPETITION_ID,
                    "competition_name": COMPETITION_NAME,
                    "year": year,
                    "event_date": f"{year}-12-31",
                    "discipline_key": DISCIPLINE_KEY,
                    "discipline_name": DISCIPLINE_NAME,
                    "event_key": event_key,
                    "event_name": f"{COMPETITION_NAME} {gender.title()} {event_label}",
                    "gender": gender,
                    "event_label": event_label,
                    "rank": rank,
                    "medal": RANK_TO_MEDAL[rank],
                    "participant_type": participant_type,
                    "participant_name": participant_name,
                    "country_name": country.name,
                    "country_code": country.code,
                    "team_members": team_members,
                    "source_url": SOURCE_URL,
                }
            )
    return rows


def main() -> None:
    response = requests.get(SOURCE_URL, headers=HEADERS, timeout=60)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    rows: list[dict[str, Any]] = []
    for gender_label, event_label, table in iter_medal_tables(soup):
        rows.extend(parse_table(gender_label, event_label, table))

    frame = pd.DataFrame(rows)
    if frame.empty:
        raise RuntimeError("No Biathlon World Championships podium rows parsed.")
    if (frame["year"] <= 2000).any():
        offenders = frame.loc[frame["year"] <= 2000, ["year", "gender", "event_key"]].head(10).to_dict("records")
        raise RuntimeError(f"Post-2000 guard violated in seed builder: {offenders}")

    frame = frame.drop_duplicates(
        subset=["year", "gender", "event_key", "rank", "participant_name", "country_code"],
        keep="first",
    )
    frame = frame.sort_values(["year", "gender", "event_key", "rank", "country_code", "participant_name"])
    profiles = (
        frame.groupby(["year", "gender", "event_key"])["rank"]
        .apply(lambda values: tuple(sorted(int(value) for value in values.tolist())))
        .to_dict()
    )
    bad_profiles = {key: value for key, value in profiles.items() if value not in ALLOWED_PROFILES}
    if bad_profiles:
        sample = dict(list(bad_profiles.items())[:30])
        raise RuntimeError(f"Unexpected seed rank profiles: {sample}")

    frame.to_csv(OUTPUT_FILE, index=False)
    years = sorted(int(year) for year in frame["year"].unique().tolist())
    events = frame[["year", "gender", "event_key"]].drop_duplicates().shape[0]
    print(f"Wrote {len(frame)} rows, {events} events for {years[0]}-{years[-1]} to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
