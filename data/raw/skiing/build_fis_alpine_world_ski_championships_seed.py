from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag


BASE_DIR = Path(__file__).resolve().parent
SEED_PATH = BASE_DIR / "fis_alpine_world_ski_championships_top3_seed.csv"
SOURCE_URL = "https://en.wikipedia.org/wiki/List_of_FIS_Alpine_World_Ski_Championships_medalists"
ANNUAL_2003_URL = "https://en.wikipedia.org/wiki/FIS_Alpine_World_Ski_Championships_2003"
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder)"}

COMPETITION_ID = "fis_alpine_world_ski_championships"
COMPETITION_NAME = "FIS Alpine World Ski Championships"
DISCIPLINE_KEY = "alpine-skiing"
DISCIPLINE_NAME = "Alpine skiing"
START_YEAR = 2001
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}
ALLOWED_PROFILES = {
    (1, 2, 3),
    (1, 2, 3, 3),
    (1, 1, 3),
    (1, 2, 2),
}

TABLE_SPECS = {
    0: ("men", "downhill", "Downhill", "athlete"),
    1: ("men", "super-g", "Super-G", "athlete"),
    2: ("men", "giant-slalom", "Giant slalom", "athlete"),
    3: ("men", "slalom", "Slalom", "athlete"),
    4: ("women", "downhill", "Downhill", "athlete"),
    5: ("women", "super-g", "Super-G", "athlete"),
    6: ("women", "giant-slalom", "Giant slalom", "athlete"),
    7: ("women", "slalom", "Slalom", "athlete"),
    8: ("mixed", "mixed-team", "Mixed team", "team"),
    10: ("men", "combined", "Combined", "athlete"),
    11: ("women", "combined", "Combined", "athlete"),
}
ANNUAL_2003_TABLE_SPECS = {
    1: ("men", "downhill", "Downhill"),
    2: ("men", "super-g", "Super-G"),
    3: ("men", "giant-slalom", "Giant slalom"),
    4: ("men", "slalom", "Slalom"),
    5: ("men", "combined", "Combined"),
    6: ("women", "downhill", "Downhill"),
    7: ("women", "super-g", "Super-G"),
    8: ("women", "giant-slalom", "Giant slalom"),
    9: ("women", "slalom", "Slalom"),
    10: ("women", "combined", "Combined"),
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


def slugify(value: str) -> str:
    text = clean_text(value).lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-") or "unknown"


COUNTRY_ALIASES = {
    "Czech Republic": ("Czech Republic", "CZE"),
    "United States": ("United States", "USA"),
}


def country_from_label(label: str) -> Country | None:
    text = clean_text(label)
    if not text:
        return None
    alias = COUNTRY_ALIASES.get(text)
    if alias:
        return Country(alias[0], alias[1])

    try:
        import pycountry

        country = pycountry.countries.lookup(text)
        code = getattr(country, "alpha_3", None)
        name = getattr(country, "name", text)
        if code:
            return Country(clean_text(name), str(code).upper())
    except Exception:
        return None

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


def fetch_soup() -> BeautifulSoup:
    response = requests.get(SOURCE_URL, headers=HEADERS, timeout=60)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def fetch_annual_2003_soup() -> BeautifulSoup:
    response = requests.get(ANNUAL_2003_URL, headers=HEADERS, timeout=60)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def source_url_from_year_cell(cell: Tag) -> str:
    for link in cell.find_all("a"):
        href = link.get("href")
        title = clean_text(link.get("title") or "")
        if href and "FIS Alpine World Ski Championships" in title:
            return urljoin(SOURCE_URL, href)
    return SOURCE_URL


def rows_from_individual_cell(cell: Tag) -> list[tuple[str, str, str, str]]:
    medalists: list[tuple[str, str, str, str]] = []
    current_country: Country | None = None
    for link in cell.find_all("a"):
        country = is_country_link(link)
        if country is not None:
            current_country = country
            continue

        athlete_name = clean_text(link.get_text(" ", strip=True))
        if current_country is not None and athlete_name:
            medalists.append((current_country.name, current_country.code, athlete_name, ""))
            current_country = None

    return medalists


def team_name_from_cell(cell: Tag, country: Country, athlete_names: list[str]) -> str:
    text = clean_text(cell.get_text(" ", strip=True))
    label = clean_text(cell.find("a").get_text(" ", strip=True) if cell.find("a") else country.name)
    if not label:
        label = country.name

    prefix = text
    if athlete_names:
        first_athlete = athlete_names[0]
        prefix = text.split(first_athlete, 1)[0].strip()
    if prefix.startswith(label):
        suffix = prefix[len(label) :].strip()
        if suffix:
            return f"{country.name} {suffix}".strip()
    return country.name


def rows_from_team_cell(cell: Tag) -> list[tuple[str, str, str, str]]:
    country: Country | None = None
    athlete_names: list[str] = []
    for link in cell.find_all("a"):
        link_country = is_country_link(link)
        if link_country is not None and country is None:
            country = link_country
            continue

        athlete_name = clean_text(link.get_text(" ", strip=True))
        if athlete_name:
            athlete_names.append(athlete_name)

    if country is None:
        return []

    team_name = team_name_from_cell(cell, country, athlete_names)
    return [(country.name, country.code, team_name, "; ".join(athlete_names))]


def year_from_cell(cell: Tag) -> int | None:
    for link in cell.find_all("a"):
        title = clean_text(link.get("title") or "")
        href = clean_text(link.get("href") or "")
        for candidate in (title, href):
            match = re.search(r"\b(19|20)\d{2}\b", candidate)
            if match:
                return int(match.group(0))

    text = clean_text(cell.get_text(" ", strip=True))
    match = re.search(r"\b(19|20)\d{2}\b", text)
    return int(match.group(0)) if match else None


def parse_standard_table(
    table: Tag,
    gender: str,
    event_key: str,
    event_name: str,
    participant_type: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for tr in table.find_all("tr")[1:]:
        cells = tr.find_all(["th", "td"], recursive=False)
        if len(cells) < 6:
            continue
        year = year_from_cell(cells[2])
        if year is None or year < START_YEAR:
            continue

        source_url = source_url_from_year_cell(cells[2])
        medal_cells = [(1, cells[3]), (2, cells[4]), (3, cells[5])]
        for rank, medal_cell in medal_cells:
            medalists = (
                rows_from_team_cell(medal_cell)
                if participant_type == "team"
                else rows_from_individual_cell(medal_cell)
            )
            for country_name, country_code, participant_name, team_members in medalists:
                rows.append(
                    {
                        "competition_id": COMPETITION_ID,
                        "competition_name": COMPETITION_NAME,
                        "year": year,
                        "event_date": f"{year}-12-31",
                        "discipline_key": DISCIPLINE_KEY,
                        "discipline_name": DISCIPLINE_NAME,
                        "event_key": event_key,
                        "event_name": event_name,
                        "gender": gender,
                        "rank": rank,
                        "medal": RANK_TO_MEDAL[rank],
                        "participant_type": participant_type,
                        "participant_name": participant_name,
                        "country_name": country_name,
                        "country_code": country_code,
                        "team_members": team_members,
                        "source_url": source_url,
                    }
                )
    return rows


def parse_gendered_table(table: Tag, event_key: str, event_name: str, participant_type: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    current_gender = ""
    for tr in table.find_all("tr")[1:]:
        texts = [clean_text(cell.get_text(" ", strip=True)) for cell in tr.find_all(["th", "td"], recursive=False)]
        if len(texts) == 1 and "WOMEN" in texts[0].upper():
            current_gender = "women"
            continue
        if len(texts) == 1 and "MEN" in texts[0].upper():
            current_gender = "men"
            continue
        if current_gender not in {"men", "women"}:
            continue
        rows.extend(parse_standard_table_fragment(tr, current_gender, event_key, event_name, participant_type))
    return rows


def country_from_result_cell(cell: Tag) -> Country | None:
    for link in cell.find_all("a"):
        country = is_country_link(link)
        if country is not None:
            return country
    return country_from_label(clean_text(cell.get_text(" ", strip=True)))


def athlete_from_result_cell(cell: Tag) -> str:
    for link in cell.find_all("a"):
        if is_country_link(link) is not None:
            continue
        text = clean_text(link.get_text(" ", strip=True))
        if text:
            return text
    return clean_text(cell.get_text(" ", strip=True))


def parse_annual_2003_rows() -> list[dict[str, Any]]:
    soup = fetch_annual_2003_soup()
    tables = soup.find_all("table")
    rows: list[dict[str, Any]] = []
    for table_index, (gender, event_key, event_name) in ANNUAL_2003_TABLE_SPECS.items():
        table = tables[table_index]
        current_rank: int | None = None
        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all(["th", "td"], recursive=False)
            if len(cells) < 3:
                continue

            rank_text = clean_text(cells[0].get_text(" ", strip=True))
            if rank_text:
                if not rank_text.isdigit():
                    continue
                current_rank = int(rank_text)
                if current_rank > 3:
                    break
            if current_rank is None or current_rank > 3:
                continue

            country = country_from_result_cell(cells[1])
            athlete_name = athlete_from_result_cell(cells[2])
            if country is None or not athlete_name:
                continue

            rows.append(
                {
                    "competition_id": COMPETITION_ID,
                    "competition_name": COMPETITION_NAME,
                    "year": 2003,
                    "event_date": "2003-12-31",
                    "discipline_key": DISCIPLINE_KEY,
                    "discipline_name": DISCIPLINE_NAME,
                    "event_key": event_key,
                    "event_name": event_name,
                    "gender": gender,
                    "rank": current_rank,
                    "medal": RANK_TO_MEDAL[current_rank],
                    "participant_type": "athlete",
                    "participant_name": athlete_name,
                    "country_name": country.name,
                    "country_code": country.code,
                    "team_members": "",
                    "source_url": ANNUAL_2003_URL,
                }
            )
    return rows


def parse_standard_table_fragment(
    tr: Tag,
    gender: str,
    event_key: str,
    event_name: str,
    participant_type: str,
) -> list[dict[str, Any]]:
    cells = tr.find_all(["th", "td"], recursive=False)
    if len(cells) < 6:
        return []
    year = year_from_cell(cells[2])
    if year is None or year < START_YEAR:
        return []

    source_url = source_url_from_year_cell(cells[2])
    rows: list[dict[str, Any]] = []
    for rank, medal_cell in [(1, cells[3]), (2, cells[4]), (3, cells[5])]:
        medalists = rows_from_team_cell(medal_cell) if participant_type == "team" else rows_from_individual_cell(medal_cell)
        for country_name, country_code, participant_name, team_members in medalists:
            rows.append(
                {
                    "competition_id": COMPETITION_ID,
                    "competition_name": COMPETITION_NAME,
                    "year": year,
                    "event_date": f"{year}-12-31",
                    "discipline_key": DISCIPLINE_KEY,
                    "discipline_name": DISCIPLINE_NAME,
                    "event_key": event_key,
                    "event_name": event_name,
                    "gender": gender,
                    "rank": rank,
                    "medal": RANK_TO_MEDAL[rank],
                    "participant_type": participant_type,
                    "participant_name": participant_name,
                    "country_name": country_name,
                    "country_code": country_code,
                    "team_members": team_members,
                    "source_url": source_url,
                }
            )
    return rows


def build_seed(start_year: int, max_year: int, output: Path) -> pd.DataFrame:
    global START_YEAR
    START_YEAR = int(start_year)
    soup = fetch_soup()
    tables = soup.find_all("table", {"class": "wikitable"})
    if len(tables) < 13:
        raise RuntimeError(f"Expected at least 13 FIS Alpine medalist tables, found {len(tables)}.")

    all_rows: list[dict[str, Any]] = []
    for table_index, spec in TABLE_SPECS.items():
        all_rows.extend(parse_standard_table(tables[table_index], *spec))
    all_rows.extend(parse_gendered_table(tables[9], "team-combined", "Team combined", "team"))
    all_rows.extend(parse_gendered_table(tables[12], "parallel-giant-slalom", "Parallel giant slalom", "athlete"))
    if int(start_year) <= 2003 <= int(max_year):
        all_rows.extend(parse_annual_2003_rows())

    frame = pd.DataFrame(all_rows)
    if frame.empty:
        raise RuntimeError("FIS Alpine seed extraction produced no rows.")

    frame["year"] = pd.to_numeric(frame["year"], errors="coerce")
    frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce")
    frame = frame.dropna(subset=["year", "rank"]).copy()
    frame["year"] = frame["year"].astype(int)
    frame["rank"] = frame["rank"].astype(int)
    frame = frame.loc[(frame["year"] >= int(start_year)) & (frame["year"] <= int(max_year))].copy()
    frame = frame.drop_duplicates(
        subset=[
            "competition_id",
            "year",
            "gender",
            "event_key",
            "rank",
            "participant_type",
            "participant_name",
            "country_code",
        ]
    )
    frame = frame.sort_values(["year", "gender", "event_key", "rank", "country_code", "participant_name"])
    frame = frame.reset_index(drop=True)

    if (frame["year"] <= 2000).any():
        offenders = frame.loc[frame["year"] <= 2000, ["year", "event_key", "gender"]].head(10).to_dict("records")
        raise RuntimeError(f"Post-2000 guard violated for FIS Alpine seed: {offenders}")

    profiles = frame.groupby(["year", "gender", "event_key"])["rank"].apply(
        lambda values: tuple(sorted(int(value) for value in values.tolist()))
    )
    bad_profiles = {key: value for key, value in profiles.items() if value not in ALLOWED_PROFILES}
    if bad_profiles:
        sample = dict(list(bad_profiles.items())[:30])
        raise RuntimeError(f"Unexpected FIS Alpine rank profiles in seed: {sample}")

    output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output, index=False)
    return frame


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build FIS Alpine World Ski Championships podium seed.")
    parser.add_argument("--start-year", type=int, default=START_YEAR)
    parser.add_argument("--max-year", type=int, default=datetime.now(timezone.utc).year)
    parser.add_argument("--output", type=Path, default=SEED_PATH)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    seed = build_seed(start_year=int(args.start_year), max_year=int(args.max_year), output=Path(args.output))
    print(f"[fis-alpine-seed] wrote {args.output} rows={len(seed)} years={seed.year.min()}-{seed.year.max()}")
    print(f"[fis-alpine-seed] events={seed.groupby(['year', 'gender', 'event_key']).ngroups}")
    print(f"[fis-alpine-seed] rows by year:\n{seed.groupby('year').size()}")


if __name__ == "__main__":
    main()
