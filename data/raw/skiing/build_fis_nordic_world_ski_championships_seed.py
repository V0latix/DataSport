from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag


BASE_DIR = Path(__file__).resolve().parent
SEED_PATH = BASE_DIR / "fis_nordic_world_ski_championships_top3_seed.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder)"}

COMPETITION_ID = "fis_nordic_world_ski_championships"
COMPETITION_NAME = "FIS Nordic World Ski Championships"
START_YEAR = 2001
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}

PAGES = [
    {
        "url": "https://en.wikipedia.org/wiki/List_of_FIS_Nordic_World_Ski_Championships_medalists_in_men%27s_cross-country_skiing",
        "discipline_key": "cross-country-skiing",
        "discipline_name": "Cross-country skiing",
        "gender": "men",
    },
    {
        "url": "https://en.wikipedia.org/wiki/List_of_FIS_Nordic_World_Ski_Championships_medalists_in_women%27s_cross-country_skiing",
        "discipline_key": "cross-country-skiing",
        "discipline_name": "Cross-country skiing",
        "gender": "women",
    },
    {
        "url": "https://en.wikipedia.org/wiki/List_of_FIS_Nordic_World_Ski_Championships_medalists_in_nordic_combined",
        "discipline_key": "nordic-combined-skiing",
        "discipline_name": "Nordic combined",
        "gender_by_table": {
            0: "men",
            2: "men",
            4: "men",
            6: "men",
            8: "men",
            10: "women",
            12: "women",
            14: "mixed",
        },
    },
    {
        "url": "https://en.wikipedia.org/wiki/List_of_FIS_Nordic_World_Ski_Championships_medalists_in_ski_jumping",
        "discipline_key": "ski-jumping",
        "discipline_name": "Ski jumping",
        "gender_by_table": {
            0: "men",
            2: "men",
            4: "men",
            6: "men",
            8: "women",
            10: "women",
            12: "women",
            14: "mixed",
            16: "mixed",
        },
    },
]


@dataclass(frozen=True)
class Country:
    name: str
    code: str


COUNTRY_ALIASES = {
    "Czech Republic": ("Czech Republic", "CZE"),
    "United States": ("United States", "USA"),
    "Russia": ("Russia", "RUS"),
    "Soviet Union": ("Soviet Union", "URS"),
    "East Germany": ("East Germany", "GDR"),
    "West Germany": ("West Germany", "FRG"),
    "Czechoslovakia": ("Czechoslovakia", "TCH"),
    "Yugoslavia": ("Yugoslavia", "YUG"),
}


def clean_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = re.sub(r"\(\d+\)", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text.replace("–", "-").strip()


def slugify(value: str) -> str:
    text = clean_text(value).lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-") or "unknown"


def country_from_label(label: str) -> Country | None:
    text = clean_text(label)
    if not text:
        return None
    if text in COUNTRY_ALIASES:
        name, code = COUNTRY_ALIASES[text]
        return Country(name, code)
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
    for candidate in [clean_text(link.get("title") or ""), clean_text(link.get_text(" ", strip=True))]:
        country = country_from_label(candidate)
        if country is not None:
            return country
    return None


def fetch_soup(url: str) -> BeautifulSoup:
    response = requests.get(url, headers=HEADERS, timeout=60)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def heading_for_table(table: Tag) -> str:
    heading = table.find_previous(["h2", "h3", "h4"])
    if heading is None:
        return "unknown"
    return clean_text(heading.get_text(" ", strip=True)).replace("[ edit ]", "").strip()


def source_url_from_year_cell(cell: Tag, page_url: str) -> str:
    for link in cell.find_all("a"):
        href = link.get("href")
        if href:
            return urljoin(page_url, href)
    return page_url


def year_from_cell(cell: Tag) -> int | None:
    text = clean_text(cell.get_text(" ", strip=True))
    match = re.search(r"\b(19|20)\d{2}\b", text)
    if match:
        return int(match.group(0))
    for link in cell.find_all("a"):
        for candidate in [clean_text(link.get("title") or ""), clean_text(link.get("href") or "")]:
            match = re.search(r"\b(19|20)\d{2}\b", candidate)
            if match:
                return int(match.group(0))
    return None


def medal_segments(cell: Tag) -> list[tuple[Country, list[str]]]:
    segments: list[tuple[Country, list[str]]] = []
    current_country: Country | None = None
    current_athletes: list[str] = []
    pending_athletes: list[str] = []

    def flush() -> None:
        nonlocal current_country, current_athletes
        if current_country is not None:
            segments.append((current_country, current_athletes.copy()))
        current_country = None
        current_athletes = []

    for link in cell.find_all("a"):
        country = is_country_link(link)
        if country is not None:
            if current_country is not None:
                flush()
            current_country = country
            if pending_athletes:
                current_athletes.extend(pending_athletes)
                pending_athletes = []
                flush()
            continue

        athlete_name = clean_text(link.get_text(" ", strip=True))
        if not athlete_name:
            continue
        if current_country is None:
            pending_athletes.append(athlete_name)
        else:
            current_athletes.append(athlete_name)

    if current_country is not None:
        flush()
    return segments


def participant_from_segment(country: Country, athletes: list[str]) -> tuple[str, str, str]:
    clean_athletes = [clean_text(name) for name in athletes if clean_text(name)]
    if len(clean_athletes) <= 1:
        return "athlete", clean_athletes[0] if clean_athletes else country.name, ""
    return "team", country.name, "; ".join(clean_athletes)


def parse_medal_table(
    table: Tag,
    page_url: str,
    discipline_key: str,
    discipline_name: str,
    gender: str,
    event_name: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    current_event_name = event_name
    for tr in table.find_all("tr")[1:]:
        cells = tr.find_all(["th", "td"], recursive=False)
        if len(cells) < 4:
            continue
        year = year_from_cell(cells[0])
        if year is None:
            label = clean_text(cells[0].get_text(" ", strip=True))
            if label and not re.search(r"\bnot included\b", label, flags=re.IGNORECASE):
                current_event_name = label
            continue
        if year < START_YEAR:
            continue
        row_event_name = current_event_name
        first_cell_text = clean_text(cells[0].get_text(" ", strip=True))
        qualifier_match = re.search(r"\(([^)]*(?:normal hill|large hill)[^)]*)\)", first_cell_text, flags=re.IGNORECASE)
        if qualifier_match:
            row_event_name = f"{event_name} {clean_text(qualifier_match.group(1))}"
        event_key = slugify(row_event_name)
        source_url = source_url_from_year_cell(cells[0], page_url)
        for rank, medal_cell in [(1, cells[-3]), (2, cells[-2]), (3, cells[-1])]:
            for country, athletes in medal_segments(medal_cell):
                if not athletes:
                    continue
                participant_type, participant_name, team_members = participant_from_segment(country, athletes)
                rows.append(
                    {
                        "competition_id": COMPETITION_ID,
                        "competition_name": COMPETITION_NAME,
                        "year": year,
                        "event_date": f"{year}-12-31",
                        "discipline_key": discipline_key,
                        "discipline_name": discipline_name,
                        "event_key": event_key,
                        "event_name": row_event_name,
                        "gender": gender,
                        "rank": rank,
                        "medal": RANK_TO_MEDAL[rank],
                        "participant_type": participant_type,
                        "participant_name": participant_name,
                        "country_name": country.name,
                        "country_code": country.code,
                        "team_members": team_members,
                        "source_url": source_url,
                    }
                )
    return rows


def is_medal_table(table: Tag) -> bool:
    header = [clean_text(cell.get_text(" ", strip=True)).lower() for cell in table.find_all("tr")[0].find_all(["th", "td"], recursive=False)]
    return bool(header) and header[-3:] == ["gold", "silver", "bronze"] and header[0] in {"championships", "edition"}


def build_seed() -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for page in PAGES:
        soup = fetch_soup(str(page["url"]))
        medal_table_index = 0
        for table in soup.find_all("table", class_="wikitable"):
            if not is_medal_table(table):
                continue
            gender = page.get("gender")
            if gender is None:
                gender = page["gender_by_table"].get(medal_table_index)
            medal_table_index += 2
            if gender is None:
                continue
            rows.extend(
                parse_medal_table(
                    table=table,
                    page_url=str(page["url"]),
                    discipline_key=str(page["discipline_key"]),
                    discipline_name=str(page["discipline_name"]),
                    gender=str(gender),
                    event_name=heading_for_table(table),
                )
            )

    frame = pd.DataFrame(rows)
    if frame.empty:
        raise RuntimeError("No FIS Nordic World Ski Championships rows extracted.")
    frame = frame.sort_values(["year", "discipline_key", "gender", "event_key", "rank", "country_code", "participant_name"])
    profiles = (
        frame.groupby(["year", "discipline_key", "gender", "event_key"])["rank"]
        .apply(lambda values: tuple(sorted(int(value) for value in values.tolist())))
        .to_dict()
    )
    allowed_profiles = {(1, 2, 3), (1, 2, 3, 3), (1, 2, 2), (1, 1, 3)}
    bad_profiles = {key: value for key, value in profiles.items() if value not in allowed_profiles}
    if bad_profiles:
        sample = dict(list(bad_profiles.items())[:30])
        raise RuntimeError(f"Unexpected FIS Nordic rank profiles: {sample}")
    return frame.reset_index(drop=True)


def main() -> None:
    frame = build_seed()
    frame.to_csv(SEED_PATH, index=False)
    print(f"Wrote {len(frame)} rows to {SEED_PATH}")
    print(frame.groupby("discipline_key")["year"].agg(["min", "max", "nunique"]).to_string())
    print(f"events={frame.groupby(['year', 'discipline_key', 'gender', 'event_key']).ngroups}")


if __name__ == "__main__":
    main()
