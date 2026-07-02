from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd
import pycountry
import requests
from bs4 import BeautifulSoup

ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.core.utils import slugify


COMPETITION_ID = "world_karate_championships"
COMPETITION_NAME = "World Karate Championships"
DISCIPLINE_ID = "karate"
DISCIPLINE_NAME = "Karate"
INDEX_URL = "https://en.wikipedia.org/wiki/Karate_World_Championships"
OUTPUT_FILE = Path(__file__).with_name("world_karate_championships_top4_seed.csv")
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder; contact local)"}
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}
COUNTRY_ALIASES = {
    "Czech Republic": ("CZE", "Czech Republic"),
    "Chinese Taipei": ("TPE", "Chinese Taipei"),
    "England": ("ENG", "England"),
    "Hong Kong": ("HKG", "Hong Kong"),
    "Individual Neutral Athletes": ("ANA", "Individual Neutral Athletes"),
    "Iran": ("IRI", "Iran"),
    "Kosovo": ("KOS", "Kosovo"),
    "Macedonia": ("MKD", "North Macedonia"),
    "Macau": ("MAC", "Macau"),
    "Russia": ("RUS", "Russia"),
    "Russian Karate Federation": ("RKF", "Russian Karate Federation"),
    "Scotland": ("SCO", "Scotland"),
    "Slovakia": ("SVK", "Slovakia"),
    "South Korea": ("KOR", "South Korea"),
    "Taiwan": ("TPE", "Chinese Taipei"),
    "Turkey": ("TUR", "Türkiye"),
    "United States": ("USA", "United States"),
    "Venezuela": ("VEN", "Venezuela"),
    "Vietnam": ("VIE", "Vietnam"),
    "World Karate Federation-1": ("WKF1", "World Karate Federation-1"),
    "World Karate Federation-2": ("WKF2", "World Karate Federation-2"),
    "Yugoslavia": ("YUG", "Yugoslavia"),
}
COUNTRY_NAME_BY_CODE = {code: name for code, name in COUNTRY_ALIASES.values()}


def clean_text(value: str) -> str:
    value = re.sub(r"\[[^\]]+\]", "", value)
    return re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip()


def country_from_name(name: str) -> tuple[str, str] | None:
    cleaned = clean_text(name).strip("*")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if cleaned in COUNTRY_ALIASES:
        return COUNTRY_ALIASES[cleaned]
    try:
        country = pycountry.countries.lookup(cleaned)
        return country.alpha_3, getattr(country, "name", cleaned)
    except LookupError:
        return None


def edition_links() -> list[tuple[int, str]]:
    soup = BeautifulSoup(requests.get(INDEX_URL, headers=HEADERS, timeout=60).text, "html.parser")
    links: dict[int, str] = {}
    for anchor in soup.find_all("a", href=True):
        title = anchor.get("title") or anchor.get_text(" ", strip=True)
        match = re.match(r"((?:19|20)\d{2}) World Karate Championships$", title)
        if not match:
            continue
        year = int(match.group(1))
        if year > 2000:
            links[year] = "https://en.wikipedia.org" + anchor["href"]
    return sorted(links.items())


def heading_for_table(table) -> str:
    node = table
    while node:
        node = node.find_previous(["h2", "h3", "h4"])
        if not node:
            break
        text = clean_text(node.get_text(" ", strip=True).replace("[edit]", ""))
        if text:
            return text
    return ""


def medalist_links(cell) -> list[str]:
    values = []
    for anchor in cell.find_all("a"):
        text = clean_text(anchor.get_text(" ", strip=True))
        if text and text.lower() != "details":
            values.append(text)
    return values


def country_from_cell(cell) -> tuple[str, str]:
    text = clean_text(cell.get_text(" ", strip=True))
    if "World Karate Federation -1" in text:
        return COUNTRY_ALIASES["World Karate Federation-1"]
    if "World Karate Federation -2" in text:
        return COUNTRY_ALIASES["World Karate Federation-2"]

    candidates = medalist_links(cell)
    for value in reversed(candidates):
        match = country_from_name(value)
        if match:
            return match
    for country_name, match in sorted(COUNTRY_ALIASES.items(), key=lambda item: len(item[0]), reverse=True):
        if re.search(rf"\b{re.escape(country_name)}(?:\s+\*)?$", text):
            return match
    tokens = text.strip("*").split()
    for size in range(min(4, len(tokens)), 0, -1):
        suffix = " ".join(tokens[-size:])
        match = country_from_name(suffix)
        if match:
            return match
    raise RuntimeError(f"Could not resolve country from medalist cell: {text!r}")


def participant_from_cell(cell, event_name: str, gender: str) -> tuple[str, str, str, str, str]:
    country_code, country_name = country_from_cell(cell)
    text = clean_text(cell.get_text(" ", strip=True))
    links = medalist_links(cell)
    country_names = {country_name, *(COUNTRY_NAME_BY_CODE.get(country_code, country_name),)}

    is_team = "team" in event_name.lower()
    if is_team:
        participant_type = "team"
        participant_name = f"{country_name} {gender.title()} {event_name}"
        roster = text
    else:
        participant_type = "athlete"
        athlete_links = []
        for value in links:
            if value in country_names:
                continue
            if country_from_name(value):
                continue
            if value == "World Karate Federation":
                continue
            athlete_links.append(re.sub(r" \(page does not exist\)$", "", value))
        if athlete_links:
            participant_name = athlete_links[0]
        else:
            suffixes = sorted(country_names | {"World Karate Federation"}, key=len, reverse=True)
            participant_name = text
            for suffix in suffixes:
                participant_name = re.sub(rf"\s+{re.escape(suffix)}(?:\s+-[12])?$", "", participant_name).strip()
        roster = ""

    return participant_type, participant_name, country_name, country_code, roster or text


def event_key_from_name(event_name: str) -> str:
    normalized = clean_text(event_name)
    normalized = normalized.replace("−", "-").replace("+", "plus")
    normalized = re.sub(r"\s*details$", "", normalized, flags=re.IGNORECASE).strip()
    normalized = normalized.replace("Individual kata", "kata")
    normalized = normalized.replace("Kata", "kata")
    return slugify(normalized)


def parse_medal_table(year: int, url: str, gender: str, table) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    body_rows = table.find_all("tr")[1:]
    index = 0
    while index < len(body_rows):
        cells = body_rows[index].find_all(["td", "th"])
        if len(cells) < 4:
            index += 1
            continue

        event_name = re.sub(r"\s*details$", "", clean_text(cells[0].get_text(" ", strip=True)), flags=re.IGNORECASE)
        event_key = event_key_from_name(event_name)
        medal_cells = [cells[1], cells[2], cells[3]]
        if index + 1 < len(body_rows):
            bronze2_cells = body_rows[index + 1].find_all(["td", "th"])
            if len(bronze2_cells) == 1:
                medal_cells.append(bronze2_cells[0])

        for rank, cell in zip([1, 2, 3, 3], medal_cells):
            participant_type, participant_name, country_name, country_code, source_text = participant_from_cell(
                cell, event_name, gender
            )
            rows.append(
                {
                    "competition_id": COMPETITION_ID,
                    "competition_name": COMPETITION_NAME,
                    "year": year,
                    "event_date": f"{year}-12-31",
                    "discipline_key": DISCIPLINE_ID,
                    "discipline_name": DISCIPLINE_NAME,
                    "event_key": event_key,
                    "event_name": f"{COMPETITION_NAME} {gender.title()} {event_name}",
                    "gender": gender,
                    "event_label": event_name,
                    "rank": rank,
                    "medal": RANK_TO_MEDAL[rank],
                    "participant_type": participant_type,
                    "participant_name": participant_name,
                    "country_name": country_name,
                    "country_code": country_code,
                    "source_text": source_text,
                    "source_url": url,
                }
            )

        index += 2 if len(medal_cells) == 4 else 1

    return rows


def parse_edition(year: int, url: str) -> list[dict[str, object]]:
    soup = BeautifulSoup(requests.get(url, headers=HEADERS, timeout=60).text, "html.parser")
    rows: list[dict[str, object]] = []
    for table in soup.select("table.wikitable"):
        heading = heading_for_table(table).lower()
        if heading not in {"men", "women"}:
            continue
        header = [clean_text(cell.get_text(" ", strip=True)) for cell in table.find_all("tr")[0].find_all(["th", "td"])]
        if header[:4] != ["Event", "Gold", "Silver", "Bronze"]:
            continue
        # 2025 also has Para Karate Men/Women tables later in the page; keep only senior championship events.
        first_event = clean_text(table.find_all("tr")[1].get_text(" ", strip=True)) if len(table.find_all("tr")) > 1 else ""
        if first_event.startswith("K-"):
            continue
        rows.extend(parse_medal_table(year, url, heading, table))
    return rows


def main() -> None:
    rows: list[dict[str, object]] = []
    for year, url in edition_links():
        rows.extend(parse_edition(year, url))

    frame = pd.DataFrame(rows)
    if frame.empty:
        raise RuntimeError("No World Karate Championships podium rows parsed.")
    if (frame["year"] <= 2000).any():
        offenders = frame.loc[frame["year"] <= 2000, ["year", "gender", "event_key"]].head(10).to_dict("records")
        raise RuntimeError(f"Post-2000 guard violated in seed builder: {offenders}")

    frame = frame.sort_values(["year", "gender", "event_key", "rank", "country_code", "participant_name"])
    profiles = (
        frame.groupby(["year", "gender", "event_key"])["rank"]
        .apply(lambda values: tuple(sorted(int(value) for value in values.tolist())))
        .to_dict()
    )
    bad_profiles = {key: value for key, value in profiles.items() if value != (1, 2, 3, 3)}
    if bad_profiles:
        sample = dict(list(bad_profiles.items())[:30])
        raise RuntimeError(f"Unexpected seed rank profiles: {sample}")

    frame.to_csv(OUTPUT_FILE, index=False)
    years = sorted(int(year) for year in frame["year"].unique().tolist())
    events = frame[["year", "gender", "event_key"]].drop_duplicates().shape[0]
    print(f"Wrote {len(frame)} rows, {events} events for {years[0]}-{years[-1]} to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
