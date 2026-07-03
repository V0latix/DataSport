from __future__ import annotations

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import pycountry
import requests
from bs4 import BeautifulSoup

ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.core.utils import slugify


DISCIPLINE_ID = "boxing"
DISCIPLINE_NAME = "Boxing"
OUTPUT_FILE = Path(__file__).with_name("world_boxing_championships_top4_seed.csv")
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder; contact local)"}
INDEX_SOURCES = [
    {
        "competition_id": "iba_mens_world_boxing_championships",
        "competition_name": "IBA Men's World Boxing Championships",
        "gender": "men",
        "index_url": "https://en.wikipedia.org/wiki/IBA_Men%27s_World_Boxing_Championships",
    },
    {
        "competition_id": "iba_womens_world_boxing_championships",
        "competition_name": "IBA Women's World Boxing Championships",
        "gender": "women",
        "index_url": "https://en.wikipedia.org/wiki/IBA_Women%27s_World_Boxing_Championships",
    },
]
WORLD_BOXING_2025_URL = "https://en.wikipedia.org/wiki/2025_World_Boxing_Championships"
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}
COUNTRY_ALIASES = {
    "Chinese Taipei": ("TPE", "Chinese Taipei"),
    "Czech Republic": ("CZE", "Czech Republic"),
    "England": ("ENG", "England"),
    "Great Britain": ("GBR", "Great Britain"),
    "Hong Kong": ("HKG", "Hong Kong"),
    "Iran": ("IRI", "Iran"),
    "Ireland": ("IRL", "Ireland"),
    "Kosovo": ("KOS", "Kosovo"),
    "North Korea": ("PRK", "North Korea"),
    "Russia": ("RUS", "Russia"),
    "Republic of Ireland": ("IRL", "Ireland"),
    "Serbia and Montenegro": ("SCG", "Serbia and Montenegro"),
    "Scotland": ("SCO", "Scotland"),
    "South Korea": ("KOR", "South Korea"),
    "Thailand Boxing Federation": ("TBF", "Thailand Boxing Federation"),
    "Turkey": ("TUR", "Türkiye"),
    "United States": ("USA", "United States"),
    "Vietnam": ("VIE", "Vietnam"),
    "Wales": ("WAL", "Wales"),
}
COUNTRY_NAME_BY_CODE = {code: name for code, name in COUNTRY_ALIASES.values()}
_COUNTRY_CANDIDATES: list[tuple[str, str, str]] | None = None


def clean_text(value: str) -> str:
    value = re.sub(r"\[[^\]]+\]", "", value)
    return re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip()


def country_from_name(name: str) -> tuple[str, str] | None:
    cleaned = clean_text(name).strip("*")
    cleaned = re.sub(r"\s+\(country\)$", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if cleaned in COUNTRY_ALIASES:
        return COUNTRY_ALIASES[cleaned]
    try:
        country = pycountry.countries.lookup(cleaned)
        return country.alpha_3, getattr(country, "name", cleaned)
    except LookupError:
        return None


def country_candidates() -> list[tuple[str, str, str]]:
    global _COUNTRY_CANDIDATES
    if _COUNTRY_CANDIDATES is not None:
        return _COUNTRY_CANDIDATES

    candidates: dict[str, tuple[str, str, str]] = {}
    for name, (code, canonical_name) in COUNTRY_ALIASES.items():
        candidates[name] = (name, code, canonical_name)
    for country in pycountry.countries:
        names = {country.name}
        official_name = getattr(country, "official_name", None)
        if official_name:
            names.add(official_name)
        common_name = getattr(country, "common_name", None)
        if common_name:
            names.add(common_name)
        for name in names:
            candidates.setdefault(name, (name, country.alpha_3, country.name))

    _COUNTRY_CANDIDATES = sorted(candidates.values(), key=lambda item: len(item[0]), reverse=True)
    return _COUNTRY_CANDIDATES


def country_mentions(text: str) -> list[tuple[int, int, str, str]]:
    mentions: list[tuple[int, int, str, str]] = []
    occupied: list[tuple[int, int]] = []
    for country_name, country_code, canonical_name in country_candidates():
        pattern = rf"(?<!\S){re.escape(country_name)}(?:\s+\*)?(?!\S)"
        for match in re.finditer(pattern, text):
            start, end = match.span()
            if any(start < used_end and used_start < end for used_start, used_end in occupied):
                continue
            mentions.append((start, end, country_code, canonical_name))
            occupied.append((start, end))
    return sorted(mentions)


def participants_from_combined_text(text: str) -> list[tuple[str, str, str, str]]:
    mentions = country_mentions(text)
    if len(mentions) < 2:
        return []

    parsed: list[tuple[str, str, str, str]] = []
    previous_end = 0
    for start, end, country_code, country_name in mentions:
        participant_name = text[previous_end:start].strip(" ,;")
        if not participant_name:
            return []
        participant_name = re.sub(r" \(page does not exist\)$", "", participant_name)
        parsed.append((participant_name, country_name, country_code, text))
        previous_end = end

    trailing_text = text[previous_end:].strip(" ,;")
    if trailing_text:
        return []
    return parsed


def read_soup(url: str) -> BeautifulSoup:
    response = requests.get(url, headers=HEADERS, timeout=60)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def edition_links(index_url: str) -> list[tuple[int, str]]:
    soup = read_soup(index_url)
    table = soup.select_one("table.wikitable")
    if table is None:
        raise RuntimeError(f"Could not find editions table on {index_url}")

    links: dict[int, str] = {}
    for row in table.find_all("tr")[1:]:
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        year_text = clean_text(cells[1].get_text(" ", strip=True))
        match = re.search(r"\b(20\d{2})\b", year_text)
        if not match:
            continue
        year = int(match.group(1))
        if year <= 2000:
            continue
        anchor = cells[1].find("a", href=True)
        if not anchor:
            continue
        links[year] = urljoin(index_url, anchor["href"])
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
        title = clean_text(anchor.get("title") or "")
        value = text or title
        if value and value.lower() != "details":
            values.append(value)
    return values


def country_from_cell(cell) -> tuple[str, str]:
    text = clean_text(cell.get_text(" ", strip=True))
    code_match = re.search(r"\(\s*([A-Z]{3})\s*\)\s*$", text)
    if code_match:
        country_code = code_match.group(1)
        country_obj = pycountry.countries.get(alpha_3=country_code)
        return country_code, COUNTRY_NAME_BY_CODE.get(
            country_code, getattr(country_obj, "name", country_code) if country_obj else country_code
        )
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


def participant_from_cell(cell) -> tuple[str, str, str, str]:
    country_code, country_name = country_from_cell(cell)
    text = clean_text(cell.get_text(" ", strip=True))
    links = medalist_links(cell)
    country_names = {country_name, COUNTRY_NAME_BY_CODE.get(country_code, country_name)}
    athlete_links = []
    for value in links:
        if value in country_names:
            continue
        if country_from_name(value):
            continue
        athlete_links.append(re.sub(r" \(page does not exist\)$", "", value))
    if athlete_links:
        participant_name = athlete_links[0]
    else:
        participant_name = text
        for suffix in sorted(country_names, key=len, reverse=True):
            participant_name = re.sub(rf"\s+{re.escape(suffix)}(?:\s+\*)?$", "", participant_name).strip()
    return participant_name, country_name, country_code, text


def participants_from_cell(cell) -> list[tuple[str, str, str, str]]:
    text = clean_text(cell.get_text(" ", strip=True))
    text_participants = participants_from_combined_text(text)
    if len(text_participants) >= 2:
        return text_participants

    values = medalist_links(cell)
    country_positions: list[tuple[int, tuple[str, str]]] = []
    for index, value in enumerate(values):
        match = country_from_name(value)
        if match:
            country_positions.append((index, match))

    if len(country_positions) >= 2:
        parsed: list[tuple[str, str, str, str]] = []
        previous_country_index = -1
        for country_index, (country_code, country_name) in country_positions:
            athlete_values = [
                value
                for value in values[previous_country_index + 1 : country_index]
                if value and not country_from_name(value)
            ]
            if athlete_values:
                participant_name = re.sub(r" \(page does not exist\)$", "", athlete_values[-1])
                parsed.append((participant_name, country_name, country_code, text))
            previous_country_index = country_index
        if len(parsed) >= 2:
            return parsed

    return [participant_from_cell(cell)]


def event_key_from_name(event_name: str) -> str:
    normalized = clean_text(event_name)
    normalized = normalized.replace("−", "-").replace("+", "plus")
    normalized = re.sub(r"\s*details$", "", normalized, flags=re.IGNORECASE).strip()
    return slugify(normalized)


def is_medal_table(table) -> bool:
    first_row = table.find("tr")
    if not first_row:
        return False
    header = [clean_text(cell.get_text(" ", strip=True)) for cell in first_row.find_all(["th", "td"])]
    return header[:4] == ["Event", "Gold", "Silver", "Bronze"]


def parse_medal_table(
    *,
    year: int,
    url: str,
    competition_id: str,
    competition_name: str,
    gender: str,
    table,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    body_rows = table.find_all("tr")[1:]
    index = 0
    while index < len(body_rows):
        cells = body_rows[index].find_all(["td", "th"])
        if len(cells) < 4:
            index += 1
            continue
        event_label = re.sub(r"\s*details$", "", clean_text(cells[0].get_text(" ", strip=True)), flags=re.IGNORECASE)
        event_key = event_key_from_name(event_label)
        medal_cells = [cells[1], cells[2], cells[3]]
        if index + 1 < len(body_rows):
            bronze2_cells = body_rows[index + 1].find_all(["td", "th"])
            if len(bronze2_cells) == 1:
                medal_cells.append(bronze2_cells[0])

        for rank, cell in zip([1, 2, 3, 3], medal_cells):
            for participant_name, country_name, country_code, source_text in participants_from_cell(cell):
                rows.append(
                    {
                        "competition_id": competition_id,
                        "competition_name": competition_name,
                        "year": year,
                        "event_date": f"{year}-12-31",
                        "discipline_key": DISCIPLINE_ID,
                        "discipline_name": DISCIPLINE_NAME,
                        "event_key": event_key,
                        "event_name": f"{competition_name} {gender.title()} {event_label}",
                        "gender": gender,
                        "event_label": event_label,
                        "rank": rank,
                        "medal": RANK_TO_MEDAL[rank],
                        "participant_type": "athlete",
                        "participant_name": participant_name,
                        "country_name": country_name,
                        "country_code": country_code,
                        "source_text": source_text,
                        "source_url": url,
                    }
                )

        index += 2 if len(medal_cells) == 4 else 1

    return rows


def parse_iba_edition(source: dict[str, str], year: int, url: str) -> list[dict[str, object]]:
    soup = read_soup(url)
    rows: list[dict[str, object]] = []
    for table in soup.select("table.wikitable"):
        if not is_medal_table(table):
            continue
        heading = heading_for_table(table).lower()
        if heading not in {"medalists", "medal events", "medal summary", "medal winners"}:
            continue
        rows.extend(
            parse_medal_table(
                year=year,
                url=url,
                competition_id=source["competition_id"],
                competition_name=source["competition_name"],
                gender=source["gender"],
                table=table,
            )
        )
    return rows


def parse_world_boxing_2025() -> list[dict[str, object]]:
    soup = read_soup(WORLD_BOXING_2025_URL)
    rows: list[dict[str, object]] = []
    for table in soup.select("table.wikitable"):
        if not is_medal_table(table):
            continue
        heading = heading_for_table(table).lower()
        if heading not in {"men", "women"}:
            continue
        rows.extend(
            parse_medal_table(
                year=2025,
                url=WORLD_BOXING_2025_URL,
                competition_id="world_boxing_championships",
                competition_name="World Boxing Championships",
                gender=heading,
                table=table,
            )
        )
    return rows


def main() -> None:
    rows: list[dict[str, object]] = []
    for source in INDEX_SOURCES:
        for year, url in edition_links(source["index_url"]):
            rows.extend(parse_iba_edition(source, year, url))
    rows.extend(parse_world_boxing_2025())

    frame = pd.DataFrame(rows)
    if frame.empty:
        raise RuntimeError("No World Boxing Championships podium rows parsed.")
    if (frame["year"] <= 2000).any():
        offenders = (
            frame.loc[frame["year"] <= 2000, ["year", "competition_id", "gender", "event_key"]]
            .head(10)
            .to_dict("records")
        )
        raise RuntimeError(f"Post-2000 guard violated in seed builder: {offenders}")

    frame = frame.sort_values(
        ["competition_id", "year", "gender", "event_key", "rank", "country_code", "participant_name"]
    )
    profiles = (
        frame.groupby(["competition_id", "year", "gender", "event_key"])["rank"]
        .apply(lambda values: tuple(sorted(int(value) for value in values.tolist())))
        .to_dict()
    )
    allowed_profiles = {(1, 2, 3), (1, 2, 3, 3)}
    bad_profiles = {key: value for key, value in profiles.items() if value not in allowed_profiles}
    if bad_profiles:
        sample = dict(list(bad_profiles.items())[:30])
        raise RuntimeError(f"Unexpected seed rank profiles: {sample}")

    frame.to_csv(OUTPUT_FILE, index=False)
    years = sorted(int(year) for year in frame["year"].unique().tolist())
    events = frame[["competition_id", "year", "gender", "event_key"]].drop_duplicates().shape[0]
    print(f"Wrote {len(frame)} rows, {events} events for {years[0]}-{years[-1]} to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
