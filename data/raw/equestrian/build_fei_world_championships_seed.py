from __future__ import annotations

import argparse
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag


BASE_DIR = Path(__file__).resolve().parent
SEED_PATH = BASE_DIR / "fei_world_championships_top3_seed.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder)"}

COMPETITION_ID = "fei_world_championships"
COMPETITION_NAME = "FEI World Championships"
START_YEAR = 2001
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}


@dataclass(frozen=True)
class SourcePage:
    source_url: str
    edition_name: str


PAGES: dict[int, SourcePage] = {
    2002: SourcePage("https://en.wikipedia.org/wiki/2002_FEI_World_Equestrian_Games", "FEI World Equestrian Games"),
    2006: SourcePage("https://en.wikipedia.org/wiki/2006_FEI_World_Equestrian_Games", "FEI World Equestrian Games"),
    2010: SourcePage("https://en.wikipedia.org/wiki/2010_FEI_World_Equestrian_Games", "FEI World Equestrian Games"),
    2014: SourcePage("https://en.wikipedia.org/wiki/2014_FEI_World_Equestrian_Games", "FEI World Equestrian Games"),
    2018: SourcePage("https://en.wikipedia.org/wiki/2018_FEI_World_Equestrian_Games", "FEI World Equestrian Games"),
    2022: SourcePage("https://en.wikipedia.org/wiki/2022_FEI_World_Championships", "FEI World Championships"),
}

DISCIPLINES = {
    "equestrian-dressage-equestrian": "Equestrian Dressage (Equestrian)",
    "equestrian-driving": "Driving",
    "equestrian-endurance": "Endurance",
    "equestrian-eventing-equestrian": "Equestrian Eventing (Equestrian)",
    "equestrian-jumping-equestrian": "Equestrian Jumping (Equestrian)",
    "equestrian-para-dressage": "Para-dressage",
    "equestrian-reining": "Reining",
    "equestrian-vaulting": "Vaulting",
}

COUNTRY_CODE_NORMALIZATION = {
    "AUT": "AUT",
    "BEL": "BEL",
    "BRA": "BRA",
    "CAN": "CAN",
    "DEN": "DNK",
    "ESP": "ESP",
    "FRA": "FRA",
    "GBR": "GBR",
    "GER": "DEU",
    "IRL": "IRL",
    "ITA": "ITA",
    "NED": "NLD",
    "SUI": "CHE",
    "SWE": "SWE",
    "UAE": "ARE",
    "USA": "USA",
}
COUNTRY_NAME_ALIASES = {
    "Austria": "AUT",
    "Belgium": "BEL",
    "Brazil": "BRA",
    "Canada": "CAN",
    "Denmark": "DNK",
    "France": "FRA",
    "Germany": "DEU",
    "Great Britain": "GBR",
    "Ireland": "IRL",
    "Italy": "ITA",
    "Netherlands": "NLD",
    "Singapore": "SGP",
    "Spain": "ESP",
    "Sweden": "SWE",
    "Switzerland": "CHE",
    "United Arab Emirates": "ARE",
    "United Kingdom": "GBR",
    "United States": "USA",
}
COUNTRY_NAME_OVERRIDES = {
    "ARE": "United Arab Emirates",
    "GBR": "United Kingdom",
}


def clean_text(value: object) -> str:
    text = str(value or "")
    text = text.replace("\xa0", " ")
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = text.replace("*", "")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def slug_token(value: str) -> str:
    ascii_value = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii")
    text = clean_text(ascii_value).lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text or "unknown"


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


def heading_context(table: Tag) -> str:
    headings: list[str] = []
    node: Tag | None = table
    while node is not None:
        node = node.find_previous(["h2", "h3", "h4"])
        if node is None:
            break
        text = clean_text(node.get_text(" ", strip=True).replace("[edit]", ""))
        if text:
            headings.append(text)
        if len(headings) >= 3:
            break
    return " / ".join(reversed(headings))


def medal_tables(soup: BeautifulSoup) -> list[Tag]:
    tables: list[Tag] = []
    for table in soup.find_all("table", class_=lambda value: value and "wikitable" in value):
        first_row = table.find("tr")
        if first_row is None:
            continue
        headers = [clean_text(cell.get_text(" ", strip=True)) for cell in first_row.find_all(["th", "td"])]
        if {"Event", "Gold", "Silver", "Bronze"}.issubset(set(headers)):
            tables.append(table)
    return tables


def country_from_cell(cell: Tag) -> tuple[str, str]:
    flag_link = cell.select_one(".flagicon a[title]")
    if flag_link is not None:
        raw_country = clean_text(flag_link.get("title"))
        if raw_country in COUNTRY_ALIASES:
            code = normalize_country_code(COUNTRY_ALIASES[raw_country])
            return country_name(code), code

    text = clean_text(cell.get_text(" ", strip=True))
    code_match = re.search(r"\((?P<code>[A-Z]{3})\)", text)
    if code_match:
        code = normalize_country_code(code_match.group("code"))
        return country_name(code), code

    for link in cell.find_all("a"):
        label = clean_text(link.get("title") or link.get_text(" ", strip=True))
        if label in COUNTRY_ALIASES:
            code = normalize_country_code(COUNTRY_ALIASES[label])
            return country_name(code), code

    for candidate_country in COUNTRY_NAMES_BY_LENGTH:
        if text.startswith(f"{candidate_country} ") or text.endswith(f" {candidate_country}") or text == candidate_country:
            code = normalize_country_code(COUNTRY_ALIASES[candidate_country])
            return country_name(code), code

    raise RuntimeError(f"Could not parse FEI medal country from cell: {text!r}")


def event_text(cell: Tag) -> str:
    text = clean_text(cell.get_text(" ", strip=True))
    text = re.sub(r"\bdetails\b", "", text, flags=re.IGNORECASE)
    return clean_text(text)


def discipline_for(event_name: str, heading: str) -> tuple[str, str]:
    combined = f"{heading} {event_name}".lower()
    if "para-dressage" in combined or "grade i" in combined or "grade ii" in combined or "grade iii" in combined:
        return "equestrian-para-dressage", DISCIPLINES["equestrian-para-dressage"]
    if "dressage" in combined:
        return "equestrian-dressage-equestrian", DISCIPLINES["equestrian-dressage-equestrian"]
    if "driving" in combined:
        return "equestrian-driving", DISCIPLINES["equestrian-driving"]
    if "endurance" in combined:
        return "equestrian-endurance", DISCIPLINES["equestrian-endurance"]
    if "eventing" in combined:
        return "equestrian-eventing-equestrian", DISCIPLINES["equestrian-eventing-equestrian"]
    if "jumping" in combined:
        return "equestrian-jumping-equestrian", DISCIPLINES["equestrian-jumping-equestrian"]
    if "reining" in combined:
        return "equestrian-reining", DISCIPLINES["equestrian-reining"]
    if "vaulting" in combined or "pas-de-deux" in combined or "squad" in combined:
        return "equestrian-vaulting", DISCIPLINES["equestrian-vaulting"]
    raise RuntimeError(f"Could not map FEI discipline for event={event_name!r}, heading={heading!r}")


def gender_for(event_name: str) -> str:
    lowered = event_name.lower()
    if "women" in lowered or "female" in lowered:
        return "women"
    if "men" in lowered or "male" in lowered:
        return "men"
    return "mixed"


def event_key_for(event_name: str) -> str:
    lowered = clean_text(event_name).lower()
    lowered = re.sub(r"\bdetails\b", "", lowered)
    lowered = lowered.replace("women's", "women").replace("men's", "men")
    return slug_token(lowered)


def is_team_event(event_name: str) -> bool:
    lowered = event_name.lower()
    return any(token in lowered for token in ["team", "squad", "pas-de-deux"])


def athlete_name_from_cell(cell: Tag, country_code: str) -> str:
    country_labels = set(COUNTRY_ALIASES)
    country_labels.add(country_name(country_code))
    for link in cell.find_all("a"):
        text = clean_text(link.get_text(" ", strip=True))
        title = clean_text(link.get("title") or text)
        if not text or text.lower() == "details" or text.startswith("["):
            continue
        if text in country_labels or title in country_labels:
            continue
        if title in COUNTRY_ALIASES or text in COUNTRY_ALIASES:
            continue
        return text

    text = clean_text(cell.get_text(" ", strip=True))
    text = re.sub(r"\([A-Z]{3}\)", "", text)
    for candidate_country in COUNTRY_NAMES_BY_LENGTH:
        text = re.sub(rf"\b{re.escape(candidate_country)}\b", "", text)
    text = re.sub(r"\b\d+(?:\.\d+)?\s*(?:%|pens|pts|points)?\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+on\s*$", "", text)
    return clean_text(text) or f"Unknown {country_code}"


def linked_entry_name_from_cell(cell: Tag, country_code: str, max_names: int = 2) -> str:
    country_labels = set(COUNTRY_ALIASES)
    country_labels.add(country_name(country_code))
    names: list[str] = []
    for link in cell.find_all("a"):
        text = clean_text(link.get_text(" ", strip=True))
        title = clean_text(link.get("title") or text)
        if not text or text.lower() == "details" or text.startswith("["):
            continue
        if text in country_labels or title in country_labels:
            continue
        if title in COUNTRY_ALIASES or text in COUNTRY_ALIASES:
            continue
        if text not in names:
            names.append(text)
        if len(names) >= max_names:
            break
    return " / ".join(names)


def participant_from_cell(cell: Tag, event_name: str) -> tuple[str, str, str]:
    resolved_country_name, country_code = country_from_cell(cell)
    if "pas-de-deux" in event_name.lower():
        pair_name = linked_entry_name_from_cell(cell, country_code, max_names=2)
        return "team", pair_name or resolved_country_name, country_code
    if is_team_event(event_name):
        return "team", resolved_country_name, country_code
    return "athlete", athlete_name_from_cell(cell, country_code), country_code


def extract_rows_for_page(year: int, source: SourcePage) -> list[dict[str, Any]]:
    soup = fetch_soup(source.source_url)
    rows: list[dict[str, Any]] = []
    for table in medal_tables(soup):
        heading = heading_context(table)
        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all(["th", "td"], recursive=False)
            if len(cells) < 4:
                continue
            raw_event_name = event_text(cells[0])
            if not raw_event_name:
                continue
            if any("competition abandoned" in clean_text(cell.get_text(" ", strip=True)).lower() for cell in cells[1:4]):
                continue
            discipline_id, discipline_name = discipline_for(raw_event_name, heading)
            event_key = event_key_for(raw_event_name)
            gender = gender_for(raw_event_name)
            for rank, cell in ((1, cells[1]), (2, cells[2]), (3, cells[3])):
                participant_type, participant_name, country_code = participant_from_cell(cell, raw_event_name)
                rows.append(
                    {
                        "competition_id": COMPETITION_ID,
                        "competition_name": COMPETITION_NAME,
                        "edition_name": source.edition_name,
                        "year": year,
                        "event_date": f"{year}-12-31",
                        "discipline_key": discipline_id,
                        "discipline_name": discipline_name,
                        "event_key": event_key,
                        "event_name": raw_event_name,
                        "gender": gender,
                        "rank": rank,
                        "medal": RANK_TO_MEDAL[rank],
                        "participant_type": participant_type,
                        "participant_name": participant_name,
                        "country_name": country_name(country_code),
                        "country_code": country_code,
                        "source_url": source.source_url,
                    }
                )
    return rows


def build_rows(max_year: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for year, source in PAGES.items():
        if year < START_YEAR or year > int(max_year):
            continue
        rows.extend(extract_rows_for_page(year, source))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Build FEI World Championships podium seed.")
    parser.add_argument("--max-year", type=int, default=2026)
    parser.add_argument("--out", type=Path, default=SEED_PATH)
    args = parser.parse_args()

    rows = build_rows(max(START_YEAR, int(args.max_year)))
    if not rows:
        raise RuntimeError("No FEI World Championships rows extracted.")

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
        sample = dict(list(invalid_profiles.items())[:20])
        raise RuntimeError(f"Unexpected FEI rank profile(s): {sample}")

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
        raise RuntimeError(f"Duplicate FEI entry in same event: {sample}")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(args.out, index=False)

    years = sorted(int(year) for year in frame["year"].unique().tolist())
    event_count = frame[["year", "discipline_key", "event_key", "gender"]].drop_duplicates().shape[0]
    print(
        f"[seed] fei_world_championships rows={len(frame)} "
        f"years={years[0]}-{years[-1]} events={event_count} out={args.out}"
    )


if __name__ == "__main__":
    main()
