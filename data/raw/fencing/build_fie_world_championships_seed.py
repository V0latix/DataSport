from __future__ import annotations

import argparse
import re
import unicodedata
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag


INDEX_URL = "https://en.wikipedia.org/wiki/World_Fencing_Championships"
WIKI_BASE = "https://en.wikipedia.org"
SEED_PATH = Path(__file__).resolve().parent / "fie_world_championships_top3_seed.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder)"}

COMPETITION_ID = "fie_world_championships"
COMPETITION_NAME = "FIE World Championships"
START_YEAR = 2001
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}

COUNTRY_OVERRIDES = {
    "ain": "AIN",
    "china": "CHN",
    "chinese taipei": "TPE",
    "czech republic": "CZE",
    "great britain": "GBR",
    "hong kong": "HKG",
    "individual neutral athletes": "AIN",
    "iran": "IRN",
    "north korea": "PRK",
    "people's republic of china": "CHN",
    "russia": "RUS",
    "russian federation": "RUS",
    "south korea": "KOR",
    "turkey": "TUR",
    "turkiye": "TUR",
    "ukraine": "UKR",
    "united states": "USA",
    "united states of america": "USA",
}
IOC_TO_ISO3 = {
    "BUL": "BGR",
    "CRO": "HRV",
    "DEN": "DNK",
    "GER": "DEU",
    "GRE": "GRC",
    "LAT": "LVA",
    "MGL": "MNG",
    "NED": "NLD",
    "SLO": "SVN",
    "SUI": "CHE",
    "TPE": "TPE",
    "TUR": "TUR",
}
COUNTRY_NAME_OVERRIDES = {
    "AIN": "Individual Neutral Athletes",
    "TPE": "Chinese Taipei",
}


def clean_text(value: str) -> str:
    text = str(value or "")
    text = text.replace("\xa0", " ")
    text = text.replace("–", "-").replace("−", "-")
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_text(value: str) -> str:
    text = unicodedata.normalize("NFKD", clean_text(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.lower()


def canonical_country_name(code: str, fallback_name: str) -> str:
    code = str(code).upper().strip()
    if code in COUNTRY_NAME_OVERRIDES:
        return COUNTRY_NAME_OVERRIDES[code]
    try:
        import pycountry

        country = pycountry.countries.get(alpha_3=code)
        if country is not None:
            return str(getattr(country, "name"))
    except Exception:
        pass
    return clean_text(fallback_name) or code


def resolve_country(value: str) -> tuple[str, str] | None:
    text = clean_text(value)
    if not text:
        return None

    match = re.fullmatch(r"[A-Z]{3}", text)
    if match:
        code = IOC_TO_ISO3.get(text, text)
        return code, canonical_country_name(code, text)

    alias = COUNTRY_OVERRIDES.get(normalize_text(text))
    if alias:
        code = IOC_TO_ISO3.get(alias, alias)
        return code, canonical_country_name(code, text)

    try:
        import pycountry

        country = pycountry.countries.lookup(text)
        code = str(getattr(country, "alpha_3"))
        code = IOC_TO_ISO3.get(code, code)
        return code, canonical_country_name(code, text)
    except Exception:
        return None


def request_soup(url: str) -> BeautifulSoup:
    response = requests.get(url, headers=HEADERS, timeout=45)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def discover_edition_links(max_year: int) -> dict[int, str]:
    soup = request_soup(INDEX_URL)
    page_re = re.compile(r"/wiki/(?P<year>20\d{2})_World_Fencing_Championships$")
    links: dict[int, str] = {}
    for link in soup.find_all("a", href=True):
        href = str(link.get("href") or "")
        if "redlink=1" in href:
            continue
        match = page_re.fullmatch(href)
        if not match:
            continue
        year = int(match.group("year"))
        if START_YEAR <= year <= max_year:
            links.setdefault(year, urljoin(WIKI_BASE, href))

    if not links:
        raise RuntimeError(f"No FIE World Championships edition links found after {START_YEAR - 1}.")
    return dict(sorted(links.items()))


def is_medal_table(table: Tag) -> bool:
    first_row = table.find("tr")
    if first_row is None:
        return False
    headers = [normalize_text(cell.get_text(" ", strip=True)) for cell in first_row.find_all(["th", "td"])[:6]]
    line = " | ".join(headers)
    return "event" in line and "gold" in line and "silver" in line and "bronze" in line


def heading_gender(table: Tag) -> str | None:
    for heading in table.find_all_previous(["h2", "h3", "h4"], limit=8):
        text = normalize_text(heading.get_text(" ", strip=True))
        if "women" in text:
            return "women"
        if re.search(r"\bmen\b", text):
            return "men"
    return None


def parse_event_label(label: str, fallback_gender: str | None) -> tuple[str, str, str, str, str] | None:
    text = clean_text(label)
    text = re.sub(r"\s*details$", "", text, flags=re.IGNORECASE).strip()
    norm = normalize_text(text)

    gender = fallback_gender
    if "women" in norm:
        gender = "women"
    elif re.search(r"\bmen\b", norm):
        gender = "men"
    if gender not in {"men", "women"}:
        return None

    event_format = "team" if "team" in norm else "individual"
    if "epee" in norm or "épée" in text.lower():
        discipline_key = "epee"
        discipline_name = "Epee"
    elif "foil" in norm:
        discipline_key = "foil"
        discipline_name = "Foil"
    elif "sabre" in norm or "saber" in norm:
        discipline_key = "sabre"
        discipline_name = "Sabre"
    else:
        return None

    label_format = "Team" if event_format == "team" else "Individual"
    event_name = f"{gender.title()} {label_format} {discipline_name}"
    return discipline_key, discipline_name, gender, event_format, event_name


def country_links(cell: Tag) -> list[tuple[int, tuple[str, str], Tag]]:
    links: list[tuple[int, tuple[str, str], Tag]] = []
    for index, link in enumerate(cell.find_all("a")):
        resolved = None
        for candidate in (link.get("title", ""), link.get_text(" ", strip=True)):
            resolved = resolve_country(str(candidate))
            if resolved is not None:
                break
        if resolved is not None:
            links.append((index, resolved, link))
    return links


def parse_individual_cell(cell: Tag) -> list[tuple[str, str, str]]:
    links = cell.find_all("a")
    country_positions = country_links(cell)
    parsed: list[tuple[str, str, str]] = []

    if country_positions:
        country_indices = {index for index, _, _ in country_positions}
        country_before = country_positions[0][0] == 0
        for pos, (country_index, (country_code, country_name), _) in enumerate(country_positions):
            next_country_index = country_positions[pos + 1][0] if pos + 1 < len(country_positions) else len(links)
            previous_country_index = country_positions[pos - 1][0] if pos else -1

            if country_before:
                name_links = [
                    link
                    for index, link in enumerate(links[country_index + 1 : next_country_index], start=country_index + 1)
                    if index not in country_indices
                ]
            else:
                name_links = [
                    link
                    for index, link in enumerate(links[previous_country_index + 1 : country_index], start=previous_country_index + 1)
                    if index not in country_indices
                ]

            athlete_name = clean_text(" ".join(link.get_text(" ", strip=True) for link in name_links))
            if athlete_name:
                parsed.append((athlete_name, country_name, country_code))

    if parsed:
        return parsed

    raw_text = clean_text(cell.get_text(" ", strip=True))
    parts = raw_text.split()
    for size in range(1, min(6, len(parts))):
        suffix = " ".join(parts[-size:])
        resolved = resolve_country(suffix)
        if resolved is None:
            continue
        country_code, country_name = resolved
        athlete_name = clean_text(" ".join(parts[:-size]))
        if athlete_name:
            return [(athlete_name, country_name, country_code)]
    return []


def parse_team_cell(cell: Tag) -> tuple[str, str, str, str] | None:
    links = cell.find_all("a")
    resolved_links = country_links(cell)
    if resolved_links:
        country_index, (country_code, country_name), _ = resolved_links[0]
        roster_links = [
            link.get_text(" ", strip=True)
            for index, link in enumerate(links[country_index + 1 :], start=country_index + 1)
            if index not in {country_index for country_index, _, _ in resolved_links}
        ]
        roster = clean_text("; ".join(name for name in roster_links if name))
        return country_name, country_name, country_code, roster

    raw_text = clean_text(cell.get_text(" ", strip=True))
    parts = raw_text.split()
    for size in range(1, min(6, len(parts))):
        prefix = " ".join(parts[:size])
        resolved = resolve_country(prefix)
        if resolved is None:
            continue
        country_code, country_name = resolved
        roster = clean_text(" ".join(parts[size:]))
        return country_name, country_name, country_code, roster
    return None


def append_medal_rows(
    rows: list[dict[str, Any]],
    *,
    year: int,
    source_url: str,
    event_context: tuple[str, str, str, str, str],
    rank: int,
    cell: Tag,
) -> None:
    discipline_key, discipline_name, gender, event_format, event_name = event_context
    if event_format == "team":
        parsed_team = parse_team_cell(cell)
        if parsed_team is None:
            return
        participant_name, country_name, country_code, team_members = parsed_team
        rows.append(
            {
                "competition_id": COMPETITION_ID,
                "competition_name": COMPETITION_NAME,
                "year": year,
                "event_date": f"{year}-12-31",
                "discipline_key": discipline_key,
                "discipline_name": discipline_name,
                "event_name": event_name,
                "gender": gender,
                "event_format": event_format,
                "rank": rank,
                "medal": RANK_TO_MEDAL[rank],
                "participant_type": "team",
                "participant_name": participant_name,
                "country_name": country_name,
                "country_code": country_code,
                "team_members": team_members,
                "source_url": source_url,
            }
        )
        return

    for athlete_name, country_name, country_code in parse_individual_cell(cell):
        rows.append(
            {
                "competition_id": COMPETITION_ID,
                "competition_name": COMPETITION_NAME,
                "year": year,
                "event_date": f"{year}-12-31",
                "discipline_key": discipline_key,
                "discipline_name": discipline_name,
                "event_name": event_name,
                "gender": gender,
                "event_format": event_format,
                "rank": rank,
                "medal": RANK_TO_MEDAL[rank],
                "participant_type": "athlete",
                "participant_name": athlete_name,
                "country_name": country_name,
                "country_code": country_code,
                "team_members": "",
                "source_url": source_url,
            }
        )


def parse_edition(year: int, source_url: str) -> list[dict[str, Any]]:
    soup = request_soup(source_url)
    rows: list[dict[str, Any]] = []
    for table in soup.find_all("table", class_="wikitable"):
        if not is_medal_table(table):
            continue
        fallback_gender = heading_gender(table)
        current_context: tuple[str, str, str, str, str] | None = None

        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all(["th", "td"], recursive=False)
            if len(cells) >= 4:
                current_context = parse_event_label(cells[0].get_text(" ", strip=True), fallback_gender)
                if current_context is None:
                    continue
                append_medal_rows(rows, year=year, source_url=source_url, event_context=current_context, rank=1, cell=cells[1])
                append_medal_rows(rows, year=year, source_url=source_url, event_context=current_context, rank=2, cell=cells[2])
                append_medal_rows(rows, year=year, source_url=source_url, event_context=current_context, rank=3, cell=cells[3])
            elif len(cells) == 1 and current_context is not None and current_context[3] == "individual":
                append_medal_rows(rows, year=year, source_url=source_url, event_context=current_context, rank=3, cell=cells[0])

    return rows


def build_rows(max_year: int) -> tuple[list[dict[str, Any]], list[int]]:
    all_rows: list[dict[str, Any]] = []
    missing_years: list[int] = []
    for year, source_url in discover_edition_links(max_year).items():
        parsed = parse_edition(year, source_url)
        if not parsed:
            missing_years.append(year)
            continue
        all_rows.extend(parsed)
    return all_rows, missing_years


def main() -> None:
    parser = argparse.ArgumentParser(description="Build FIE World Championships podium seed (post-2000).")
    parser.add_argument("--max-year", type=int, default=2026)
    parser.add_argument("--out", type=Path, default=SEED_PATH)
    args = parser.parse_args()

    rows, missing_years = build_rows(max(START_YEAR, int(args.max_year)))
    if not rows:
        raise RuntimeError("No rows extracted for FIE World Championships seed.")

    frame = pd.DataFrame(rows)
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype(int)
    frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce").astype(int)
    frame = frame.loc[frame["year"] > 2000].copy()
    frame = frame.loc[frame["discipline_key"].isin(["epee", "foil", "sabre"])].copy()
    frame = frame.loc[frame["gender"].isin(["men", "women"])].copy()
    frame = frame.loc[frame["event_format"].isin(["individual", "team"])].copy()
    frame = frame.drop_duplicates(
        subset=[
            "year",
            "discipline_key",
            "gender",
            "event_format",
            "rank",
            "participant_type",
            "participant_name",
            "country_code",
        ],
        keep="first",
    )
    frame = frame.sort_values(
        ["year", "gender", "discipline_key", "event_format", "rank", "country_code", "participant_name"]
    ).reset_index(drop=True)

    profiles = (
        frame.groupby(["year", "discipline_key", "gender", "event_format"])["rank"]
        .apply(lambda s: tuple(sorted(int(v) for v in s.tolist())))
        .to_dict()
    )
    allowed_profiles = {(1, 2, 3), (1, 2, 3, 3)}
    invalid_profiles = {key: value for key, value in profiles.items() if value not in allowed_profiles}
    if invalid_profiles:
        sample = dict(list(invalid_profiles.items())[:30])
        raise RuntimeError(f"Unexpected FIE podium rank profile(s): {sample}")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(args.out, index=False)

    years = sorted(frame["year"].unique().tolist())
    event_count = frame[["year", "discipline_key", "gender", "event_format"]].drop_duplicates().shape[0]
    print(
        f"[seed] fie_world_championships rows={len(frame)} years={years[0]}-{years[-1]} "
        f"events={event_count} out={args.out}"
    )
    if missing_years:
        print(f"[seed] warning missing_years={sorted(set(missing_years))}")


if __name__ == "__main__":
    main()
