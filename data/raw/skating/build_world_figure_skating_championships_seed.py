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
SEED_PATH = BASE_DIR / "world_figure_skating_championships_top3_seed.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder)"}

COMPETITION_ID = "world_figure_skating_championships"
COMPETITION_NAME = "World Figure Skating Championships"
DISCIPLINE_KEY = "figure-skating"
DISCIPLINE_NAME = "Figure skating"
START_YEAR = 2001
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}
ALLOWED_PROFILES = {(1, 2, 3)}
EVENT_ALIASES = {
    "men": ("men", "men-singles", "Men's singles", "athlete"),
    "men's singles": ("men", "men-singles", "Men's singles", "athlete"),
    "women": ("women", "women-singles", "Women's singles", "athlete"),
    "ladies": ("women", "women-singles", "Women's singles", "athlete"),
    "women's singles": ("women", "women-singles", "Women's singles", "athlete"),
    "pairs": ("mixed", "pairs", "Pairs", "team"),
    "pair skating": ("mixed", "pairs", "Pairs", "team"),
    "ice dance": ("mixed", "ice-dance", "Ice dance", "team"),
    "ice dancing": ("mixed", "ice-dance", "Ice dance", "team"),
}
COUNTRY_ALIASES = {
    "Chinese Taipei": ("Chinese Taipei", "TPE"),
    "Czech Republic": ("Czech Republic", "CZE"),
    "Figure Skating Federation of Russia": ("Russia", "RUS"),
    "FSR": ("Russia", "RUS"),
    "Georgia (country)": ("Georgia", "GEO"),
    "Russia": ("Russia", "RUS"),
    "South Korea": ("South Korea", "KOR"),
    "United States": ("United States", "USA"),
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


def source_url(year: int) -> str:
    return f"https://en.wikipedia.org/wiki/{year}_World_Figure_Skating_Championships"


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


def country_from_cell(cell: Tag) -> Country | None:
    for link in cell.find_all("a"):
        country = is_country_link(link)
        if country is not None:
            return country
    text = clean_text(cell.get_text(" ", strip=True))
    text = re.sub(r"\s*\(([A-Z]{3}|[A-Z]{2,4})\)\s*$", "", text).strip()
    return country_from_label(text)


def fetch_soup(year: int) -> BeautifulSoup:
    response = requests.get(source_url(year), headers=HEADERS, timeout=60)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def event_spec(label: str) -> tuple[str, str, str, str] | None:
    key = clean_text(label).lower()
    return EVENT_ALIASES.get(key)


def participant_from_medal_cell(cell: Tag, participant_type: str) -> tuple[Country, str] | None:
    country: Country | None = None
    names: list[str] = []
    for link in cell.find_all("a"):
        link_country = is_country_link(link)
        if link_country is not None and country is None:
            country = link_country
            continue
        name = clean_text(link.get_text(" ", strip=True))
        if name:
            names.append(name)

    if country is None:
        return None
    if participant_type == "team":
        if not names:
            return None
        return country, " / ".join(names)
    if names:
        return country, names[0]

    text = clean_text(cell.get_text(" ", strip=True))
    return (country, text) if text else None


def parse_medalist_table(soup: BeautifulSoup, year: int) -> list[dict[str, Any]]:
    for table in soup.find_all("table"):
        first_row = table.find("tr")
        if first_row is None:
            continue
        headers = [clean_text(cell.get_text(" ", strip=True)) for cell in first_row.find_all(["th", "td"], recursive=False)]
        if headers[:4] != ["Discipline", "Gold", "Silver", "Bronze"]:
            continue

        rows: list[dict[str, Any]] = []
        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all(["th", "td"], recursive=False)
            if len(cells) < 4:
                continue
            spec = event_spec(cells[0].get_text(" ", strip=True))
            if spec is None:
                continue
            gender, event_key, event_name, participant_type = spec
            for rank, medal_cell in [(1, cells[1]), (2, cells[2]), (3, cells[3])]:
                parsed = participant_from_medal_cell(medal_cell, participant_type)
                if parsed is None:
                    continue
                country, participant_name = parsed
                rows.append(seed_row(year, gender, event_key, event_name, rank, participant_type, participant_name, country))
        if rows:
            return rows
    return []


def next_table_after_heading(heading: Tag) -> Tag | None:
    node = heading.parent if isinstance(heading.parent, Tag) and "mw-heading" in (heading.parent.get("class") or []) else heading
    while True:
        node = node.find_next_sibling()
        if node is None or getattr(node, "name", None) in {"h2", "h3"}:
            return None
        if getattr(node, "name", None) == "table":
            return node
        table = node.find("table") if isinstance(node, Tag) else None
        if table is not None:
            return table


def column_index(headers: list[str], names: set[str]) -> int | None:
    for index, header in enumerate(headers):
        if clean_text(header).lower() in names:
            return index
    return None


def parse_result_table(table: Tag, year: int, spec: tuple[str, str, str, str]) -> list[dict[str, Any]]:
    gender, event_key, event_name, participant_type = spec
    first_row = table.find("tr")
    if first_row is None:
        return []
    headers = [clean_text(cell.get_text(" ", strip=True)) for cell in first_row.find_all(["th", "td"], recursive=False)]
    rank_idx = column_index(headers, {"rank", "pl.", "place"})
    name_idx = column_index(headers, {"name", "skater"})
    nation_idx = column_index(headers, {"nation", "country"})
    if rank_idx is None or name_idx is None or nation_idx is None:
        return []

    rows: list[dict[str, Any]] = []
    for tr in table.find_all("tr")[1:]:
        cells = tr.find_all(["th", "td"], recursive=False)
        if len(cells) <= max(rank_idx, name_idx, nation_idx):
            continue
        rank_text = clean_text(cells[rank_idx].get_text(" ", strip=True))
        if not rank_text.isdigit():
            continue
        rank = int(rank_text)
        if rank > 3:
            break
        country = country_from_cell(cells[nation_idx])
        participant_name = clean_text(cells[name_idx].get_text(" ", strip=True))
        if country is None or not participant_name:
            continue
        rows.append(seed_row(year, gender, event_key, event_name, rank, participant_type, participant_name, country))
    return rows


def parse_legacy_result_tables(soup: BeautifulSoup, year: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for heading in soup.find_all("h3"):
        heading_text = clean_text(heading.get_text(" ", strip=True))
        spec = event_spec(heading_text)
        if spec is None:
            continue
        table = next_table_after_heading(heading)
        if table is None:
            continue
        rows.extend(parse_result_table(table, year, spec))
    return rows


def seed_row(
    year: int,
    gender: str,
    event_key: str,
    event_name: str,
    rank: int,
    participant_type: str,
    participant_name: str,
    country: Country,
) -> dict[str, Any]:
    return {
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
        "participant_name": clean_text(participant_name),
        "country_name": country.name,
        "country_code": country.code,
        "source_url": source_url(year),
    }


def rows_for_year(year: int) -> list[dict[str, Any]]:
    soup = fetch_soup(year)
    rows = parse_medalist_table(soup, year)
    if rows:
        return rows
    return parse_legacy_result_tables(soup, year)


def build_seed(start_year: int, max_year: int, output: Path) -> pd.DataFrame:
    all_rows: list[dict[str, Any]] = []
    empty_years: list[int] = []
    for year in range(int(start_year), int(max_year) + 1):
        rows = rows_for_year(year)
        if not rows:
            empty_years.append(year)
            continue
        all_rows.extend(rows)

    frame = pd.DataFrame(all_rows)
    if frame.empty:
        raise RuntimeError("World Figure Skating seed extraction produced no rows.")

    frame["year"] = pd.to_numeric(frame["year"], errors="coerce")
    frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce")
    frame = frame.dropna(subset=["year", "rank"]).copy()
    frame["year"] = frame["year"].astype(int)
    frame["rank"] = frame["rank"].astype(int)
    frame = frame.loc[frame["year"] > 2000].copy()
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
        raise RuntimeError(f"Post-2000 guard violated for World Figure Skating seed: {offenders}")

    profiles = frame.groupby(["year", "gender", "event_key"])["rank"].apply(
        lambda values: tuple(sorted(int(value) for value in values.tolist()))
    )
    bad_profiles = {key: value for key, value in profiles.items() if value not in ALLOWED_PROFILES}
    if bad_profiles:
        sample = dict(list(bad_profiles.items())[:30])
        raise RuntimeError(f"Unexpected World Figure Skating rank profiles in seed: {sample}")

    output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output, index=False)
    if empty_years:
        print(f"[world-figure-skating-seed] skipped empty/unavailable years: {empty_years}")
    return frame


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build World Figure Skating Championships podium seed.")
    parser.add_argument("--start-year", type=int, default=START_YEAR)
    parser.add_argument("--max-year", type=int, default=datetime.now(timezone.utc).year)
    parser.add_argument("--output", type=Path, default=SEED_PATH)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    seed = build_seed(start_year=int(args.start_year), max_year=int(args.max_year), output=Path(args.output))
    print(f"[world-figure-skating-seed] wrote {args.output} rows={len(seed)} years={seed.year.min()}-{seed.year.max()}")
    print(f"[world-figure-skating-seed] events={seed.groupby(['year', 'gender', 'event_key']).ngroups}")
    print(f"[world-figure-skating-seed] rows by year:\n{seed.groupby('year').size()}")


if __name__ == "__main__":
    main()
