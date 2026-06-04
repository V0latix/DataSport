from __future__ import annotations

import argparse
import re
import unicodedata
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup


INDEX_URL = "https://web.worldtaekwondo.martial.services/competitions?type=wc"
WT_BASE_URL = "https://web.worldtaekwondo.martial.services"
SEED_PATH = Path(__file__).resolve().parent / "world_taekwondo_championships_top4_seed.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder)"}

COMPETITION_ID = "world_taekwondo_championships"
COMPETITION_NAME = "World Taekwondo Championships"
START_YEAR = 2001
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}
MEDAL_TO_RANK = {"gold": 1, "silver": 2, "bronze": 3}


def clean_text(value: str) -> str:
    text = str(value or "")
    text = text.replace("\xa0", " ")
    text = text.replace("−", "-").replace("–", "-")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_text(value: str) -> str:
    text = unicodedata.normalize("NFKD", clean_text(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.lower()


def request_soup(url: str) -> BeautifulSoup:
    response = requests.get(url, headers=HEADERS, timeout=45)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def discover_edition_links(max_year: int) -> dict[int, str]:
    soup = request_soup(INDEX_URL)
    links: dict[int, str] = {}
    pattern = re.compile(r"/competitions/(?P<slug>[^\"/]*(?:wtf-|world-)taekwondo-championships)/medalists$")
    for link in soup.find_all("a", href=True):
        href = str(link.get("href") or "")
        match = pattern.search(href)
        if not match:
            continue
        year_match = re.search(r"(19|20)\d{2}", href)
        if not year_match:
            continue
        year = int(year_match.group(0))
        if START_YEAR <= year <= int(max_year):
            links.setdefault(year, urljoin(WT_BASE_URL, href))

    if not links:
        raise RuntimeError(f"No World Taekwondo Championships medalist links found after {START_YEAR - 1}.")
    return dict(sorted(links.items()))


def parse_weight_category(category: str) -> tuple[str, str, str] | None:
    text = clean_text(category)
    norm = normalize_text(text)
    if norm.startswith("men "):
        gender = "men"
        weight = text[4:].strip()
    elif norm.startswith("women "):
        gender = "women"
        weight = text[6:].strip()
    else:
        return None

    match = re.fullmatch(r"([+-])\s*(\d+)\s*kg", weight, flags=re.IGNORECASE)
    if match:
        sign, limit = match.groups()
        direction = "over" if sign == "+" else "under"
        weight_key = f"{direction}-{limit}kg"
        weight_name = f"{'+' if sign == '+' else '-'}{limit} kg"
        event_name = f"{gender.title()} {weight_name}"
        return gender, weight_key, event_name

    fallback = re.sub(r"[^a-z0-9]+", "-", normalize_text(weight)).strip("-")
    return gender, fallback, f"{gender.title()} {weight}"


def parse_medal(value: str) -> str | None:
    norm = normalize_text(value)
    for medal in ("gold", "silver", "bronze"):
        if medal in norm:
            return medal
    return None


def parse_edition(year: int, source_url: str) -> list[dict[str, Any]]:
    soup = request_soup(source_url)
    table = soup.find("table", class_="medallists")
    if table is None:
        return []

    rows: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all("td", recursive=False)
        if len(cells) < 4:
            continue

        athlete_link = cells[1].find("a")
        athlete_name = clean_text(athlete_link.get_text(" ", strip=True) if athlete_link else cells[1].get_text(" ", strip=True))
        flag = cells[1].select_one("div.flag span")
        country_code = clean_text(flag.get_text(" ", strip=True) if flag else "").upper()
        category = clean_text(cells[2].get_text(" ", strip=True))
        parsed_category = parse_weight_category(category)
        medal = parse_medal(cells[3].get_text(" ", strip=True))

        if not athlete_name or not country_code or parsed_category is None or medal is None:
            continue

        gender, weight_key, event_name = parsed_category
        rank = MEDAL_TO_RANK[medal]
        rows.append(
            {
                "competition_id": COMPETITION_ID,
                "competition_name": COMPETITION_NAME,
                "year": year,
                "event_date": f"{year}-12-31",
                "discipline_key": "taekwondo",
                "discipline_name": "Taekwondo",
                "weight_class": weight_key,
                "event_name": event_name,
                "gender": gender,
                "rank": rank,
                "medal": RANK_TO_MEDAL[rank],
                "participant_type": "athlete",
                "participant_name": athlete_name,
                "country_name": country_code,
                "country_code": country_code,
                "source_url": source_url,
            }
        )
    return rows


def build_rows(max_year: int) -> tuple[list[dict[str, Any]], list[int]]:
    rows: list[dict[str, Any]] = []
    missing_years: list[int] = []
    for year, source_url in discover_edition_links(max_year).items():
        parsed = parse_edition(year, source_url)
        if not parsed:
            missing_years.append(year)
            continue
        rows.extend(parsed)
    return rows, missing_years


def main() -> None:
    parser = argparse.ArgumentParser(description="Build World Taekwondo Championships podium seed (post-2000).")
    parser.add_argument("--max-year", type=int, default=2026)
    parser.add_argument("--out", type=Path, default=SEED_PATH)
    args = parser.parse_args()

    rows, missing_years = build_rows(max(START_YEAR, int(args.max_year)))
    if not rows:
        raise RuntimeError("No rows extracted for World Taekwondo Championships seed.")

    frame = pd.DataFrame(rows)
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype(int)
    frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce").astype(int)
    frame = frame.loc[frame["year"] > 2000].copy()
    frame = frame.loc[
        (frame["competition_id"] == COMPETITION_ID)
        & (frame["discipline_key"] == "taekwondo")
        & frame["gender"].isin(["men", "women"])
        & frame["rank"].isin([1, 2, 3])
    ].copy()
    frame = frame.drop_duplicates(
        subset=[
            "year",
            "gender",
            "weight_class",
            "rank",
            "participant_name",
            "country_code",
        ],
        keep="first",
    )
    frame = frame.sort_values(["year", "gender", "weight_class", "rank", "country_code", "participant_name"]).reset_index(
        drop=True
    )

    profiles = (
        frame.groupby(["year", "gender", "weight_class"])["rank"]
        .apply(lambda values: tuple(sorted(int(value) for value in values.tolist())))
        .to_dict()
    )
    allowed_profiles = {(1, 2, 3), (1, 2, 3, 3)}
    invalid_profiles = {key: value for key, value in profiles.items() if value not in allowed_profiles}
    if invalid_profiles:
        invalid_index = pd.MultiIndex.from_tuples(invalid_profiles.keys(), names=["year", "gender", "weight_class"])
        frame = frame.set_index(["year", "gender", "weight_class"])
        frame = frame.loc[~frame.index.isin(invalid_index)].reset_index()
        sample = dict(list(invalid_profiles.items())[:30])
        print(f"[seed] warning excluded_non_standard_profiles={sample}")

    duplicated_participants = frame.loc[
        frame.duplicated(
            subset=["year", "gender", "weight_class", "participant_name", "country_code"],
            keep=False,
        )
    ]
    if not duplicated_participants.empty:
        duplicate_keys = (
            duplicated_participants[["year", "gender", "weight_class"]]
            .drop_duplicates()
            .sort_values(["year", "gender", "weight_class"])
        )
        duplicate_index = pd.MultiIndex.from_frame(duplicate_keys)
        frame = frame.set_index(["year", "gender", "weight_class"])
        frame = frame.loc[~frame.index.isin(duplicate_index)].reset_index()
        sample = duplicate_keys.head(30).to_dict("records")
        print(f"[seed] warning excluded_duplicate_participant_events={sample}")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(args.out, index=False)

    years = sorted(int(year) for year in frame["year"].unique().tolist())
    event_count = frame[["year", "gender", "weight_class"]].drop_duplicates().shape[0]
    print(
        f"[seed] world_taekwondo_championships rows={len(frame)} years={years[0]}-{years[-1]} "
        f"events={event_count} out={args.out}"
    )
    if missing_years:
        print(f"[seed] warning missing_years={sorted(set(missing_years))}")


if __name__ == "__main__":
    main()
