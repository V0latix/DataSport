from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.core.utils import slugify


COMPETITION_ID = "world_weightlifting_championships"
COMPETITION_NAME = "World Weightlifting Championships"
DISCIPLINE_ID = "weightlifting"
DISCIPLINE_NAME = "Weightlifting"
OUTPUT_FILE = Path(__file__).with_name("world_weightlifting_championships_top3_seed.csv")
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder; contact local)"}
SOURCES = {
    "men": "https://en.wikipedia.org/wiki/List_of_World_Championships_medalists_in_weightlifting_(men)",
    "women": "https://en.wikipedia.org/wiki/List_of_World_Championships_medalists_in_weightlifting_(women)",
}
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}
COUNTRY_NAME_OVERRIDES = {
    "AIN": "Individual Neutral Athletes",
    "BLR": "Belarus",
    "BRN": "Bahrain",
    "BUL": "Bulgaria",
    "CHI": "Chile",
    "CHN": "China",
    "EGY": "Egypt",
    "GBR": "Great Britain",
    "GER": "Germany",
    "GRE": "Greece",
    "INA": "Indonesia",
    "IRI": "Iran",
    "KOR": "South Korea",
    "KSA": "Saudi Arabia",
    "LAT": "Latvia",
    "MAD": "Madagascar",
    "MAS": "Malaysia",
    "MDA": "Moldova",
    "MGL": "Mongolia",
    "NGR": "Nigeria",
    "PHI": "Philippines",
    "PRK": "North Korea",
    "RUS": "Russia",
    "RWF": "Refugee Weightlifting Team",
    "TPE": "Chinese Taipei",
    "TUR": "Türkiye",
    "VIE": "Vietnam",
}


def clean_text(value: str) -> str:
    value = re.sub(r"\[[^\]]+\]", "", value)
    return re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip()


def nearest_heading(table) -> str:
    node = table
    while node:
        node = node.find_previous(["h2", "h3", "h4"])
        if not node:
            break
        text = clean_text(node.get_text(" ", strip=True).replace("[edit]", ""))
        if text and "Medal table" not in text:
            return text
    raise RuntimeError("Could not resolve weight-class heading for table.")


def parse_games(value: str) -> tuple[int, str]:
    text = clean_text(value)
    match = re.search(r"\b(18|19|20)\d{2}\b", text)
    if not match:
        raise RuntimeError(f"Could not parse championship year from games cell: {value!r}")
    return int(match.group(0)), text


def country_name_for_code(country_code: str) -> str:
    if country_code in COUNTRY_NAME_OVERRIDES:
        return COUNTRY_NAME_OVERRIDES[country_code]
    try:
        import pycountry

        country = pycountry.countries.get(alpha_3=country_code)
        if country:
            return country.name
    except Exception:
        pass
    return country_code


def parse_medalist(value: str) -> tuple[str, str]:
    text = clean_text(value)
    if not text:
        raise RuntimeError("Missing medalist cell.")
    match = re.match(r"^(?P<name>.+?)\s*\(\s*(?P<country>[A-Z]{3})\s*\)\s*$", text)
    if not match:
        raise RuntimeError(f"Could not parse medalist/country cell: {value!r}")
    return clean_text(match.group("name")), match.group("country").upper()


def parse_source(gender: str, url: str) -> list[dict[str, object]]:
    response = requests.get(url, headers=HEADERS, timeout=60)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    rows: list[dict[str, object]] = []

    for table in soup.select("table.wikitable"):
        header_cells = [clean_text(cell.get_text(" ", strip=True)) for cell in table.find_all("tr")[0].find_all(["th", "td"])]
        if header_cells[:4] != ["Games", "Gold", "Silver", "Bronze"]:
            continue

        weight_class = nearest_heading(table)
        event_key = slugify(weight_class)
        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all(["th", "td"])
            if len(cells) < 4:
                continue
            year, games_label = parse_games(cells[0].get_text(" ", strip=True))
            if year <= 2000:
                continue

            for rank, cell in enumerate(cells[1:4], start=1):
                participant_name, country_code = parse_medalist(cell.get_text(" ", strip=True))
                rows.append(
                    {
                        "competition_id": COMPETITION_ID,
                        "competition_name": COMPETITION_NAME,
                        "year": year,
                        "event_date": f"{year}-12-31",
                        "discipline_key": DISCIPLINE_ID,
                        "discipline_name": DISCIPLINE_NAME,
                        "event_key": event_key,
                        "event_name": f"{COMPETITION_NAME} {gender.title()} {weight_class} total",
                        "gender": gender,
                        "weight_class": weight_class,
                        "games_label": games_label,
                        "rank": rank,
                        "medal": RANK_TO_MEDAL[rank],
                        "participant_type": "athlete",
                        "participant_name": participant_name,
                        "country_name": country_name_for_code(country_code),
                        "country_code": country_code,
                        "source_url": url,
                    }
                )

    return rows


def main() -> None:
    rows: list[dict[str, object]] = []
    for gender, url in SOURCES.items():
        rows.extend(parse_source(gender, url))

    frame = pd.DataFrame(rows)
    if frame.empty:
        raise RuntimeError("No World Weightlifting Championships podium rows parsed.")

    if (frame["year"] <= 2000).any():
        offenders = frame.loc[frame["year"] <= 2000, ["year", "gender", "event_key"]].head(10).to_dict("records")
        raise RuntimeError(f"Post-2000 guard violated in seed builder: {offenders}")

    frame = frame.sort_values(["year", "gender", "event_key", "rank", "country_code", "participant_name"])
    profiles = (
        frame.groupby(["year", "gender", "event_key"])["rank"]
        .apply(lambda values: tuple(sorted(int(value) for value in values.tolist())))
        .to_dict()
    )
    bad_profiles = {key: value for key, value in profiles.items() if value != (1, 2, 3)}
    if bad_profiles:
        sample = dict(list(bad_profiles.items())[:30])
        raise RuntimeError(f"Unexpected seed rank profiles: {sample}")

    frame.to_csv(OUTPUT_FILE, index=False)
    years = sorted(int(year) for year in frame["year"].unique().tolist())
    print(
        f"Wrote {len(frame)} rows, {frame[['year', 'gender', 'event_key']].drop_duplicates().shape[0]} events "
        f"for {years[0]}-{years[-1]} to {OUTPUT_FILE}"
    )


if __name__ == "__main__":
    main()
