from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup


SOURCE_URL = "https://en.wikipedia.org/wiki/World_Curling_Championships"
OUT_FILE = Path(__file__).resolve().with_name("world_curling_championships_top3_seed.csv")
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}
COUNTRY_CODE_OVERRIDES = {
    "Australia": "AUS",
    "Austria": "AUT",
    "Canada": "CAN",
    "China": "CHN",
    "Czech Republic": "CZE",
    "Denmark": "DEN",
    "England": "ENG",
    "Estonia": "EST",
    "Finland": "FIN",
    "France": "FRA",
    "Germany": "GER",
    "Hungary": "HUN",
    "Italy": "ITA",
    "Japan": "JPN",
    "Latvia": "LAT",
    "New Zealand": "NZL",
    "Norway": "NOR",
    "Russia": "RUS",
    "Scotland": "SCO",
    "South Korea": "KOR",
    "Spain": "ESP",
    "Sweden": "SWE",
    "Switzerland": "SUI",
    "Turkey": "TUR",
    "United States": "USA",
}


@dataclass(frozen=True)
class CompetitionSpec:
    table_index: int
    competition_id: str
    competition_name: str
    event_key: str
    event_name: str
    gender: str


SPECS = [
    CompetitionSpec(
        table_index=0,
        competition_id="world_mens_curling_championship",
        competition_name="World Men's Curling Championship",
        event_key="men",
        event_name="Men",
        gender="men",
    ),
    CompetitionSpec(
        table_index=1,
        competition_id="world_womens_curling_championship",
        competition_name="World Women's Curling Championship",
        event_key="women",
        event_name="Women",
        gender="women",
    ),
    CompetitionSpec(
        table_index=2,
        competition_id="world_mixed_curling_championship",
        competition_name="World Mixed Curling Championship",
        event_key="mixed",
        event_name="Mixed",
        gender="mixed",
    ),
    CompetitionSpec(
        table_index=3,
        competition_id="world_mixed_doubles_curling_championship",
        competition_name="World Mixed Doubles Curling Championship",
        event_key="mixed-doubles",
        event_name="Mixed Doubles",
        gender="mixed",
    ),
]


def clean_text(value: str) -> str:
    value = re.sub(r"\[[^\]]+\]", "", str(value))
    value = re.sub(r"\([^)]*\)", "", value)
    return re.sub(r"\s+", " ", value).strip()


def country_name_from_cell(cell) -> str:
    for link in cell.find_all("a"):
        title = link.get("title") or link.get_text(" ", strip=True)
        title = clean_text(title)
        if title in COUNTRY_CODE_OVERRIDES:
            return title
    return clean_text(cell.get_text(" ", strip=True))


def country_code_for_name(country_name: str) -> str:
    if country_name in COUNTRY_CODE_OVERRIDES:
        return COUNTRY_CODE_OVERRIDES[country_name]
    try:
        import pycountry

        return pycountry.countries.lookup(country_name).alpha_3
    except Exception as exc:
        raise RuntimeError(f"Missing country code mapping for {country_name!r}") from exc


def build_seed() -> pd.DataFrame:
    response = requests.get(SOURCE_URL, headers={"User-Agent": "DataSport seed builder"}, timeout=60)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    tables = soup.select("table.wikitable")

    rows: list[dict[str, object]] = []
    for spec in SPECS:
        table = tables[spec.table_index]
        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all(["th", "td"])
            if len(cells) < 5:
                continue
            year_text = clean_text(cells[0].get_text(" ", strip=True))
            if not re.fullmatch(r"\d{4}", year_text):
                continue
            year = int(year_text)
            if year <= 2000:
                continue

            medal_cells = cells[2:5]
            medal_names = [country_name_from_cell(cell) for cell in medal_cells]
            if any(name in {"", "Cancelled", "Future event", "Not Held", "TBA"} for name in medal_names):
                continue

            for rank, country_name in enumerate(medal_names, start=1):
                country_code = country_code_for_name(country_name)
                rows.append(
                    {
                        "competition_id": spec.competition_id,
                        "competition_name": spec.competition_name,
                        "year": year,
                        "event_date": f"{year}-12-31",
                        "discipline_key": "curling",
                        "discipline_name": "Curling",
                        "event_key": spec.event_key,
                        "event_name": spec.event_name,
                        "gender": spec.gender,
                        "rank": rank,
                        "medal": RANK_TO_MEDAL[rank],
                        "participant_type": "team",
                        "participant_name": country_name,
                        "country_name": country_name,
                        "country_code": country_code,
                        "source_url": SOURCE_URL,
                    }
                )

    frame = pd.DataFrame(rows)
    if frame.empty:
        raise RuntimeError("No World Curling Championships rows extracted.")
    frame = frame.sort_values(["competition_id", "year", "rank", "country_code"]).reset_index(drop=True)
    return frame


def main() -> None:
    frame = build_seed()
    profiles = (
        frame.groupby(["competition_id", "year"])["rank"]
        .apply(lambda values: tuple(sorted(int(value) for value in values.tolist())))
        .to_dict()
    )
    bad_profiles = {key: value for key, value in profiles.items() if value != (1, 2, 3)}
    if bad_profiles:
        sample = dict(list(bad_profiles.items())[:20])
        raise RuntimeError(f"Unexpected rank profiles: {sample}")
    frame.to_csv(OUT_FILE, index=False)
    print(f"Wrote {len(frame)} rows to {OUT_FILE}")
    print(frame.groupby("competition_id")["year"].agg(["min", "max", "nunique"]).to_string())


if __name__ == "__main__":
    main()
