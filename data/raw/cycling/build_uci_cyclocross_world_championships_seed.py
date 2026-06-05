from __future__ import annotations

import argparse
import io
import re
from pathlib import Path
from typing import Any

import pandas as pd
import requests


BASE_DIR = Path(__file__).resolve().parent
SEED_PATH = BASE_DIR / "uci_cyclocross_world_championships_top3_seed.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder)"}

PODIUM_PAGES = {
    "men": "https://en.wikipedia.org/wiki/UCI_Cyclo-cross_World_Championships_%E2%80%93_Men%27s_elite_race",
    "women": "https://en.wikipedia.org/wiki/UCI_Cyclo-cross_World_Championships_%E2%80%93_Women%27s_elite_race",
}
COMPETITION_ID = "uci_cyclocross_world_championships"
COMPETITION_NAME = "UCI Cyclo-cross World Championships"
DISCIPLINE_ID = "cycling-cyclo-cross"
DISCIPLINE_NAME = "Cyclo-cross"
START_YEAR = 2001
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}
COUNTRY_CODE_NORMALIZATION = {
    "CRO": "HRV",
    "DEN": "DNK",
    "GRE": "GRC",
    "LAT": "LVA",
    "NED": "NLD",
    "SLO": "SVN",
    "SUI": "CHE",
}
COUNTRY_NAME_OVERRIDES = {
    "GER": "Germany",
}


def clean_text(value: object) -> str:
    text = str(value or "")
    text = text.replace("\xa0", " ")
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_country_code(code: str) -> str:
    value = clean_text(code).upper()
    return COUNTRY_CODE_NORMALIZATION.get(value, value)


def country_name(country_code: str) -> str:
    override = COUNTRY_NAME_OVERRIDES.get(country_code)
    if override:
        return override
    try:
        import pycountry

        country = pycountry.countries.get(alpha_3=country_code)
        if country is not None:
            return str(getattr(country, "name"))
    except Exception:
        pass
    return country_code


def parse_rider(value: object) -> tuple[str, str, str]:
    text = clean_text(value)
    match = re.match(r"^(?P<name>.+?)\s+\((?P<code>[A-Z]{3})\)$", text)
    if not match:
        raise RuntimeError(f"Could not parse rider/country value: {text!r}")
    rider_name = clean_text(match.group("name"))
    code = normalize_country_code(match.group("code"))
    if not rider_name or not re.fullmatch(r"[A-Z]{3}", code):
        raise RuntimeError(f"Invalid rider/country value: {text!r}")
    return rider_name, country_name(code), code


def fetch_podium_table(source_url: str) -> pd.DataFrame:
    response = requests.get(source_url, headers=HEADERS, timeout=45)
    response.raise_for_status()
    tables = pd.read_html(io.StringIO(response.text))
    for table in tables:
        if {"Year", "Gold", "Silver", "Bronze"}.issubset(set(table.columns)):
            return table[["Year", "Gold", "Silver", "Bronze"]].copy()
    raise RuntimeError(f"No podium table found at {source_url}")


def build_rows(max_year: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for gender, source_url in PODIUM_PAGES.items():
        table = fetch_podium_table(source_url)
        table["Year"] = pd.to_numeric(table["Year"], errors="coerce")
        table = table.dropna(subset=["Year"]).copy()
        table["Year"] = table["Year"].astype(int)
        table = table.loc[(table["Year"] >= START_YEAR) & (table["Year"] <= int(max_year))].copy()

        for row in table.itertuples(index=False):
            year = int(getattr(row, "Year"))
            for rank, medal_column in ((1, "Gold"), (2, "Silver"), (3, "Bronze")):
                rider_name, resolved_country_name, country_code = parse_rider(getattr(row, medal_column))
                rows.append(
                    {
                        "competition_id": COMPETITION_ID,
                        "competition_name": COMPETITION_NAME,
                        "year": year,
                        "event_date": f"{year}-02-01",
                        "discipline_key": DISCIPLINE_ID,
                        "discipline_name": DISCIPLINE_NAME,
                        "event_name": f"{gender.title()} Elite",
                        "gender": gender,
                        "rank": rank,
                        "medal": RANK_TO_MEDAL[rank],
                        "participant_type": "athlete",
                        "participant_name": rider_name,
                        "country_name": resolved_country_name,
                        "country_code": country_code,
                        "source_url": source_url,
                    }
                )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Build UCI Cyclo-cross World Championships elite podium seed.")
    parser.add_argument("--max-year", type=int, default=2026)
    parser.add_argument("--out", type=Path, default=SEED_PATH)
    args = parser.parse_args()

    rows = build_rows(max(START_YEAR, int(args.max_year)))
    if not rows:
        raise RuntimeError("No UCI Cyclo-cross World Championships rows extracted.")

    frame = pd.DataFrame(rows)
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype(int)
    frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce").astype(int)
    frame = frame.loc[frame["year"] > 2000].copy()
    frame = frame.sort_values(["year", "gender", "rank", "participant_name"]).reset_index(drop=True)

    profiles = (
        frame.groupby(["year", "gender"])["rank"]
        .apply(lambda values: tuple(sorted(int(value) for value in values.tolist())))
        .to_dict()
    )
    invalid_profiles = {key: value for key, value in profiles.items() if value != (1, 2, 3)}
    if invalid_profiles:
        sample = dict(list(invalid_profiles.items())[:30])
        raise RuntimeError(f"Unexpected UCI Cyclo-cross rank profile(s): {sample}")

    duplicate_events = frame.loc[
        frame.duplicated(subset=["year", "gender", "participant_name", "country_code"], keep=False)
    ]
    if not duplicate_events.empty:
        sample = duplicate_events[["year", "gender", "participant_name", "country_code"]].head(20).to_dict("records")
        raise RuntimeError(f"Duplicate rider in same UCI Cyclo-cross event: {sample}")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(args.out, index=False)

    years = sorted(int(year) for year in frame["year"].unique().tolist())
    event_count = frame[["year", "gender"]].drop_duplicates().shape[0]
    print(
        f"[seed] uci_cyclocross_world_championships rows={len(frame)} "
        f"years={years[0]}-{years[-1]} events={event_count} out={args.out}"
    )


if __name__ == "__main__":
    main()
