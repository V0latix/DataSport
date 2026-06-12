from __future__ import annotations

import argparse
import io
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import requests


BASE_DIR = Path(__file__).resolve().parent
SEED_PATH = BASE_DIR / "world_triathlon_championship_series_top3_seed.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder)"}

COMPETITION_ID = "world_triathlon_championship_series"
COMPETITION_NAME = "World Triathlon Championship Series"
START_YEAR = 2009
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}


@dataclass(frozen=True)
class TableConfig:
    source_url: str
    men_table: int
    women_table: int
    score_label: str
    note: str = "series_final_standings"


PAGES: dict[int, TableConfig] = {
    2009: TableConfig("https://en.wikipedia.org/wiki/2009_ITU_World_Championship_Series", 2, 3, "points"),
    2010: TableConfig("https://en.wikipedia.org/wiki/2010_ITU_World_Championship_Series", 2, 3, "points"),
    2011: TableConfig("https://en.wikipedia.org/wiki/2011_ITU_World_Championship_Series", 17, 28, "points"),
    2012: TableConfig("https://en.wikipedia.org/wiki/2012_ITU_World_Triathlon_Series", 4, 5, "points"),
    2013: TableConfig("https://en.wikipedia.org/wiki/2013_ITU_World_Triathlon_Series", 5, 6, "points"),
    2014: TableConfig("https://en.wikipedia.org/wiki/2014_ITU_World_Triathlon_Series", 5, 6, "points"),
    2015: TableConfig("https://en.wikipedia.org/wiki/2015_ITU_World_Triathlon_Series", 5, 6, "points"),
    2016: TableConfig("https://en.wikipedia.org/wiki/2016_ITU_World_Triathlon_Series", 5, 6, "points"),
    2017: TableConfig("https://en.wikipedia.org/wiki/2017_ITU_World_Triathlon_Series", 5, 6, "points"),
    2018: TableConfig("https://en.wikipedia.org/wiki/2018_ITU_World_Triathlon_Series", 6, 7, "points"),
    2019: TableConfig("https://en.wikipedia.org/wiki/2019_ITU_World_Triathlon_Series", 6, 7, "points"),
    2020: TableConfig(
        "https://en.wikipedia.org/wiki/2020_ITU_World_Triathlon_Series",
        3,
        4,
        "time",
        "covid_single_race_world_championship",
    ),
    2021: TableConfig(
        "https://en.wikipedia.org/wiki/2021_World_Triathlon_Championship_Series", 6, 7, "points"
    ),
    2022: TableConfig(
        "https://en.wikipedia.org/wiki/2022_World_Triathlon_Championship_Series", 7, 8, "points"
    ),
    2023: TableConfig(
        "https://en.wikipedia.org/wiki/2023_World_Triathlon_Championship_Series", 22, 23, "points"
    ),
    2024: TableConfig(
        "https://en.wikipedia.org/wiki/2024_World_Triathlon_Championship_Series", 17, 16, "points"
    ),
    2025: TableConfig(
        "https://en.wikipedia.org/wiki/2025_World_Triathlon_Championship_Series", 22, 23, "points"
    ),
}

COUNTRY_CODE_NORMALIZATION = {
    "BER": "BMU",
    "CHI": "CHL",
    "GER": "DEU",
    "NED": "NLD",
    "POR": "PRT",
    "RSA": "ZAF",
    "SUI": "CHE",
}
COUNTRY_NAME_ALIASES = {
    "Australia": "AUS",
    "Bermuda": "BMU",
    "Brazil": "BRA",
    "France": "FRA",
    "Germany": "DEU",
    "Great Britain": "GBR",
    "New Zealand": "NZL",
    "Portugal": "PRT",
    "Russia": "RUS",
    "South Africa": "ZAF",
    "Spain": "ESP",
    "Sweden": "SWE",
    "Switzerland": "CHE",
    "United States": "USA",
}
COUNTRY_NAME_OVERRIDES = {
    "BMU": "Bermuda",
    "GBR": "United Kingdom",
}


def clean_text(value: object) -> str:
    text = str(value or "")
    text = text.replace("\xa0", " ")
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = text.replace("*", "")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


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


def flatten_column_name(column: object) -> str:
    if isinstance(column, tuple):
        parts = [clean_text(part) for part in column if clean_text(part) and not clean_text(part).startswith("Unnamed")]
        deduped: list[str] = []
        for part in parts:
            if part not in deduped:
                deduped.append(part)
        return " ".join(deduped)
    return clean_text(column)


def find_column(frame: pd.DataFrame, candidates: tuple[str, ...]) -> object:
    labels = {column: flatten_column_name(column).lower() for column in frame.columns}
    for candidate in candidates:
        candidate_lower = candidate.lower()
        for column, label in labels.items():
            if label == candidate_lower:
                return column
    for candidate in candidates:
        candidate_lower = candidate.lower()
        for column, label in labels.items():
            if candidate_lower in label:
                return column
    raise RuntimeError(f"Could not find one of {candidates!r} in columns {list(labels.values())!r}")


def parse_country_from_text(value: object) -> tuple[str, str]:
    text = clean_text(value)
    code_match = re.search(r"\((?P<code>[A-Z]{3})\)\s*$", text)
    if code_match:
        code = normalize_country_code(code_match.group("code"))
        name = clean_text(re.sub(r"\([A-Z]{3}\)\s*$", "", text)) or country_name(code)
        return country_name(code) if len(name) <= 3 else name, code
    if text in COUNTRY_ALIASES:
        code = normalize_country_code(COUNTRY_ALIASES[text])
        return country_name(code), code
    raise RuntimeError(f"Could not parse country value: {text!r}")


def parse_athlete_and_country(athlete_value: object, nation_value: object | None = None) -> tuple[str, str, str]:
    athlete_text = clean_text(athlete_value)
    source_country_name = ""
    source_country_code = ""
    code_match = re.search(r"\((?P<code>[A-Z]{3})\)\s*$", athlete_text)
    if code_match:
        source_country_code = normalize_country_code(code_match.group("code"))
        athlete_text = clean_text(re.sub(r"\([A-Z]{3}\)\s*$", "", athlete_text))
        source_country_name = country_name(source_country_code)
    elif nation_value is not None:
        source_country_name, source_country_code = parse_country_from_text(nation_value)
    else:
        raise RuntimeError(f"Could not parse athlete/country value: {athlete_text!r}")

    if not athlete_text or not source_country_code:
        raise RuntimeError(f"Incomplete athlete/country parse: {athlete_value!r}, {nation_value!r}")
    return athlete_text, country_name(source_country_code), source_country_code


def fetch_tables(source_url: str) -> list[pd.DataFrame]:
    response = requests.get(source_url, headers=HEADERS, timeout=45)
    response.raise_for_status()
    return pd.read_html(io.StringIO(response.text))


def extract_top3(table: pd.DataFrame, year: int, gender: str, config: TableConfig) -> list[dict[str, Any]]:
    athlete_column = find_column(table, ("Athlete", "Name"))
    nation_column = None
    try:
        nation_column = find_column(table, ("Nation",))
    except RuntimeError:
        nation_column = None

    score_column = find_column(table, ("WTCS Points", "Total points", "Points", "Total", "Time"))

    rows: list[dict[str, Any]] = []
    for rank, row in enumerate(table.head(3).itertuples(index=False, name=None), start=1):
        values = dict(zip(table.columns, row))
        athlete_name, resolved_country_name, country_code = parse_athlete_and_country(
            values[athlete_column], values[nation_column] if nation_column is not None else None
        )
        score_value = clean_text(values[score_column])
        rows.append(
            {
                "competition_id": COMPETITION_ID,
                "competition_name": COMPETITION_NAME,
                "year": year,
                "event_date": f"{year}-12-31",
                "discipline_key": "triathlon",
                "discipline_name": "Triathlon",
                "event_key": "elite_final_standings",
                "event_name": f"{year} {COMPETITION_NAME} {gender.title()}",
                "gender": gender,
                "rank": rank,
                "medal": RANK_TO_MEDAL[rank],
                "participant_type": "athlete",
                "participant_name": athlete_name,
                "country_name": resolved_country_name,
                "country_code": country_code,
                "score_label": config.score_label,
                "score_value": score_value,
                "source_note": config.note,
                "source_url": config.source_url,
            }
        )
    return rows


def build_rows(max_year: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for year, config in PAGES.items():
        if year < START_YEAR or year > int(max_year):
            continue
        tables = fetch_tables(config.source_url)
        for gender, table_index in (("men", config.men_table), ("women", config.women_table)):
            if table_index >= len(tables):
                raise RuntimeError(f"Missing table {table_index} for {year} at {config.source_url}")
            rows.extend(extract_top3(tables[table_index], year, gender, config))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Build World Triathlon Championship Series top-three seed.")
    parser.add_argument("--max-year", type=int, default=2026)
    parser.add_argument("--out", type=Path, default=SEED_PATH)
    args = parser.parse_args()

    rows = build_rows(max(START_YEAR, int(args.max_year)))
    if not rows:
        raise RuntimeError("No World Triathlon Championship Series rows extracted.")

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
        sample = dict(list(invalid_profiles.items())[:20])
        raise RuntimeError(f"Unexpected WTCS rank profile(s): {sample}")

    duplicates = frame.loc[
        frame.duplicated(subset=["year", "gender", "participant_name", "country_code"], keep=False)
    ]
    if not duplicates.empty:
        sample = duplicates[["year", "gender", "participant_name", "country_code"]].head(20).to_dict("records")
        raise RuntimeError(f"Duplicate entry in same WTCS event: {sample}")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(args.out, index=False)

    years = sorted(int(year) for year in frame["year"].unique().tolist())
    event_count = frame[["year", "gender"]].drop_duplicates().shape[0]
    print(
        f"[seed] world_triathlon_championship_series rows={len(frame)} "
        f"years={years[0]}-{years[-1]} events={event_count} out={args.out}"
    )


if __name__ == "__main__":
    main()
