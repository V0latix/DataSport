from __future__ import annotations

import argparse
import re
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests


BASE_DIR = Path(__file__).resolve().parent
SEED_PATH = BASE_DIR / "iihf_ice_hockey_world_championship_top4_seed.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder)"}

SOURCES = [
    {
        "url": "https://en.wikipedia.org/wiki/List_of_IIHF_World_Championship_medalists",
        "competition_id": "iihf_ice_hockey_world_championship_men",
        "competition_name": "IIHF Ice Hockey World Championship (Men)",
        "gender": "men",
        "table_kind": "men",
    },
    {
        "url": "https://en.wikipedia.org/wiki/IIHF_Women%27s_World_Championship",
        "competition_id": "iihf_ice_hockey_world_championship_women",
        "competition_name": "IIHF Women's World Championship",
        "gender": "women",
        "table_kind": "women",
    },
]

RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze", 4: ""}
COUNTRY_OVERRIDES = {
    "Czech Republic": "CZE",
    "Czechia": "CZE",
    "Denmark": "DNK",
    "Germany": "DEU",
    "Norway": "NOR",
    "Russia": "RUS",
    "Soviet Union": "URS",
    "Sweden": "SWE",
    "Switzerland": "CHE",
    "United States": "USA",
}


def clean_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"[\*\^#†‡]+$", "", text).strip()
    text = re.sub(r"\s+\(\d+(?:/\d+)?\)$", "", text).strip()
    if text.lower() in {"", "nan", "none", "tbd", "to be decided"}:
        return ""
    return text


def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if isinstance(out.columns, pd.MultiIndex):
        column_names: list[str] = []
        for col in out.columns:
            parts: list[str] = []
            for part in col:
                value = str(part)
                if not value or value.startswith("Unnamed:"):
                    continue
                if parts and parts[-1] == value:
                    continue
                parts.append(value)
            column_names.append(" ".join(parts).strip())
        out.columns = column_names
    else:
        out.columns = [str(col).strip() for col in out.columns]
    return out


def extract_year(value: Any) -> int | None:
    match = re.search(r"(19|20)\d{2}", clean_text(value))
    return int(match.group(0)) if match else None


def resolve_country_code(country_name: str) -> str:
    alias = COUNTRY_OVERRIDES.get(country_name)
    if alias:
        return alias

    try:
        import pycountry

        country = pycountry.countries.lookup(country_name)
        code = getattr(country, "alpha_3", None)
        if code:
            return str(code).upper()
    except Exception:
        pass

    return re.sub(r"[^A-Za-z0-9]", "", country_name.upper())[:3]


def find_results_table(page_html: str, table_kind: str, source_url: str) -> pd.DataFrame:
    tables = pd.read_html(StringIO(page_html))
    if table_kind == "men":
        required = {"Year", "Gold", "Silver", "Bronze", "4th place"}
        for table in tables:
            candidate = flatten_columns(table)
            candidate = candidate.loc[:, [bool(col) for col in candidate.columns]]
            candidate = candidate.loc[:, ~candidate.columns.duplicated()]
            if required.issubset(set(candidate.columns)):
                return candidate
    else:
        required = {
            "Year",
            "Final Champions",
            "Final Runners-up",
            "Third place match Third place",
            "Third place match Fourth place",
        }
        for table in tables:
            candidate = flatten_columns(table)
            candidate = candidate.loc[:, [bool(col) for col in candidate.columns]]
            candidate = candidate.loc[:, ~candidate.columns.duplicated()]
            if required.issubset(set(candidate.columns)):
                return candidate

    raise RuntimeError(f"Could not locate IIHF {table_kind} results table in {source_url}")


def rank_columns(table_kind: str) -> list[tuple[int, str]]:
    if table_kind == "men":
        return [(1, "Gold"), (2, "Silver"), (3, "Bronze"), (4, "4th place")]
    return [
        (1, "Final Champions"),
        (2, "Final Runners-up"),
        (3, "Third place match Third place"),
        (4, "Third place match Fourth place"),
    ]


def extract_rows(source: dict[str, str], start_year: int, max_year: int) -> list[dict[str, Any]]:
    response = requests.get(source["url"], headers=HEADERS, timeout=60)
    response.raise_for_status()
    table = find_results_table(response.text, source["table_kind"], source["url"])

    rows: list[dict[str, Any]] = []
    skipped_years: list[int] = []
    for record in table.to_dict(orient="records"):
        year = extract_year(record.get("Year"))
        if year is None or year < start_year or year > max_year:
            continue

        placements: list[tuple[int, str, str]] = []
        for rank, column in rank_columns(source["table_kind"]):
            country_name = clean_text(record.get(column))
            if not country_name or "cancelled" in country_name.lower() or "competition not held" in country_name.lower():
                placements = []
                skipped_years.append(year)
                break
            country_code = resolve_country_code(country_name)
            if not country_code:
                placements = []
                skipped_years.append(year)
                break
            placements.append((rank, country_name, country_code))

        for rank, country_name, country_code in placements:
            rows.append(
                {
                    "competition_id": source["competition_id"],
                    "competition_name": source["competition_name"],
                    "year": year,
                    "event_date": f"{year}-12-31",
                    "discipline_key": "ice-hockey",
                    "discipline_name": "Ice Hockey",
                    "gender": source["gender"],
                    "rank": rank,
                    "medal": RANK_TO_MEDAL[rank],
                    "participant_type": "team",
                    "participant_name": country_name,
                    "country_name": country_name,
                    "country_code": country_code,
                    "source_url": source["url"],
                }
            )

    if skipped_years:
        print(
            f"[iihf-seed] skipped incomplete/cancelled years for {source['competition_id']}: "
            f"{sorted(set(skipped_years))}"
        )

    return rows


def build_seed(start_year: int, max_year: int, output: Path) -> pd.DataFrame:
    all_rows: list[dict[str, Any]] = []
    for source in SOURCES:
        all_rows.extend(extract_rows(source=source, start_year=start_year, max_year=max_year))

    frame = pd.DataFrame(all_rows)
    if frame.empty:
        raise RuntimeError("IIHF Ice Hockey World Championship seed extraction produced no rows.")

    frame = frame.loc[frame["year"].astype(int) > 2000].copy()
    frame = frame.drop_duplicates(
        subset=["competition_id", "year", "rank", "participant_name", "country_code"]
    )
    frame = frame.sort_values(["competition_id", "year", "rank", "participant_name"]).reset_index(drop=True)

    profiles = (
        frame.groupby(["competition_id", "year"])["rank"]
        .apply(lambda s: tuple(sorted(int(v) for v in s.tolist())))
        .to_dict()
    )
    expected_profile = (1, 2, 3, 4)
    bad_profiles = {key: value for key, value in profiles.items() if value != expected_profile}
    if bad_profiles:
        sample = dict(list(bad_profiles.items())[:20])
        raise RuntimeError(f"Unexpected IIHF rank profiles (expected 1,2,3,4): {sample}")

    output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output, index=False)
    return frame


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build IIHF Ice Hockey World Championship top4 seed.")
    parser.add_argument("--start-year", type=int, default=2001)
    parser.add_argument("--max-year", type=int, default=datetime.now(timezone.utc).year)
    parser.add_argument("--output", type=Path, default=SEED_PATH)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    seed = build_seed(start_year=int(args.start_year), max_year=int(args.max_year), output=Path(args.output))
    print(f"[iihf-seed] wrote {args.output} rows={len(seed)} years={seed.year.min()}-{seed.year.max()}")
    counts = seed.groupby(["competition_id", "year"]).size()
    print(f"[iihf-seed] rows by competition/year:\n{counts}")


if __name__ == "__main__":
    main()
