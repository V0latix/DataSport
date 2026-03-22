from __future__ import annotations

import argparse
import re
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests


SEED_PATH = Path(__file__).resolve().parent / "fih_hockey_world_cup_top4_seed.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder)"}

SOURCES = [
    {
        "url": "https://en.wikipedia.org/wiki/Men%27s_FIH_Hockey_World_Cup",
        "competition_id": "fih_hockey_world_cup_men",
        "competition_name": "Men's FIH Hockey World Cup",
        "gender": "men",
    },
    {
        "url": "https://en.wikipedia.org/wiki/Women%27s_FIH_Hockey_World_Cup",
        "competition_id": "fih_hockey_world_cup_women",
        "competition_name": "Women's FIH Hockey World Cup",
        "gender": "women",
    },
]

REQUIRED_COLUMNS = {
    "Year Year",
    "Final Winner",
    "Final Runner-up",
    "Third place match Third place",
    "Third place match Fourth place",
}

RANK_COLUMNS = [
    (1, "Final Winner"),
    (2, "Final Runner-up"),
    (3, "Third place match Third place"),
    (4, "Third place match Fourth place"),
]

COUNTRY_OVERRIDES = {
    "England": "ENG",
    "Korea": "KOR",
    "South Korea": "KOR",
}


def _clean_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"[\*\^#†‡]+$", "", text).strip()
    if text.lower() in {"", "nan", "none", "tbd", "to be decided"}:
        return ""
    return text


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = [" ".join([str(part) for part in col if str(part) != "nan"]).strip() for col in out.columns]
    else:
        out.columns = [str(col).strip() for col in out.columns]
    return out


def _extract_year(value: Any) -> int | None:
    text = _clean_text(value)
    match = re.search(r"(19|20)\d{2}", text)
    if not match:
        return None
    return int(match.group(0))


def _resolve_country_code(country_name: str) -> str:
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


def _find_results_table(page_html: str, source_url: str) -> pd.DataFrame:
    tables = pd.read_html(StringIO(page_html))
    for table in tables:
        candidate = _flatten_columns(table)
        if REQUIRED_COLUMNS.issubset(set(candidate.columns)):
            return candidate
    raise RuntimeError(f"Could not locate FIH results table in {source_url}")


def _extract_rows(source: dict[str, str], max_year: int, start_year: int) -> list[dict[str, Any]]:
    response = requests.get(source["url"], headers=HEADERS, timeout=60)
    response.raise_for_status()
    table = _find_results_table(response.text, source["url"])

    rows: list[dict[str, Any]] = []
    for record in table.to_dict(orient="records"):
        year = _extract_year(record.get("Year Year"))
        if year is None or year < start_year or year > max_year:
            continue

        places: list[tuple[int, str, str]] = []
        for rank, column in RANK_COLUMNS:
            country_name = _clean_text(record.get(column))
            if not country_name:
                places = []
                break
            country_code = _resolve_country_code(country_name)
            if not country_code:
                places = []
                break
            places.append((rank, country_name, country_code))

        if not places:
            continue

        for rank, country_name, country_code in places:
            rows.append(
                {
                    "competition_id": source["competition_id"],
                    "competition_name": source["competition_name"],
                    "year": int(year),
                    "event_date": f"{int(year)}-12-31",
                    "discipline_key": "hockey",
                    "discipline_name": "Hockey",
                    "gender": source["gender"],
                    "rank": int(rank),
                    "participant_type": "team",
                    "participant_name": country_name,
                    "country_name": country_name,
                    "country_code": country_code,
                    "source_url": source["url"],
                }
            )

    return rows


def build_seed(start_year: int, max_year: int, output: Path) -> pd.DataFrame:
    all_rows: list[dict[str, Any]] = []
    for source in SOURCES:
        all_rows.extend(_extract_rows(source=source, max_year=max_year, start_year=start_year))

    frame = pd.DataFrame(all_rows)
    if frame.empty:
        raise RuntimeError("FIH Hockey World Cup seed extraction produced no rows.")

    frame = frame.drop_duplicates(
        subset=[
            "competition_id",
            "year",
            "rank",
            "participant_name",
            "country_code",
        ]
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
        raise RuntimeError(f"Unexpected rank profiles in FIH seed (expected 1,2,3,4): {sample}")

    output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output, index=False)
    return frame


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build FIH Hockey World Cup top4 seed from Wikipedia.")
    parser.add_argument("--start-year", type=int, default=2001)
    parser.add_argument("--max-year", type=int, default=datetime.now(timezone.utc).year)
    parser.add_argument("--output", type=Path, default=SEED_PATH)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    seed = build_seed(start_year=int(args.start_year), max_year=int(args.max_year), output=Path(args.output))
    print(f"[fih-hockey-seed] wrote {args.output} rows={len(seed)} years={seed.year.min()}-{seed.year.max()}")
    counts = seed.groupby(["competition_id", "year"]).size()
    print(f"[fih-hockey-seed] rows by competition/year:\n{counts}")


if __name__ == "__main__":
    main()
