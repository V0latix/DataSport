from __future__ import annotations

from io import StringIO
from pathlib import Path

import pandas as pd
import requests


SOURCE_URL = "https://en.wikipedia.org/wiki/FIFA_Futsal_World_Cup"
OUT_FILE = Path(__file__).resolve().with_name("fifa_futsal_world_cup_top4_seed.csv")
COMPETITION_ID = "fifa_futsal_world_cup"
COMPETITION_NAME = "FIFA Futsal World Cup"
DISCIPLINE_KEY = "futsal"
DISCIPLINE_NAME = "Futsal"
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze", 4: ""}
COUNTRY_CODE_OVERRIDES = {
    "Argentina": "ARG",
    "Brazil": "BRA",
    "Colombia": "COL",
    "France": "FRA",
    "Iran": "IRN",
    "Italy": "ITA",
    "Kazakhstan": "KAZ",
    "Portugal": "PRT",
    "Russia": "RUS",
    "Spain": "ESP",
    "Ukraine": "UKR",
}


def country_code(country_name: str) -> str:
    if country_name in COUNTRY_CODE_OVERRIDES:
        return COUNTRY_CODE_OVERRIDES[country_name]
    try:
        import pycountry

        return pycountry.countries.lookup(country_name).alpha_3
    except Exception as exc:
        raise RuntimeError(f"Missing country code mapping for {country_name!r}") from exc


def clean_year(value: object) -> int | None:
    text = str(value).strip()
    year_text = "".join(ch for ch in text[:4] if ch.isdigit())
    if len(year_text) != 4:
        return None
    return int(year_text)


def editions_table(html: str) -> pd.DataFrame:
    for frame in pd.read_html(StringIO(html)):
        flat_columns = [
            " ".join(str(part).strip() for part in column if str(part) != "nan")
            if isinstance(column, tuple)
            else str(column)
            for column in frame.columns
        ]
        if {
            "Year Year",
            "Final Champions",
            "Final Runners-up",
            "Third place game Third place",
            "Third place game Fourth place",
        }.issubset(set(flat_columns)):
            frame = frame.copy()
            frame.columns = flat_columns
            return frame
    raise RuntimeError("Could not find FIFA Futsal World Cup editions table.")


def build_seed() -> pd.DataFrame:
    response = requests.get(SOURCE_URL, headers={"User-Agent": "DataSport seed builder"}, timeout=60)
    response.raise_for_status()
    table = editions_table(response.text)
    table = table.rename(
        columns={
            "Year Year": "year",
            "Final Champions": "champion",
            "Final Runners-up": "runner_up",
            "Third place game Third place": "third_place",
            "Third place game Fourth place": "fourth_place",
        }
    )
    required_cols = {"year", "champion", "runner_up", "third_place", "fourth_place"}
    if not required_cols.issubset(set(table.columns)):
        raise RuntimeError(f"Unsupported FIFA Futsal World Cup table columns: {list(table.columns)}")

    rows: list[dict[str, object]] = []
    for record in table.to_dict("records"):
        year = clean_year(record["year"])
        if year is None or year <= 2000:
            continue

        ranked = [
            (1, str(record["champion"]).strip()),
            (2, str(record["runner_up"]).strip()),
            (3, str(record["third_place"]).strip()),
            (4, str(record["fourth_place"]).strip()),
        ]
        for rank, team_name in ranked:
            if not team_name or team_name.lower() in {"nan", "tbd"}:
                continue
            rows.append(
                {
                    "competition_id": COMPETITION_ID,
                    "competition_name": COMPETITION_NAME,
                    "year": year,
                    "event_date": f"{year}-12-31",
                    "discipline_key": DISCIPLINE_KEY,
                    "discipline_name": DISCIPLINE_NAME,
                    "gender": "men",
                    "rank": rank,
                    "medal": RANK_TO_MEDAL[rank],
                    "participant_type": "team",
                    "participant_name": team_name,
                    "country_name": team_name,
                    "country_code": country_code(team_name),
                    "source_url": SOURCE_URL,
                }
            )

    frame = pd.DataFrame(rows)
    if frame.empty:
        raise RuntimeError("No FIFA Futsal World Cup top4 rows extracted.")
    frame = frame.sort_values(["year", "rank", "country_code"]).reset_index(drop=True)
    profiles = frame.groupby("year")["rank"].apply(lambda values: tuple(sorted(values.tolist()))).to_dict()
    bad_profiles = {year: profile for year, profile in profiles.items() if profile != (1, 2, 3, 4)}
    if bad_profiles:
        raise RuntimeError(f"Unexpected FIFA Futsal World Cup rank profiles: {bad_profiles}")
    return frame


def main() -> None:
    frame = build_seed()
    frame.to_csv(OUT_FILE, index=False)
    print(f"Wrote {len(frame)} rows to {OUT_FILE}")
    print(frame.groupby("competition_id")["year"].agg(["min", "max", "nunique"]).to_string())


if __name__ == "__main__":
    main()
