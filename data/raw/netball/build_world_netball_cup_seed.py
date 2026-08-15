from __future__ import annotations

import re
from io import StringIO
from pathlib import Path

import pandas as pd
import requests


SOURCE_URL = "https://en.wikipedia.org/wiki/Netball_World_Cup"
OUT_FILE = Path(__file__).resolve().with_name("world_netball_cup_top4_seed.csv")
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze", 4: ""}
COUNTRY_CODE_OVERRIDES = {
    "Australia": "AUS",
    "England": "ENG",
    "Jamaica": "JAM",
    "New Zealand": "NZL",
    "South Africa": "ZAF",
}


def clean_text(value: object) -> str:
    value = re.sub(r"\[[^\]]+\]", "", str(value))
    value = re.sub(r"\([^)]*\)", "", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def clean_score(value: object) -> str:
    value = clean_text(value)
    match = re.search(r"\d+\s*[–-]\s*\d+", value)
    if not match:
        return value
    return re.sub(r"\s+", "", match.group(0))


def country_code(country_name: str) -> str:
    if country_name in COUNTRY_CODE_OVERRIDES:
        return COUNTRY_CODE_OVERRIDES[country_name]
    try:
        import pycountry

        return pycountry.countries.lookup(country_name).alpha_3
    except Exception as exc:
        raise RuntimeError(f"Missing country code mapping for {country_name!r}") from exc


def tournament_table(html: str) -> pd.DataFrame:
    for frame in pd.read_html(StringIO(html)):
        columns = frame.columns
        if isinstance(columns, pd.MultiIndex):
            frame.columns = [str(a if a == b else f"{a} {b}") for a, b in columns]
        if {
            "Year",
            "First/Second place Champion",
            "First/Second place Runner-up",
            "Third/Fourth place Third",
            "Third/Fourth place Fourth",
        }.issubset(set(map(str, frame.columns))):
            return frame
    raise RuntimeError("Could not find Netball World Cup tournament table.")


def build_seed() -> pd.DataFrame:
    response = requests.get(SOURCE_URL, headers={"User-Agent": "DataSport seed builder"}, timeout=60)
    response.raise_for_status()
    table = tournament_table(response.text)

    rows: list[dict[str, object]] = []
    for record in table.to_dict("records"):
        year_text = clean_text(record["Year"])
        if not re.fullmatch(r"\d{4}", year_text):
            continue
        year = int(year_text)
        if year <= 2000:
            continue

        entries = [
            (1, clean_text(record["First/Second place Champion"])),
            (2, clean_text(record["First/Second place Runner-up"])),
            (3, clean_text(record["Third/Fourth place Third"])),
            (4, clean_text(record["Third/Fourth place Fourth"])),
        ]
        if any(not name or name in {"—N/a", "N/a", "nan"} for _, name in entries):
            continue
        final_score = clean_score(record.get("First/Second place Score", ""))
        third_place_score = clean_score(record.get("Third/Fourth place Score", ""))
        host = clean_text(record.get("Host", ""))
        for rank, team_name in entries:
            rows.append(
                {
                    "competition_id": "world_netball_cup",
                    "competition_name": "World Netball Cup",
                    "year": year,
                    "event_date": f"{year}-12-31",
                    "discipline_key": "netball",
                    "discipline_name": "Netball",
                    "gender": "women",
                    "rank": rank,
                    "medal": RANK_TO_MEDAL[rank],
                    "participant_type": "team",
                    "participant_name": team_name,
                    "country_name": team_name,
                    "country_code": country_code(team_name),
                    "host": host,
                    "final_score": final_score,
                    "third_place_score": third_place_score,
                    "source_url": SOURCE_URL,
                }
            )

    frame = pd.DataFrame(rows)
    if frame.empty:
        raise RuntimeError("No World Netball Cup top4 rows extracted.")
    frame = frame.sort_values(["year", "rank"]).reset_index(drop=True)
    return frame


def main() -> None:
    frame = build_seed()
    profiles = frame.groupby("year")["rank"].apply(lambda values: tuple(sorted(values.tolist()))).to_dict()
    bad_profiles = {key: value for key, value in profiles.items() if value != (1, 2, 3, 4)}
    if bad_profiles:
        sample = dict(list(bad_profiles.items())[:20])
        raise RuntimeError(f"Unexpected World Netball Cup rank profiles: {sample}")
    frame.to_csv(OUT_FILE, index=False)
    print(f"Wrote {len(frame)} rows to {OUT_FILE}")
    print(frame.groupby("competition_id")["year"].agg(["min", "max", "nunique"]).to_string())


if __name__ == "__main__":
    main()
