from __future__ import annotations

from io import StringIO
from pathlib import Path
import re

import pandas as pd
import requests


OPEN_SOURCE_URL = "https://en.wikipedia.org/wiki/Chess_Olympiad"
WOMEN_SOURCE_URL = "https://en.wikipedia.org/wiki/Women%27s_Chess_Olympiad"
OUT_FILE = Path(__file__).resolve().with_name("fide_chess_olympiad_team_podium_seed.csv")
DISCIPLINE_KEY = "chess-team"
DISCIPLINE_NAME = "Team chess"
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}
COMPETITIONS = {
    "open": {
        "competition_id": "fide_chess_olympiad_open",
        "competition_name": "FIDE Chess Olympiad (Open)",
        "source_url": OPEN_SOURCE_URL,
        "table_index": 2,
    },
    "women": {
        "competition_id": "fide_chess_olympiad_women",
        "competition_name": "FIDE Chess Olympiad (Women)",
        "source_url": WOMEN_SOURCE_URL,
        "table_index": 0,
    },
}
TEAM_CODE_OVERRIDES = {
    "Armenia": ("ARM", "ARM"),
    "China": ("CHN", "CHN"),
    "Georgia": ("GEO", "GEO"),
    "Hungary": ("HUN", "HUN"),
    "India": ("IND", "IND"),
    "India 2": ("IND2", "IND"),
    "Israel": ("ISR", "ISR"),
    "Kazakhstan": ("KAZ", "KAZ"),
    "Poland": ("POL", "POL"),
    "Russia": ("RUS", "RUS"),
    "Ukraine": ("UKR", "UKR"),
    "United States": ("USA", "USA"),
    "Uzbekistan": ("UZB", "UZB"),
}


def clean_text(value: object) -> str:
    text = str(value).strip()
    text = re.sub(r"\[[^\]]+\]", "", text)
    return re.sub(r"\s+", " ", text).strip()


def clean_year(value: object) -> int | None:
    match = re.search(r"\d{4}", clean_text(value))
    if not match:
        return None
    return int(match.group(0))


def medal_team(value: object) -> str:
    text = clean_text(value)
    if not text or text.lower() == "nan":
        return ""
    for team_name in sorted(TEAM_CODE_OVERRIDES, key=len, reverse=True):
        if text == team_name or text.startswith(f"{team_name} "):
            return team_name
    raise RuntimeError(f"Missing Chess Olympiad team mapping for medal cell: {text!r}")


def load_table(source_url: str, table_index: int) -> pd.DataFrame:
    response = requests.get(source_url, headers={"User-Agent": "DataSport seed builder"}, timeout=60)
    response.raise_for_status()
    table = pd.read_html(StringIO(response.text))[table_index]
    required_cols = {"Year", "Event", "Gold", "Silver", "Bronze"}
    if not required_cols.issubset(set(table.columns)):
        raise RuntimeError(f"Unsupported Chess Olympiad table columns for {source_url}: {list(table.columns)}")
    return table


def build_rows(gender: str, config: dict[str, object]) -> list[dict[str, object]]:
    table = load_table(str(config["source_url"]), int(config["table_index"]))
    rows: list[dict[str, object]] = []
    for record in table.to_dict("records"):
        year = clean_year(record["Year"])
        event_name = clean_text(record["Event"])
        if year is None or year <= 2000 or "Online Chess Olympiad" in event_name:
            continue

        ranked = [
            (1, medal_team(record["Gold"])),
            (2, medal_team(record["Silver"])),
            (3, medal_team(record["Bronze"])),
        ]
        for rank, team_name in ranked:
            if not team_name:
                continue
            participant_id, country_id = TEAM_CODE_OVERRIDES[team_name]
            rows.append(
                {
                    "competition_id": config["competition_id"],
                    "competition_name": config["competition_name"],
                    "year": year,
                    "event_date": f"{year}-12-31",
                    "discipline_key": DISCIPLINE_KEY,
                    "discipline_name": DISCIPLINE_NAME,
                    "gender": gender,
                    "rank": rank,
                    "medal": RANK_TO_MEDAL[rank],
                    "participant_type": "team",
                    "participant_id": participant_id,
                    "participant_name": team_name,
                    "country_name": "India" if team_name == "India 2" else team_name,
                    "country_id": country_id,
                    "source_url": config["source_url"],
                }
            )
    return rows


def build_seed() -> pd.DataFrame:
    rows = build_rows("open", COMPETITIONS["open"]) + build_rows("women", COMPETITIONS["women"])
    frame = pd.DataFrame(rows)
    if frame.empty:
        raise RuntimeError("No FIDE Chess Olympiad podium rows extracted.")
    frame = frame.sort_values(["competition_id", "year", "rank", "participant_id"]).reset_index(drop=True)

    profiles = (
        frame.groupby(["competition_id", "year"])["rank"]
        .apply(lambda values: tuple(sorted(values.tolist())))
        .to_dict()
    )
    bad_profiles = {key: profile for key, profile in profiles.items() if profile != (1, 2, 3)}
    if bad_profiles:
        raise RuntimeError(f"Unexpected FIDE Chess Olympiad rank profiles: {bad_profiles}")
    return frame


def main() -> None:
    frame = build_seed()
    frame.to_csv(OUT_FILE, index=False)
    print(f"Wrote {len(frame)} rows to {OUT_FILE}")
    print(frame.groupby("competition_id")["year"].agg(["min", "max", "nunique"]).to_string())


if __name__ == "__main__":
    main()
