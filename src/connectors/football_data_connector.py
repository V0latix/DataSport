from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

import pandas as pd

from src.core.db import SQLiteDB
from src.core.utils import optional_iso2_to_iso3, slugify, stable_id, utc_now_iso

from .base import Connector, MissingCredentialError


NATIONAL_COMPETITION_CODES = {
    "WC",
    "EC",
    "UNL",
    "WQC",
    "EQC",
    "WWC",
    "ENC",
}

NON_ISO_TEAM_CODE_MAP = {
    "ENG": "GBR",
    "SCO": "GBR",
    "WAL": "GBR",
    "NIR": "GBR",
    "KOS": "XKX",
}


class FootballDataConnector(Connector):
    id = "football_data"
    name = "football-data.org"
    source_type = "api"
    license_notes = "Free tier terms apply; do not republish restricted payloads."
    base_url = "https://api.football-data.org/v4"

    def fetch(self, season_year: int, out_dir: Path) -> list[Path]:
        token = os.getenv("FOOTBALL_DATA_TOKEN")
        if not token:
            raise MissingCredentialError("FOOTBALL_DATA_TOKEN is missing; connector skipped.")

        headers = {"X-Auth-Token": token}
        raw_paths: list[Path] = []

        competitions_url = f"{self.base_url}/competitions"
        competitions_payload = self._request_json(competitions_url, headers=headers, params={})
        competitions_path = out_dir / f"competitions_{season_year}.json"
        self._write_json(competitions_path, competitions_payload)
        raw_paths.append(competitions_path)

        competitions = competitions_payload.get("competitions", []) or []
        selected = [
            comp
            for comp in competitions
            if str(comp.get("code", "")).upper() in NATIONAL_COMPETITION_CODES
        ]

        for comp in selected:
            code = str(comp.get("code", "")).upper()
            if not code:
                continue
            standings_url = f"{self.base_url}/competitions/{code}/standings"
            params = {"season": season_year}
            try:
                standings_payload = self._request_json(standings_url, headers=headers, params=params, retries=3)
            except Exception as exc:
                standings_payload = {"error": str(exc), "competition_code": code, "season": season_year}
            path = out_dir / f"standings_{code}_{season_year}.json"
            self._write_json(path, standings_payload)
            raw_paths.append(path)
            time.sleep(0.35)

        return raw_paths

    def _resolve_country_id(self, team_obj: dict[str, Any]) -> str | None:
        tla = str(team_obj.get("tla", "")).upper().strip()
        if len(tla) == 3:
            if tla in NON_ISO_TEAM_CODE_MAP:
                return NON_ISO_TEAM_CODE_MAP[tla]
            return tla
        area = team_obj.get("area") or {}
        iso2 = area.get("countryCode") or area.get("code")
        country_id = optional_iso2_to_iso3(iso2)
        return country_id

    def parse(self, raw_paths: list[Path], season_year: int) -> dict[str, pd.DataFrame]:
        competitions_file = next((path for path in raw_paths if path.name.startswith("competitions_")), None)
        competitions_lookup: dict[str, dict[str, Any]] = {}
        if competitions_file:
            payload = json.loads(competitions_file.read_text(encoding="utf-8"))
            for competition in payload.get("competitions", []) or []:
                code = str(competition.get("code", "")).upper()
                if code:
                    competitions_lookup[code] = competition

        timestamp = utc_now_iso()
        sports_rows = [
            {
                "sport_id": slugify("Football"),
                "sport_name": "Football",
                "sport_slug": slugify("Football"),
                "created_at_utc": timestamp,
            }
        ]

        competition_rows: list[dict[str, Any]] = []
        event_rows: list[dict[str, Any]] = []
        participant_rows: dict[str, dict[str, Any]] = {}
        result_rows: list[dict[str, Any]] = []

        for path in raw_paths:
            if not path.name.startswith("standings_"):
                continue
            payload = json.loads(path.read_text(encoding="utf-8"))
            if "error" in payload:
                continue

            competition = payload.get("competition") or {}
            code = str(competition.get("code", "")).upper()
            if not code:
                code = path.name.split("_")[1]
            info = competitions_lookup.get(code, {})
            competition_name = competition.get("name") or info.get("name") or code
            competition_id = stable_id(self.id, "competition", code, season_year)

            competition_rows.append(
                {
                    "competition_id": competition_id,
                    "sport_id": slugify("Football"),
                    "name": competition_name,
                    "season_year": season_year,
                    "level": info.get("type") or competition.get("type") or "national_team",
                    "start_date": info.get("currentSeason", {}).get("startDate"),
                    "end_date": info.get("currentSeason", {}).get("endDate"),
                    "source_id": self.id,
                }
            )

            event_id = stable_id(self.id, "event", competition_id, "standings")
            event_rows.append(
                {
                    "event_id": event_id,
                    "competition_id": competition_id,
                    "discipline_id": None,
                    "gender": "mixed",
                    "event_class": "standings",
                    "event_date": info.get("currentSeason", {}).get("endDate"),
                }
            )

            standings = payload.get("standings", []) or []
            for standing in standings:
                for row in standing.get("table", []) or []:
                    team = row.get("team", {}) or {}
                    team_name = team.get("name") or team.get("shortName") or "Unknown team"
                    participant_id = stable_id(self.id, "team", team.get("id") or team_name)
                    participant_rows[participant_id] = {
                        "participant_id": participant_id,
                        "type": "team",
                        "display_name": team_name,
                        "country_id": self._resolve_country_id(team),
                    }

                    rank = row.get("position")
                    points = row.get("points")
                    medal = None
                    if rank == 1:
                        medal = "gold"
                    elif rank == 2:
                        medal = "silver"
                    elif rank == 3:
                        medal = "bronze"
                    result_rows.append(
                        {
                            "event_id": event_id,
                            "participant_id": participant_id,
                            "rank": rank,
                            "medal": medal,
                            "score_raw": f"points={points}",
                            "points_awarded": points,
                        }
                    )

        return {
            "sports": pd.DataFrame(sports_rows).drop_duplicates(subset=["sport_id"]),
            "competitions": pd.DataFrame(competition_rows).drop_duplicates(subset=["competition_id"]),
            "events": pd.DataFrame(event_rows).drop_duplicates(subset=["event_id"]),
            "participants": pd.DataFrame(participant_rows.values()),
            "results": pd.DataFrame(result_rows).drop_duplicates(subset=["event_id", "participant_id"]),
            "sport_federations": pd.DataFrame(),
        }

    def upsert(self, db: SQLiteDB, payload: dict[str, pd.DataFrame]) -> None:
        db.upsert_dataframe("sports", payload.get("sports", pd.DataFrame()), ["sport_id"])
        db.upsert_dataframe("competitions", payload.get("competitions", pd.DataFrame()), ["competition_id"])
        db.upsert_dataframe("events", payload.get("events", pd.DataFrame()), ["event_id"])
        db.upsert_dataframe("participants", payload.get("participants", pd.DataFrame()), ["participant_id"])
        db.upsert_dataframe("results", payload.get("results", pd.DataFrame()), ["event_id", "participant_id"])

