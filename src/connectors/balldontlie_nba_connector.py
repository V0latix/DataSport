from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

import pandas as pd

from src.core.db import SQLiteDB
from src.core.utils import slugify, stable_id, utc_now_iso

from .base import Connector, MissingCredentialError


class BallDontLieNBAConnector(Connector):
    id = "balldontlie_nba"
    name = "balldontlie NBA"
    source_type = "api"
    license_notes = "API terms apply; mostly club data, country mapping is heuristic."
    base_url = "https://api.balldontlie.io/v1"

    def fetch(self, season_year: int, out_dir: Path) -> list[Path]:
        api_key = os.getenv("BALDONTLIE_API_KEY")
        if not api_key:
            raise MissingCredentialError("BALDONTLIE_API_KEY is missing; connector skipped.")

        headers = {"Authorization": api_key}
        raw_paths: list[Path] = []
        next_page: int | None = 1
        max_pages = 10

        for _ in range(max_pages):
            if next_page is None:
                break
            params = {"seasons[]": season_year, "per_page": 100, "page": next_page}
            payload = self._request_json(f"{self.base_url}/games", headers=headers, params=params, retries=3)
            page_path = out_dir / f"games_page_{next_page}_{season_year}.json"
            self._write_json(page_path, payload)
            raw_paths.append(page_path)

            meta = payload.get("meta", {}) or {}
            next_page = meta.get("next_page")
            time.sleep(0.35)

        return raw_paths

    def _team_country(self, team_obj: dict[str, Any]) -> str | None:
        full_name = str(team_obj.get("full_name", "")).lower()
        if "toronto raptors" in full_name:
            return "CAN"
        if full_name:
            return "USA"
        return None

    def parse(self, raw_paths: list[Path], season_year: int) -> dict[str, pd.DataFrame]:
        timestamp = utc_now_iso()
        sport_id = slugify("Basketball")
        competition_id = stable_id(self.id, "competition", "nba", season_year)

        sports_df = pd.DataFrame(
            [
                {
                    "sport_id": sport_id,
                    "sport_name": "Basketball",
                    "sport_slug": sport_id,
                    "created_at_utc": timestamp,
                }
            ]
        )
        competitions_df = pd.DataFrame(
            [
                {
                    "competition_id": competition_id,
                    "sport_id": sport_id,
                    "name": "NBA",
                    "season_year": season_year,
                    "level": "pro_club_league",
                    "start_date": None,
                    "end_date": None,
                    "source_id": self.id,
                }
            ]
        )

        event_rows: list[dict[str, Any]] = []
        participant_rows: dict[str, dict[str, Any]] = {}
        result_rows: list[dict[str, Any]] = []

        for path in raw_paths:
            payload = json.loads(path.read_text(encoding="utf-8"))
            games = payload.get("data", []) or []
            for game in games:
                game_id = game.get("id")
                if game_id is None:
                    continue
                event_id = stable_id(self.id, "event", game_id)
                game_date = str(game.get("date") or "")[:10] or None
                event_rows.append(
                    {
                        "event_id": event_id,
                        "competition_id": competition_id,
                        "discipline_id": None,
                        "gender": "mixed",
                        "event_class": "playoff" if game.get("postseason") else "regular_season",
                        "event_date": game_date,
                    }
                )

                home = game.get("home_team", {}) or {}
                visitor = game.get("visitor_team", {}) or {}
                home_score = game.get("home_team_score")
                visitor_score = game.get("visitor_team_score")

                home_pid = stable_id(self.id, "team", home.get("id") or home.get("full_name"))
                visitor_pid = stable_id(self.id, "team", visitor.get("id") or visitor.get("full_name"))

                participant_rows[home_pid] = {
                    "participant_id": home_pid,
                    "type": "team",
                    "display_name": home.get("full_name") or "Unknown home team",
                    "country_id": self._team_country(home),
                }
                participant_rows[visitor_pid] = {
                    "participant_id": visitor_pid,
                    "type": "team",
                    "display_name": visitor.get("full_name") or "Unknown visitor team",
                    "country_id": self._team_country(visitor),
                }

                home_rank, visitor_rank = 2, 2
                home_points, visitor_points = 0.0, 0.0
                if home_score is not None and visitor_score is not None:
                    if home_score > visitor_score:
                        home_rank, visitor_rank = 1, 2
                        home_points, visitor_points = 3.0, 0.0
                    elif visitor_score > home_score:
                        home_rank, visitor_rank = 2, 1
                        home_points, visitor_points = 0.0, 3.0
                    else:
                        home_rank, visitor_rank = 1, 1
                        home_points, visitor_points = 1.0, 1.0

                result_rows.append(
                    {
                        "event_id": event_id,
                        "participant_id": home_pid,
                        "rank": home_rank,
                        "medal": "gold" if home_rank == 1 else None,
                        "score_raw": f"{home_score}",
                        "points_awarded": home_points,
                    }
                )
                result_rows.append(
                    {
                        "event_id": event_id,
                        "participant_id": visitor_pid,
                        "rank": visitor_rank,
                        "medal": "gold" if visitor_rank == 1 else None,
                        "score_raw": f"{visitor_score}",
                        "points_awarded": visitor_points,
                    }
                )

        return {
            "sports": sports_df,
            "competitions": competitions_df,
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

