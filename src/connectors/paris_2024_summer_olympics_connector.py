from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from src.core.db import SQLiteDB
from src.core.utils import slugify, utc_now_iso

from .base import Connector


KEITH_RESULTS_URL = "https://raw.githubusercontent.com/KeithGalli/Olympics-Dataset/refs/heads/master/clean-data/results.csv"
PARIS2024_MEDALS_URL = "https://raw.githubusercontent.com/taniki/paris2024-data/main/datasets/medals.csv"

DISCIPLINE_ALIASES = {
    "Cycling BMX Freestyle": "BMX Freestyle",
    "Cycling BMX Racing": "BMX Racing",
    "Cycling Mountain Bike": "Mountain Bike",
    "Cycling Road": "Road Cycling",
    "Cycling Track": "Track Cycling",
}

MEDAL_TO_RANK = {"G": 1, "S": 2, "B": 3}
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}
RANK_TO_POINTS = {1: 3.0, 2: 2.0, 3: 1.0}


class Paris2024SummerOlympicsConnector(Connector):
    id = "paris_2024_summer_olympics"
    name = "Paris 2024 Summer Olympics (Event-level medals)"
    source_type = "csv"
    license_notes = (
        "Primary source requested by user: KeithGalli Olympics Dataset (CC BY 4.0 in repository README), "
        "but this file currently stops at year 2022 and has no Paris 2024 rows. "
        "Paris 2024 event-level medals are ingested from taniki/paris2024-data (public GitHub dataset)."
    )
    base_url = KEITH_RESULTS_URL

    def source_row(self) -> dict[str, str]:
        return {
            "source_id": self.id,
            "source_name": self.name,
            "source_type": self.source_type,
            "license_notes": (
                "Primary source requested by user: KeithGalli Olympics Dataset (CC BY 4.0 in repository README), "
                "but this file currently stops at year 2022 and has no Paris 2024 rows. "
                "Paris 2024 event-level medals are ingested from taniki/paris2024-data."
            ),
            "base_url": f"{KEITH_RESULTS_URL} | {PARIS2024_MEDALS_URL}",
        }

    def _local_seed_dir(self) -> Path:
        return Path(__file__).resolve().parents[2] / "data" / "raw" / "olympics"

    @staticmethod
    def _download_csv(url: str, out_path: Path) -> Path:
        headers = {"User-Agent": "DataSportPipeline/0.1 (Paris2024 Olympics fetch)"}
        response = requests.get(url, headers=headers, timeout=120)
        response.raise_for_status()
        out_path.write_bytes(response.content)
        return out_path

    def fetch(self, season_year: int, out_dir: Path) -> list[Path]:
        if season_year != 2024:
            raise RuntimeError("This connector currently supports only Paris 2024 (use --year 2024).")

        local_seed_dir = self._local_seed_dir()
        keith_local = local_seed_dir / "keithgalli_results.csv"
        medals_local = local_seed_dir / "paris2024_medals_by_event.csv"

        raw_paths: list[Path] = []
        mode_parts: list[str] = []

        for local_path, fallback_url, out_name in [
            (keith_local, KEITH_RESULTS_URL, "keithgalli_results.csv"),
            (medals_local, PARIS2024_MEDALS_URL, "paris2024_medals_by_event.csv"),
        ]:
            target = out_dir / out_name
            if local_path.exists():
                shutil.copy2(local_path, target)
                mode_parts.append(f"local:{out_name}")
            else:
                self._download_csv(fallback_url, target)
                mode_parts.append(f"download:{out_name}")
            raw_paths.append(target)

        self._write_json(
            out_dir / "fetch_meta.json",
            {
                "mode": ", ".join(mode_parts),
                "keith_results_url": KEITH_RESULTS_URL,
                "paris2024_medals_url": PARIS2024_MEDALS_URL,
                "year": season_year,
            },
        )
        return raw_paths

    @staticmethod
    def _parse_gender(event_name: str) -> str | None:
        text = str(event_name).lower()
        if text.startswith("men") or " men " in text:
            return "men"
        if text.startswith("women") or text.startswith("women's") or " women " in text:
            return "women"
        if text.startswith("mixed") or " mixed " in text:
            return "mixed"
        return None

    @staticmethod
    def _load_seed_sport_mapping(seed_path: Path) -> dict[str, str]:
        if not seed_path.exists():
            return {}
        seed = pd.read_csv(seed_path)
        out: dict[str, str] = {}
        for row in seed.itertuples(index=False):
            sport_name = str(getattr(row, "sport_name", "")).strip()
            discipline_name = str(getattr(row, "discipline_name", "")).strip()
            if sport_name and discipline_name:
                out[discipline_name.lower()] = sport_name
        return out

    def _resolve_country_name(self, code: str) -> str:
        code = str(code).strip().upper()
        try:
            import pycountry

            country = pycountry.countries.get(alpha_3=code)
            if country and getattr(country, "name", None):
                return str(country.name)
        except Exception:
            pass
        return code

    @staticmethod
    def _build_event_id(discipline_name: str, event_name: str) -> str:
        return f"paris2024_{slugify(discipline_name)}_{slugify(event_name)}"

    @staticmethod
    def _clean_person_name_for_id(name: str) -> str:
        normalized = re.sub(r"\s+", "_", str(name).strip())
        normalized = re.sub(r"[^0-9A-Za-zÀ-ÖØ-öø-ÿ_-]", "", normalized)
        return normalized or slugify(str(name))

    def _build_participant_id(self, participant_type: str, athlete_name: str, noc: str) -> str:
        if participant_type == "team":
            return f"nation_{str(noc).upper()}"
        cleaned_name = self._clean_person_name_for_id(athlete_name)
        return f"athlete_{cleaned_name}_{str(noc).upper()}"

    def parse(self, raw_paths: list[Path], season_year: int) -> dict[str, pd.DataFrame]:
        if season_year != 2024:
            raise RuntimeError("This connector currently supports only Paris 2024.")

        keith_path = next(path for path in raw_paths if path.name == "keithgalli_results.csv")
        medals_path = next(path for path in raw_paths if path.name == "paris2024_medals_by_event.csv")
        seed_dir = self._local_seed_dir()
        sports_seed_mapping = self._load_seed_sport_mapping(seed_dir / "paris2024_sports_disciplines_seed.csv")

        keith = pd.read_csv(keith_path)
        keith["year"] = pd.to_numeric(keith["year"], errors="coerce")
        keith_summer_2024_rows = int(((keith["type"] == "Summer") & (keith["year"] == 2024)).sum())

        medals_raw = pd.read_csv(medals_path)
        medals = medals_raw.rename(columns={"code": "noc", "name": "athlete_name"}).copy()
        medals["noc"] = medals["noc"].astype(str).str.strip().str.upper()
        medals["athlete_name"] = medals["athlete_name"].astype(str).str.strip()
        medals["discipline"] = medals["discipline"].astype(str).str.strip()
        medals["event"] = medals["event"].astype(str).str.strip()
        medals["color"] = medals["color"].astype(str).str.strip().str.upper()
        medals = medals.loc[
            (medals["noc"] != "")
            & (medals["athlete_name"] != "")
            & (medals["discipline"] != "")
            & (medals["event"] != "")
            & (medals["color"].isin(["G", "S", "B"]))
        ].copy()

        medals["discipline_name"] = medals["discipline"].map(lambda value: DISCIPLINE_ALIASES.get(value, value))
        medals["sport_name"] = medals["discipline_name"].str.lower().map(sports_seed_mapping)
        medals["sport_name"] = medals["sport_name"].fillna(medals["discipline_name"])

        timestamp = utc_now_iso()
        competition_id = "summer_olympics_paris_2024"
        olympic_games_sport_id = slugify("Olympic Games")

        sports_rows: list[dict[str, Any]] = [
            {
                "sport_id": olympic_games_sport_id,
                "sport_name": "Olympic Games",
                "sport_slug": olympic_games_sport_id,
                "created_at_utc": timestamp,
            }
        ]
        for sport_name in sorted(set(medals["sport_name"])):
            sport_id = slugify(sport_name)
            sports_rows.append(
                {
                    "sport_id": sport_id,
                    "sport_name": sport_name,
                    "sport_slug": sport_id,
                    "created_at_utc": timestamp,
                }
            )

        disciplines_rows: list[dict[str, Any]] = []
        for discipline_name, sport_name in (
            medals[["discipline_name", "sport_name"]]
            .drop_duplicates()
            .sort_values(["sport_name", "discipline_name"])
            .itertuples(index=False)
        ):
            disciplines_rows.append(
                {
                    "discipline_id": slugify(str(discipline_name)),
                    "discipline_name": str(discipline_name),
                    "discipline_slug": slugify(str(discipline_name)),
                    "sport_id": slugify(str(sport_name)),
                    "confidence": 1.0,
                    "mapping_source": "connector_paris_2024_summer_olympics",
                    "created_at_utc": timestamp,
                }
            )

        competitions_df = pd.DataFrame(
            [
                {
                    "competition_id": competition_id,
                    "sport_id": olympic_games_sport_id,
                    "name": "Summer Olympics Paris 2024",
                    "season_year": 2024,
                    "level": "multi_sport_games",
                    "start_date": "2024-07-26",
                    "end_date": "2024-08-11",
                    "source_id": self.id,
                }
            ]
        )

        medal_slots = (
            medals.groupby(["discipline_name", "sport_name", "event", "color", "noc"], as_index=False)
            .agg(
                athletes_count=("athlete_name", "count"),
                representative_name=("athlete_name", "first"),
            )
            .sort_values(["discipline_name", "event", "color", "noc"])
            .reset_index(drop=True)
        )
        medal_slots["rank"] = medal_slots["color"].map(MEDAL_TO_RANK)

        event_keys = (
            medal_slots[["discipline_name", "event"]].drop_duplicates().sort_values(["discipline_name", "event"])
        )
        events_rows: list[dict[str, Any]] = []
        for row in event_keys.itertuples(index=False):
            discipline_name = str(row.discipline_name)
            event_name = str(row.event)
            event_id = self._build_event_id(discipline_name, event_name)
            events_rows.append(
                {
                    "event_id": event_id,
                    "competition_id": competition_id,
                    "discipline_id": slugify(discipline_name),
                    "gender": self._parse_gender(event_name),
                    "event_class": "olympic_medal_event",
                    "event_date": None,
                }
            )

        participants_rows: dict[str, dict[str, Any]] = {}
        countries_rows: dict[str, dict[str, Any]] = {}
        results_rows: list[dict[str, Any]] = []

        medal_slots["event_id"] = medal_slots.apply(
            lambda row: self._build_event_id(str(row["discipline_name"]), str(row["event"])),
            axis=1,
        )

        for row in medal_slots.itertuples(index=False):
            participant_type = "team" if int(row.athletes_count) > 1 else "athlete"
            participant_name = f"{row.noc} nation team" if participant_type == "team" else str(row.representative_name)
            participant_id = self._build_participant_id(participant_type, participant_name, str(row.noc))
            participants_rows[participant_id] = {
                "participant_id": participant_id,
                "type": participant_type,
                "display_name": participant_name,
                "country_id": row.noc,
            }

            if row.noc not in countries_rows:
                country_name = self._resolve_country_name(row.noc)
                country_obj = None
                try:
                    import pycountry

                    country_obj = pycountry.countries.get(alpha_3=row.noc)
                except Exception:
                    country_obj = None
                countries_rows[row.noc] = {
                    "country_id": row.noc,
                    "iso2": getattr(country_obj, "alpha_2", None) if country_obj else None,
                    "iso3": row.noc,
                    "name_en": getattr(country_obj, "name", country_name) if country_obj else country_name,
                    "name_fr": None,
                }

            rank = int(row.rank)
            results_rows.append(
                {
                    "event_id": row.event_id,
                    "participant_id": participant_id,
                    "rank": rank,
                    "medal": RANK_TO_MEDAL.get(rank),
                    "score_raw": f"color={row.color};noc={row.noc}",
                    "points_awarded": RANK_TO_POINTS.get(rank),
                }
            )

        source_audit_df = pd.DataFrame(
            [
                {
                    "source": "keithgalli_results.csv",
                    "summer_2024_rows": keith_summer_2024_rows,
                    "note": "0 means dataset currently has no Paris 2024 rows.",
                },
                {
                    "source": "paris2024_medals_by_event.csv",
                    "summer_2024_rows": len(medal_slots),
                    "note": "Event-level medal slots (event + medal color + country) used for results.",
                },
            ]
        )

        return {
            "countries": pd.DataFrame(countries_rows.values()).drop_duplicates(subset=["country_id"]),
            "sports": pd.DataFrame(sports_rows).drop_duplicates(subset=["sport_id"]),
            "disciplines": pd.DataFrame(disciplines_rows).drop_duplicates(subset=["discipline_id"]),
            "competitions": competitions_df,
            "events": pd.DataFrame(events_rows).drop_duplicates(subset=["event_id"]),
            "participants": pd.DataFrame(participants_rows.values()).drop_duplicates(subset=["participant_id"]),
            "results": pd.DataFrame(results_rows).drop_duplicates(subset=["event_id", "participant_id"]),
            "sport_federations": pd.DataFrame(),
            "source_audit": source_audit_df,
        }

    def upsert(self, db: SQLiteDB, payload: dict[str, pd.DataFrame]) -> None:
        with db.connect() as conn:
            conn.execute(
                """
                DELETE FROM results
                WHERE event_id IN (
                    SELECT e.event_id
                    FROM events e
                    JOIN competitions c ON c.competition_id = e.competition_id
                    WHERE c.source_id = ?
                )
                """,
                (self.id,),
            )
            conn.execute(
                """
                DELETE FROM events
                WHERE competition_id IN (
                    SELECT competition_id FROM competitions WHERE source_id = ?
                )
                """,
                (self.id,),
            )
            conn.execute("DELETE FROM competitions WHERE source_id = ?", (self.id,))
            conn.execute("DELETE FROM disciplines WHERE mapping_source = 'connector_paris_2024_summer_olympics'")
            conn.execute(
                """
                DELETE FROM participants
                WHERE (
                    participant_id LIKE 'paris24_%'
                    OR participant_id LIKE 'athlete_%'
                    OR participant_id LIKE 'nation_%'
                )
                  AND participant_id NOT IN (SELECT DISTINCT participant_id FROM results)
                """
            )
            conn.commit()

        db.upsert_dataframe("countries", payload.get("countries", pd.DataFrame()), ["country_id"])
        db.upsert_dataframe("sports", payload.get("sports", pd.DataFrame()), ["sport_id"])
        db.upsert_dataframe("disciplines", payload.get("disciplines", pd.DataFrame()), ["discipline_id"])
        db.upsert_dataframe("competitions", payload.get("competitions", pd.DataFrame()), ["competition_id"])
        db.upsert_dataframe("events", payload.get("events", pd.DataFrame()), ["event_id"])
        db.upsert_dataframe("participants", payload.get("participants", pd.DataFrame()), ["participant_id"])
        db.upsert_dataframe("results", payload.get("results", pd.DataFrame()), ["event_id", "participant_id"])
