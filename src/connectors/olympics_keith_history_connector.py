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
MEDAL_TEXT_TO_VALUE = {"Gold": "gold", "Silver": "silver", "Bronze": "bronze"}
MEDAL_TO_POINTS = {"gold": 3.0, "silver": 2.0, "bronze": 1.0}


class OlympicsKeithHistoryConnector(Connector):
    id = "olympics_keith_history"
    name = "Olympics Historical Results (KeithGalli)"
    source_type = "csv"
    license_notes = "KeithGalli Olympics Dataset (repository indicates CC BY 4.0); check redistribution requirements."
    base_url = KEITH_RESULTS_URL

    def _local_seed_path(self) -> Path:
        return Path(__file__).resolve().parents[2] / "data" / "raw" / "olympics" / "keithgalli_results.csv"

    @staticmethod
    def _download_csv(url: str, out_path: Path) -> Path:
        headers = {"User-Agent": "DataSportPipeline/0.1 (Olympics history fetch)"}
        response = requests.get(url, headers=headers, timeout=180)
        response.raise_for_status()
        out_path.write_bytes(response.content)
        return out_path

    def fetch(self, season_year: int, out_dir: Path) -> list[Path]:
        del season_year
        out_file = out_dir / "keithgalli_results.csv"
        local_seed = self._local_seed_path()
        if local_seed.exists():
            shutil.copy2(local_seed, out_file)
            mode = "local_seed"
        else:
            self._download_csv(self.base_url, out_file)
            mode = "download"

        self._write_json(
            out_dir / "fetch_meta.json",
            {
                "mode": mode,
                "source": self.base_url,
            },
        )
        return [out_file]

    @staticmethod
    def _parse_gender(event_name: str) -> str | None:
        value = str(event_name).lower()
        if value.startswith("men") or " men " in value:
            return "men"
        if value.startswith("women") or value.startswith("women's") or " women " in value:
            return "women"
        if value.startswith("mixed") or " mixed " in value:
            return "mixed"
        return None

    @staticmethod
    def _clean_person_name_for_id(name: str) -> str:
        normalized = re.sub(r"\s+", "_", str(name).strip())
        normalized = re.sub(r"[^0-9A-Za-zÀ-ÖØ-öø-ÿ_-]", "", normalized)
        return normalized or slugify(str(name))

    @staticmethod
    def _event_id(olympic_type: str, year: int, discipline: str, event: str) -> str:
        return f"olympics_{olympic_type}_{year}_{slugify(discipline)}_{slugify(event)}"

    @staticmethod
    def _competition_id(olympic_type: str, year: int) -> str:
        return f"olympics_{olympic_type}_{year}"

    @staticmethod
    def _infer_sport_name(discipline_name: str, mapping: dict[str, str]) -> str:
        key = str(discipline_name).strip().lower()
        if key in mapping:
            return mapping[key]
        if key.startswith("cycling "):
            return "Cycling"
        if key.startswith("wrestling "):
            return "Wrestling"
        if key.startswith("equestrian "):
            return "Equestrian"
        if key.startswith("canoe "):
            return "Canoe"
        if "skating" in key:
            return "Skating"
        if "ski" in key:
            return "Skiing"
        if "ice hockey" in key:
            return "Ice Hockey"
        return str(discipline_name).strip()

    def _load_sport_mapping(self) -> dict[str, str]:
        seed_path = Path(__file__).resolve().parents[2] / "data" / "raw" / "olympics" / "paris2024_sports_disciplines_seed.csv"
        if not seed_path.exists():
            return {}
        seed = pd.read_csv(seed_path)
        mapping: dict[str, str] = {}
        for row in seed.itertuples(index=False):
            sport_name = str(getattr(row, "sport_name", "")).strip()
            discipline_name = str(getattr(row, "discipline_name", "")).strip()
            if sport_name and discipline_name:
                mapping[discipline_name.lower()] = sport_name
        return mapping

    @staticmethod
    def _normalize_medal(value: Any) -> str | None:
        text = str(value).strip()
        if not text or text == "nan":
            return None
        return MEDAL_TEXT_TO_VALUE.get(text, None)

    @staticmethod
    def _normalize_rank(value: Any) -> int | None:
        numeric = pd.to_numeric(value, errors="coerce")
        if pd.isna(numeric):
            return None
        rank = int(numeric)
        if rank < 1:
            return None
        return rank

    def parse(self, raw_paths: list[Path], season_year: int) -> dict[str, pd.DataFrame]:
        csv_path = next(path for path in raw_paths if path.name.endswith(".csv"))
        frame = pd.read_csv(csv_path)
        required = {"year", "type", "discipline", "event", "as", "athlete_id", "noc", "place", "tied", "medal"}
        missing = required - set(frame.columns)
        if missing:
            raise RuntimeError(f"Missing required columns in Keith dataset: {sorted(missing)}")

        frame = frame.copy()
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce")
        frame = frame.loc[frame["year"].notna()].copy()
        frame["year"] = frame["year"].astype(int)
        frame["type"] = frame["type"].astype(str).str.strip()
        frame = frame.loc[(frame["year"] >= season_year) & (frame["type"].isin(["Summer", "Winter"]))].copy()
        frame["discipline"] = frame["discipline"].fillna("").astype(str).str.strip()
        frame["event"] = frame["event"].fillna("").astype(str).str.strip()
        frame["athlete_name"] = frame["as"].fillna("").astype(str).str.strip()
        frame["noc"] = frame["noc"].fillna("").astype(str).str.strip().str.upper()
        frame["olympic_type"] = frame["type"].str.lower()
        frame = frame.loc[
            (frame["discipline"] != "")
            & (frame["discipline"].str.lower() != "nan")
            & (frame["event"] != "")
            & (frame["event"].str.lower() != "nan")
            & (frame["noc"] != "")
            & (frame["noc"].str.lower() != "nan")
        ].copy()

        sport_mapping = self._load_sport_mapping()
        frame["sport_name"] = frame["discipline"].map(lambda value: self._infer_sport_name(value, sport_mapping))
        frame["event_id"] = frame.apply(
            lambda row: self._event_id(row["olympic_type"], int(row["year"]), row["discipline"], row["event"]),
            axis=1,
        )
        frame["competition_id"] = frame.apply(
            lambda row: self._competition_id(row["olympic_type"], int(row["year"])),
            axis=1,
        )
        frame["medal_norm"] = frame["medal"].map(self._normalize_medal)
        frame["rank_norm"] = frame["place"].map(self._normalize_rank)
        frame["points_awarded"] = frame["medal_norm"].map(MEDAL_TO_POINTS)
        medals_frame = frame.loc[frame["medal_norm"].notna()].copy()

        timestamp = utc_now_iso()
        olympic_games_sport_id = slugify("Olympic Games")

        sports_rows: list[dict[str, Any]] = [
            {
                "sport_id": olympic_games_sport_id,
                "sport_name": "Olympic Games",
                "sport_slug": olympic_games_sport_id,
                "created_at_utc": timestamp,
            }
        ]
        for sport_name in sorted(medals_frame["sport_name"].dropna().unique()):
            sport_id = slugify(str(sport_name))
            sports_rows.append(
                {
                    "sport_id": sport_id,
                    "sport_name": str(sport_name),
                    "sport_slug": sport_id,
                    "created_at_utc": timestamp,
                }
            )

        disciplines_rows: list[dict[str, Any]] = []
        for discipline_name, sport_name in (
            medals_frame[["discipline", "sport_name"]]
            .drop_duplicates()
            .sort_values(["sport_name", "discipline"])
            .itertuples(index=False)
        ):
            disciplines_rows.append(
                {
                    "discipline_id": slugify(str(discipline_name)),
                    "discipline_name": str(discipline_name),
                    "discipline_slug": slugify(str(discipline_name)),
                    "sport_id": slugify(str(sport_name)),
                    "confidence": 0.95,
                    "mapping_source": "connector_olympics_keith_history",
                    "created_at_utc": timestamp,
                }
            )

        competitions_rows: list[dict[str, Any]] = []
        for olympic_type, year in (
            medals_frame[["olympic_type", "year"]]
            .drop_duplicates()
            .sort_values(["year", "olympic_type"])
            .itertuples(index=False)
        ):
            competitions_rows.append(
                {
                    "competition_id": self._competition_id(str(olympic_type), int(year)),
                    "sport_id": olympic_games_sport_id,
                    "name": f"{str(olympic_type).capitalize()} Olympics {int(year)}",
                    "season_year": int(year),
                    "level": "multi_sport_games",
                    "start_date": f"{int(year)}-01-01",
                    "end_date": f"{int(year)}-12-31",
                    "source_id": self.id,
                }
            )

        events_rows: list[dict[str, Any]] = []
        for olympic_type, year, discipline_name, event_name, event_id in (
            medals_frame[["olympic_type", "year", "discipline", "event", "event_id"]]
            .drop_duplicates()
            .sort_values(["year", "olympic_type", "discipline", "event"])
            .itertuples(index=False)
        ):
            events_rows.append(
                {
                    "event_id": str(event_id),
                    "competition_id": self._competition_id(str(olympic_type), int(year)),
                    "discipline_id": slugify(str(discipline_name)),
                    "gender": self._parse_gender(str(event_name)),
                    "event_class": "olympic_event",
                    "event_date": None,
                }
            )

        participants_rows: dict[str, dict[str, Any]] = {}
        countries_rows: dict[str, dict[str, Any]] = {}
        results_rows: list[dict[str, Any]] = []

        for row in medals_frame.itertuples(index=False):
            athlete_name = str(getattr(row, "athlete_name", "")).strip()
            noc = str(getattr(row, "noc")).strip().upper()
            athlete_id_raw = str(getattr(row, "athlete_id", "")).strip()
            athlete_id_clean = ""
            if athlete_id_raw and athlete_id_raw.lower() != "nan":
                try:
                    athlete_id_clean = str(int(float(athlete_id_raw)))
                except Exception:
                    athlete_id_clean = slugify(athlete_id_raw)

            if athlete_name and athlete_name.lower() != "nan":
                participant_type = "athlete"
                cleaned_name = self._clean_person_name_for_id(athlete_name)
                suffix = f"_{athlete_id_clean}" if athlete_id_clean else ""
                participant_id = f"athlete_{cleaned_name}_{noc}{suffix}"
                display_name = athlete_name
            else:
                participant_type = "team"
                participant_id = f"nation_{noc}"
                display_name = f"{noc} nation team"

            participants_rows[participant_id] = {
                "participant_id": participant_id,
                "type": participant_type,
                "display_name": display_name,
                "country_id": noc,
            }

            if noc not in countries_rows:
                country_obj = None
                try:
                    import pycountry

                    country_obj = pycountry.countries.get(alpha_3=noc)
                except Exception:
                    country_obj = None
                countries_rows[noc] = {
                    "country_id": noc,
                    "iso2": getattr(country_obj, "alpha_2", None) if country_obj else None,
                    "iso3": noc,
                    "name_en": getattr(country_obj, "name", noc) if country_obj else noc,
                    "name_fr": None,
                }

            rank = getattr(row, "rank_norm")
            medal = getattr(row, "medal_norm")
            tied_raw = getattr(row, "tied")
            tied_text = str(tied_raw).strip() if pd.notna(tied_raw) else ""
            results_rows.append(
                {
                    "event_id": str(getattr(row, "event_id")),
                    "participant_id": participant_id,
                    "rank": int(rank) if pd.notna(rank) and rank is not None else None,
                    "medal": medal,
                    "score_raw": f"place={getattr(row, 'place')};tied={tied_text};medal={getattr(row, 'medal')}",
                    "points_awarded": getattr(row, "points_awarded"),
                }
            )

        results_df = pd.DataFrame(results_rows)
        if not results_df.empty:
            rank_sort = pd.to_numeric(results_df["rank"], errors="coerce").fillna(10**9)
            results_df = (
                results_df.assign(_rank_sort=rank_sort)
                .sort_values(["event_id", "participant_id", "_rank_sort"])
                .drop_duplicates(subset=["event_id", "participant_id"], keep="first")
                .drop(columns=["_rank_sort"])
            )

        source_audit_df = pd.DataFrame(
            [
                {
                    "source": "keithgalli_results.csv",
                    "rows_total": len(frame),
                    "rows_medals_only": len(medals_frame),
                    "years_min": int(frame["year"].min()) if not frame.empty else None,
                    "years_max": int(frame["year"].max()) if not frame.empty else None,
                    "start_year_filter": season_year,
                }
            ]
        )

        return {
            "countries": pd.DataFrame(countries_rows.values()).drop_duplicates(subset=["country_id"]),
            "sports": pd.DataFrame(sports_rows).drop_duplicates(subset=["sport_id"]),
            "disciplines": pd.DataFrame(disciplines_rows).drop_duplicates(subset=["discipline_id"]),
            "competitions": pd.DataFrame(competitions_rows).drop_duplicates(subset=["competition_id"]),
            "events": pd.DataFrame(events_rows).drop_duplicates(subset=["event_id"]),
            "participants": pd.DataFrame(participants_rows.values()).drop_duplicates(subset=["participant_id"]),
            "results": results_df,
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
            conn.execute(
                """
                DELETE FROM participants
                WHERE (
                    participant_id LIKE 'athlete_%'
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
