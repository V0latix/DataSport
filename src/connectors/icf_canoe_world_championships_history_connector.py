from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Any

import pandas as pd

from src.core.db import SQLiteDB
from src.core.utils import slugify, utc_now_iso

from .base import Connector


SEED_FILE = "icf_canoe_world_championships_top3_seed.csv"
RANK_TO_POINTS = {1: 10.0, 2: 7.0, 3: 5.0}
ALLOWED_PROFILES = {(1, 2, 3), (1, 2, 3, 3), (1, 2), (1, 1, 3)}
COMPETITIONS = {
    "icf_canoe_sprint_world_championships": {
        "name": "ICF Canoe Sprint World Championships",
        "discipline_id": "canoe-sprint",
        "discipline_name": "Canoe Sprint",
    },
    "icf_canoe_slalom_world_championships": {
        "name": "ICF Canoe Slalom World Championships",
        "discipline_id": "canoe-slalom",
        "discipline_name": "Canoe Slalom",
    },
}


class IcfCanoeWorldChampionshipsHistoryConnector(Connector):
    id = "icf_canoe_world_championships_history"
    name = "ICF Canoe Sprint and Slalom World Championships Historical Podiums (2001+)"
    source_type = "csv"
    license_notes = (
        "Historical podium seed curated from public Wikipedia ICF canoe sprint/slalom world championship pages. "
        "Verify downstream redistribution requirements."
    )
    base_url = "https://en.wikipedia.org/wiki/ICF_Canoe_Sprint_World_Championships"

    def source_row(self) -> dict[str, str]:
        return {
            "source_id": self.id,
            "source_name": self.name,
            "source_type": self.source_type,
            "license_notes": (
                "Local seed data/raw/canoe/icf_canoe_world_championships_top3_seed.csv built from "
                "Wikipedia annual ICF Canoe Sprint and Canoe Slalom World Championships medal tables; "
                "senior post-2000 scope only; Paracanoe excluded."
            ),
            "base_url": self.base_url,
        }

    def _local_seed_path(self) -> Path:
        return Path(__file__).resolve().parents[2] / "data" / "raw" / "canoe" / SEED_FILE

    @staticmethod
    def _clean_person_name_for_id(name: str) -> str:
        normalized = re.sub(r"\s+", "_", str(name).strip())
        normalized = re.sub(r"[^0-9A-Za-zÀ-ÖØ-öø-ÿ_-]", "", normalized)
        return normalized or slugify(str(name))

    @staticmethod
    def _name_signatures(name: str) -> set[str]:
        cleaned = re.sub(r"[^0-9A-Za-zÀ-ÖØ-öø-ÿ]+", " ", str(name).upper()).strip()
        tokens = [token for token in cleaned.split() if token]
        if not tokens:
            return set()
        signatures = {" ".join(tokens)}
        if len(tokens) >= 2:
            signatures.add(" ".join(reversed(tokens)))
        return signatures

    @staticmethod
    def _event_id(competition_id: str, year: int, event_key: str) -> str:
        suffix = str(event_key).replace("world_championships_", "").strip("_")
        return f"{competition_id}_{year}_{suffix}"

    def fetch(self, season_year: int, out_dir: Path) -> list[Path]:
        local_seed = self._local_seed_path()
        if not local_seed.exists():
            raise RuntimeError(f"Missing local seed for ICF Canoe World Championships: {local_seed}")

        out_file = out_dir / SEED_FILE
        shutil.copy2(local_seed, out_file)

        frame = pd.read_csv(local_seed)
        frame["year"] = pd.to_numeric(frame.get("year", pd.Series(dtype=float)), errors="coerce")
        years_by_competition = {
            competition_id: sorted(
                group["year"].dropna().astype(int).loc[lambda s: (s > 2000) & (s <= season_year)].unique().tolist()
            )
            for competition_id, group in frame.groupby("competition_id")
        }
        self._write_json(
            out_dir / "fetch_meta.json",
            {
                "mode": "local_seed",
                "seed_file": str(local_seed),
                "years_by_competition": years_by_competition,
                "missing_years_post_2000": {
                    competition_id: sorted(set(range(2001, season_year + 1)) - set(years))
                    for competition_id, years in years_by_competition.items()
                },
                "notes": "Olympic-year senior sprint/slalom worlds are absent when the seed has no annual page; Paracanoe excluded.",
            },
        )
        return [out_file]

    def parse(self, raw_paths: list[Path], season_year: int) -> dict[str, pd.DataFrame]:
        seed_path = next((path for path in raw_paths if path.name == SEED_FILE), None)
        if seed_path is None:
            raise RuntimeError(f"Missing {SEED_FILE} in fetched paths.")

        frame = pd.read_csv(seed_path)
        required_cols = {
            "competition_id",
            "competition_name",
            "year",
            "event_date",
            "discipline_id",
            "discipline_name",
            "event_key",
            "event_name",
            "gender",
            "rank",
            "medal",
            "participant_type",
            "participant_name",
            "country_name",
            "country_code",
            "score_raw",
            "source_url",
        }
        if not required_cols.issubset(set(frame.columns)):
            raise RuntimeError(f"Unsupported ICF canoe seed format: {list(frame.columns)}")

        frame = frame.copy()
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce")
        frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce")
        frame["event_date"] = pd.to_datetime(frame["event_date"], errors="coerce")
        for column in required_cols - {"year", "rank", "event_date"}:
            frame[column] = frame[column].fillna("").astype(str).str.strip()
        frame["gender"] = frame["gender"].str.lower()
        frame["medal"] = frame["medal"].str.lower()
        frame["participant_type"] = frame["participant_type"].str.lower()
        frame["country_code"] = frame["country_code"].str.upper()

        frame = frame.dropna(subset=["year", "rank", "event_date"])
        frame["year"] = frame["year"].astype(int)
        frame["rank"] = frame["rank"].astype(int)
        frame["event_date"] = frame["event_date"].dt.strftime("%Y-%m-%d")
        frame = frame.loc[(frame["year"] > 2000) & (frame["year"] <= int(season_year))].copy()
        frame = frame.loc[
            frame["competition_id"].isin(COMPETITIONS.keys())
            & frame["discipline_id"].isin({"canoe-sprint", "canoe-slalom"})
            & frame["gender"].isin({"men", "women", "mixed"})
            & frame["rank"].isin({1, 2, 3})
            & frame["participant_type"].isin({"athlete", "team"})
            & (frame["event_key"] != "")
            & (frame["event_name"] != "")
            & (frame["participant_name"] != "")
            & (frame["country_code"] != "")
        ].copy()
        frame = frame.drop_duplicates(
            subset=["competition_id", "year", "event_key", "rank", "participant_name", "country_code"]
        ).sort_values(["competition_id", "year", "event_key", "rank", "country_code"]).reset_index(drop=True)

        if frame.empty:
            raise RuntimeError(f"No ICF Canoe World Championships rows available for year <= {season_year} and > 2000.")
        if (frame["year"] <= 2000).any():
            raise RuntimeError("ICF canoe parse leaked rows outside post-2000 scope.")

        profiles = (
            frame.groupby(["competition_id", "year", "event_key"])["rank"]
            .apply(lambda series: tuple(sorted(series.tolist())))
            .to_dict()
        )
        invalid_profiles = {
            f"{competition_id}_{year}_{event_key}": profile
            for (competition_id, year, event_key), profile in profiles.items()
            if profile not in ALLOWED_PROFILES
        }
        if invalid_profiles:
            sample = dict(list(invalid_profiles.items())[:40])
            raise RuntimeError(f"Unexpected ICF canoe rank profiles: {sample}")

        timestamp = utc_now_iso()
        sports_df = pd.DataFrame(
            [
                {
                    "sport_id": "canoe",
                    "sport_name": "Canoe",
                    "sport_slug": "canoe",
                    "created_at_utc": timestamp,
                }
            ]
        )
        disciplines_df = pd.DataFrame(
            [
                {
                    "discipline_id": "canoe-sprint",
                    "discipline_name": "Canoe Sprint",
                    "discipline_slug": "canoe-sprint",
                    "sport_id": "canoe",
                    "confidence": 1.0,
                    "mapping_source": f"connector_{self.id}",
                    "created_at_utc": timestamp,
                },
                {
                    "discipline_id": "canoe-slalom",
                    "discipline_name": "Canoe Slalom",
                    "discipline_slug": "canoe-slalom",
                    "sport_id": "canoe",
                    "confidence": 1.0,
                    "mapping_source": f"connector_{self.id}",
                    "created_at_utc": timestamp,
                },
            ]
        )
        competitions_rows = []
        for competition_id, config in COMPETITIONS.items():
            subset = frame.loc[frame["competition_id"] == competition_id]
            if subset.empty:
                continue
            competitions_rows.append(
                {
                    "competition_id": competition_id,
                    "sport_id": "canoe",
                    "name": config["name"],
                    "season_year": None,
                    "level": "international_championship",
                    "start_date": subset["event_date"].min(),
                    "end_date": subset["event_date"].max(),
                    "source_id": self.id,
                }
            )

        events_rows: list[dict[str, Any]] = []
        participants_rows: dict[str, dict[str, Any]] = {}
        countries_rows: dict[str, dict[str, Any]] = {}
        results_rows: list[dict[str, Any]] = []

        for row in (
            frame[
                [
                    "competition_id",
                    "year",
                    "event_key",
                    "event_name",
                    "discipline_id",
                    "gender",
                    "event_date",
                ]
            ]
            .drop_duplicates()
            .sort_values(["competition_id", "year", "event_key"])
            .itertuples(index=False)
        ):
            events_rows.append(
                {
                    "event_id": self._event_id(str(row.competition_id), int(row.year), str(row.event_key)),
                    "competition_id": str(row.competition_id),
                    "discipline_id": str(row.discipline_id),
                    "gender": str(row.gender),
                    "event_class": "final_ranking_top3",
                    "event_date": str(row.event_date),
                }
            )

        for row in frame.itertuples(index=False):
            country_code = str(row.country_code).upper()
            country_name = str(row.country_name).strip() or country_code
            if country_code not in countries_rows:
                country_obj = None
                try:
                    import pycountry

                    country_obj = pycountry.countries.get(alpha_3=country_code)
                except Exception:
                    country_obj = None
                countries_rows[country_code] = {
                    "country_id": country_code,
                    "iso2": getattr(country_obj, "alpha_2", None) if country_obj else None,
                    "iso3": country_code,
                    "name_en": getattr(country_obj, "name", country_name) if country_obj else country_name,
                    "name_fr": None,
                }

            participant_type = str(row.participant_type).lower()
            participant_name = str(row.participant_name).strip()
            if participant_type == "team":
                participant_id = f"team_{self._clean_person_name_for_id(participant_name)}_{country_code}"
                display_name = participant_name
            else:
                participant_id = f"athlete_{self._clean_person_name_for_id(participant_name)}_{country_code}"
                display_name = participant_name
            participants_rows[participant_id] = {
                "participant_id": participant_id,
                "type": participant_type,
                "display_name": display_name,
                "country_id": country_code,
            }
            rank = int(row.rank)
            results_rows.append(
                {
                    "event_id": self._event_id(str(row.competition_id), int(row.year), str(row.event_key)),
                    "participant_id": participant_id,
                    "rank": rank,
                    "medal": str(row.medal),
                    "score_raw": (
                        f"event_name={row.event_name};athletes={participant_name};country={country_code};"
                        f"score={row.score_raw};source_url={row.source_url}"
                    ),
                    "points_awarded": RANK_TO_POINTS.get(rank),
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

        return {
            "countries": pd.DataFrame(countries_rows.values()).drop_duplicates(subset=["country_id"]),
            "sports": sports_df.drop_duplicates(subset=["sport_id"]),
            "disciplines": disciplines_df.drop_duplicates(subset=["discipline_id"]),
            "competitions": pd.DataFrame(competitions_rows).drop_duplicates(subset=["competition_id"]),
            "events": pd.DataFrame(events_rows).drop_duplicates(subset=["event_id"]),
            "participants": pd.DataFrame(participants_rows.values()).drop_duplicates(subset=["participant_id"]),
            "results": results_df,
            "sport_federations": pd.DataFrame(),
        }

    def _reuse_existing_athletes(
        self,
        db: SQLiteDB,
        participants_df: pd.DataFrame,
        results_df: pd.DataFrame,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        if participants_df.empty or results_df.empty:
            return participants_df, results_df

        incoming_athletes = participants_df.loc[participants_df["type"] == "athlete"].copy()
        if incoming_athletes.empty:
            return participants_df, results_df

        with db.connect() as conn:
            existing = pd.read_sql_query(
                "SELECT participant_id, country_id, display_name, type FROM participants WHERE type = 'athlete'",
                conn,
            )
        if existing.empty:
            return participants_df, results_df

        lookup: dict[tuple[str, str], str] = {}
        for row in existing.itertuples(index=False):
            country_id = str(getattr(row, "country_id", "")).upper().strip()
            participant_id = str(getattr(row, "participant_id"))
            display_name = str(getattr(row, "display_name", ""))
            for signature in self._name_signatures(display_name):
                lookup.setdefault((country_id, signature), participant_id)

        replacement: dict[str, str] = {}
        for row in incoming_athletes.itertuples(index=False):
            incoming_pid = str(getattr(row, "participant_id"))
            country_id = str(getattr(row, "country_id", "")).upper().strip()
            display_name = str(getattr(row, "display_name", ""))
            existing_pid = None
            for signature in self._name_signatures(display_name):
                existing_pid = lookup.get((country_id, signature))
                if existing_pid:
                    break
            if existing_pid and existing_pid != incoming_pid:
                replacement[incoming_pid] = existing_pid

        if not replacement:
            return participants_df, results_df

        remapped_results = results_df.copy()
        remapped_results["participant_id"] = remapped_results["participant_id"].map(
            lambda pid: replacement.get(pid, pid)
        )
        rank_sort = pd.to_numeric(remapped_results["rank"], errors="coerce").fillna(10**9)
        remapped_results = (
            remapped_results.assign(_rank_sort=rank_sort)
            .sort_values(["event_id", "participant_id", "_rank_sort"])
            .drop_duplicates(subset=["event_id", "participant_id"], keep="first")
            .drop(columns=["_rank_sort"])
        )
        filtered_participants = participants_df.loc[
            ~participants_df["participant_id"].isin(replacement.keys())
        ].copy()
        return filtered_participants, remapped_results

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
            conn.commit()

        participants_df = payload.get("participants", pd.DataFrame()).copy()
        results_df = payload.get("results", pd.DataFrame()).copy()
        participants_df, results_df = self._reuse_existing_athletes(db, participants_df, results_df)
        payload = {**payload, "participants": participants_df, "results": results_df}

        db.upsert_dataframe("countries", payload.get("countries", pd.DataFrame()), ["country_id"])
        db.upsert_dataframe("sports", payload.get("sports", pd.DataFrame()), ["sport_id"])
        db.upsert_dataframe("disciplines", payload.get("disciplines", pd.DataFrame()), ["discipline_id"])
        db.upsert_dataframe("competitions", payload.get("competitions", pd.DataFrame()), ["competition_id"])
        db.upsert_dataframe("events", payload.get("events", pd.DataFrame()), ["event_id"])
        db.upsert_dataframe("participants", payload.get("participants", pd.DataFrame()), ["participant_id"])
        db.upsert_dataframe("results", payload.get("results", pd.DataFrame()), ["event_id", "participant_id"])
