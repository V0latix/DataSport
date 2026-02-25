from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Any

import pandas as pd

from src.core.db import SQLiteDB
from src.core.utils import slugify, utc_now_iso

from .base import Connector


SEED_FILE = "world_aquatics_championships_top3_seed.csv"
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}
RANK_TO_POINTS = {1: 3.0, 2: 2.0, 3: 1.0}


class WorldAquaticsChampionshipsHistoryConnector(Connector):
    id = "world_aquatics_championships_history"
    name = "World Aquatics Championships Historical Podiums (Top 3 by Event)"
    source_type = "csv"
    license_notes = (
        "Historical podium seed curated from public World Aquatics Championships discipline pages "
        "(Wikipedia). Verify downstream redistribution requirements."
    )
    base_url = "https://en.wikipedia.org/wiki/World_Aquatics_Championships"

    def source_row(self) -> dict[str, str]:
        return {
            "source_id": self.id,
            "source_name": self.name,
            "source_type": self.source_type,
            "license_notes": (
                "Local seed data/raw/aquatics/world_aquatics_championships_top3_seed.csv "
                "curated from public discipline medal summary pages (editions >= 2000, currently 2001-2025). "
                "Top 3 retained per event."
            ),
            "base_url": self.base_url,
        }

    def _local_seed_path(self) -> Path:
        return Path(__file__).resolve().parents[2] / "data" / "raw" / "aquatics" / SEED_FILE

    def fetch(self, season_year: int, out_dir: Path) -> list[Path]:
        del season_year
        local_seed = self._local_seed_path()
        if not local_seed.exists():
            raise RuntimeError(f"Missing local seed for world aquatics championships history: {local_seed}")

        out_file = out_dir / SEED_FILE
        shutil.copy2(local_seed, out_file)
        self._write_json(
            out_dir / "fetch_meta.json",
            {
                "mode": "local_seed",
                "seed_file": str(local_seed),
            },
        )
        return [out_file]

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
    def _discipline_id(discipline_name: str) -> str:
        return f"aquatics_{slugify(discipline_name)}"

    @staticmethod
    def _event_id(year: int, gender: str, discipline_name: str) -> str:
        return f"world_aquatics_championships_{year}_{slugify(gender)}_{slugify(discipline_name)}"

    @staticmethod
    def _normalize_base_discipline_name(name: str) -> str:
        value = str(name).strip().lower()
        value = re.sub(r"\s+", " ", value)
        aliases = {
            "synchronised swimming": "artistic swimming",
            "synchronized swimming": "artistic swimming",
        }
        return aliases.get(value, value)

    @staticmethod
    def _strip_gender_prefix(event_name: str) -> str:
        value = str(event_name).strip()
        value = re.sub(r"^(men|women|mixed)(?:'s)?\s+", "", value, flags=re.I)
        value = re.sub(r"^(men|women|mixed)(?:'s)?$", "", value, flags=re.I)
        value = value.strip(" -")
        return value

    @staticmethod
    def _canonical_event_core(base_discipline: str, event_name: str) -> str:
        core = WorldAquaticsChampionshipsHistoryConnector._strip_gender_prefix(event_name)
        core = core.replace("×", "x")
        core = core.lower()
        core = re.sub(r"details?$", "", core).strip()
        core = core.replace("metres", "m").replace("metre", "m")
        core = core.replace("kilometres", "km").replace("kilometre", "km")
        core = core.replace("synchronized", "synchro").replace("synchronised", "synchro")
        core = re.sub(r"\bi\s*m\b", "individual medley", core)
        core = re.sub(r"\bi\.?\s*m\.?\b", "individual medley", core)
        core = re.sub(r"\b(\d{2,4})\s+(freestyle|backstroke|breaststroke|butterfly|individual medley)\b", r"\1 m \2", core)
        core = re.sub(r"\b(\d+)\s*m\s*free\b", r"\1 m freestyle", core)
        core = re.sub(r"\b(\d+)\s*m\s*back\b", r"\1 m backstroke", core)
        core = re.sub(r"\b(\d+)\s*m\s*breast\b", r"\1 m breaststroke", core)
        core = re.sub(r"\b(\d+)\s*m\s*fly\b", r"\1 m butterfly", core)
        core = re.sub(r"\b(\d+)\s+free\b", r"\1 m freestyle", core)
        core = re.sub(r"\b(\d+)\s+back\b", r"\1 m backstroke", core)
        core = re.sub(r"\b(\d+)\s+breast\b", r"\1 m breaststroke", core)
        core = re.sub(r"\b(\d+)\s+fly\b", r"\1 m butterfly", core)
        core = re.sub(r"\b4\s*x\s*(\d+)\s*m\s*free relay\b", r"4 x \1 m freestyle relay", core)
        core = re.sub(r"\b4\s*x\s*(\d+)\s*free relay\b", r"4 x \1 m freestyle relay", core)
        core = re.sub(r"\s*details?\s*$", "", core).strip()
        core = re.sub(r"\s+", " ", core).strip()

        if base_discipline == "water polo":
            return "tournament"

        if base_discipline == "diving":
            if "10 m platform" in core and "synchro" in core:
                return "10 m platform synchro"
            if "3 m springboard" in core and "synchro" in core:
                return "3 m springboard synchro"
            if core in {"mixed team", "team"}:
                return "team"

        if not core:
            return "tournament"
        return core

    @classmethod
    def _discipline_name_from_row(cls, discipline_name: str, event_name: str) -> str:
        base = cls._normalize_base_discipline_name(discipline_name)
        event_core = cls._canonical_event_core(base, event_name)
        if not base or not event_core:
            return ""
        return f"{slugify(base)}-{slugify(event_core)}"

    def parse(self, raw_paths: list[Path], season_year: int) -> dict[str, pd.DataFrame]:
        seed_path = next((path for path in raw_paths if path.name == SEED_FILE), None)
        if seed_path is None:
            raise RuntimeError(f"Missing {SEED_FILE} in fetched paths.")

        frame = pd.read_csv(seed_path)
        required_cols = {
            "year",
            "event_date",
            "gender",
            "discipline_name",
            "event_name",
            "rank",
            "medal",
            "participant_type",
            "athlete_name",
            "country_name",
            "country_code",
            "performance",
        }
        if not required_cols.issubset(set(frame.columns)):
            raise RuntimeError(
                f"Unsupported World Aquatics seed format for {seed_path.name}: {list(frame.columns)}"
            )

        frame = frame.copy()
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce")
        frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce")
        frame["event_date"] = pd.to_datetime(frame["event_date"], errors="coerce")
        frame["gender"] = frame["gender"].fillna("").astype(str).str.strip().str.lower()
        frame["discipline_name"] = frame["discipline_name"].fillna("").astype(str).str.strip()
        frame["event_name"] = frame["event_name"].fillna("").astype(str).str.strip()
        frame["medal"] = frame["medal"].fillna("").astype(str).str.strip().str.lower()
        frame["participant_type"] = frame["participant_type"].fillna("").astype(str).str.strip().str.lower()
        frame["athlete_name"] = frame["athlete_name"].fillna("").astype(str).str.strip()
        frame["country_name"] = frame["country_name"].fillna("").astype(str).str.strip()
        frame["country_code"] = frame["country_code"].fillna("").astype(str).str.strip().str.upper()
        frame["performance"] = frame["performance"].fillna("").astype(str).str.strip()
        frame["discipline_name_norm"] = frame.apply(
            lambda row: self._discipline_name_from_row(row["discipline_name"], row["event_name"]),
            axis=1,
        )

        frame = frame.dropna(subset=["year", "rank", "event_date"])
        frame["year"] = frame["year"].astype(int)
        frame["rank"] = frame["rank"].astype(int)
        frame["event_date"] = frame["event_date"].dt.strftime("%Y-%m-%d")
        frame = frame.loc[(frame["year"] <= season_year) & frame["rank"].between(1, 3)].copy()
        frame = frame.loc[
            frame["gender"].isin(["men", "women", "mixed"])
            & frame["participant_type"].isin(["athlete", "team"])
            & (frame["discipline_name"] != "")
            & (frame["event_name"] != "")
            & (frame["discipline_name_norm"] != "")
            & (frame["country_code"] != "")
        ].copy()
        frame = frame.loc[~((frame["participant_type"] == "athlete") & (frame["athlete_name"] == ""))].copy()
        frame["medal"] = frame.apply(
            lambda row: row["medal"] if row["medal"] in {"gold", "silver", "bronze"} else RANK_TO_MEDAL[row["rank"]],
            axis=1,
        )
        frame = frame.drop_duplicates(
            subset=[
                "year",
                "discipline_name_norm",
                "gender",
                "rank",
                "participant_type",
                "athlete_name",
                "country_code",
            ]
        )
        frame = frame.sort_values(["year", "discipline_name_norm", "gender", "rank", "country_code"]).reset_index(
            drop=True
        )

        if frame.empty:
            raise RuntimeError(f"No world aquatics rows available for year <= {season_year}.")

        timestamp = utc_now_iso()
        sport_id = slugify("Aquatics")

        sports_df = pd.DataFrame(
            [
                {
                    "sport_id": sport_id,
                    "sport_name": "Aquatics",
                    "sport_slug": sport_id,
                    "created_at_utc": timestamp,
                }
            ]
        )

        disciplines_rows: list[dict[str, Any]] = []
        for discipline_name in sorted(frame["discipline_name_norm"].unique()):
            discipline_id = self._discipline_id(discipline_name)
            disciplines_rows.append(
                {
                    "discipline_id": discipline_id,
                    "discipline_name": discipline_name,
                    "discipline_slug": slugify(discipline_name),
                    "sport_id": sport_id,
                    "confidence": 1.0,
                    "mapping_source": "connector_world_aquatics_championships_history",
                    "created_at_utc": timestamp,
                }
            )

        competition_id = "world_aquatics_championships"
        competitions_df = pd.DataFrame(
            [
                {
                    "competition_id": competition_id,
                    "sport_id": sport_id,
                    "name": "World Aquatics Championships",
                    "season_year": None,
                    "level": "international_championship",
                    "start_date": frame["event_date"].min(),
                    "end_date": frame["event_date"].max(),
                    "source_id": self.id,
                }
            ]
        )

        events_rows: list[dict[str, Any]] = []
        for year, discipline_name, gender, event_date in (
            frame[["year", "discipline_name_norm", "gender", "event_date"]]
            .drop_duplicates()
            .sort_values(["year", "discipline_name_norm", "gender"])
            .itertuples(index=False)
        ):
            events_rows.append(
                {
                    "event_id": self._event_id(int(year), str(gender), str(discipline_name)),
                    "competition_id": competition_id,
                    "discipline_id": self._discipline_id(str(discipline_name)),
                    "gender": str(gender),
                    "event_class": "podium_top3_by_event",
                    "event_date": str(event_date),
                }
            )

        participants_rows: dict[str, dict[str, Any]] = {}
        countries_rows: dict[str, dict[str, Any]] = {}
        results_rows: list[dict[str, Any]] = []

        for row in frame.itertuples(index=False):
            country_code = str(row.country_code).upper()
            country_name = str(row.country_name).strip() or country_code
            participant_type = str(row.participant_type)

            if participant_type == "team":
                participant_id = country_code
                display_name = country_name
            else:
                athlete_name = str(row.athlete_name).strip()
                participant_id = f"athlete_{self._clean_person_name_for_id(athlete_name)}_{country_code}"
                display_name = athlete_name

            participants_rows[participant_id] = {
                "participant_id": participant_id,
                "type": participant_type,
                "display_name": display_name,
                "country_id": country_code,
            }

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

            rank = int(row.rank)
            medal = str(row.medal)
            event_id = self._event_id(int(row.year), str(row.gender), str(row.discipline_name_norm))
            score_raw = (
                f"discipline={row.discipline_name_norm};source_discipline={row.discipline_name};"
                f"source_event={row.event_name};performance={row.performance};country={country_code}"
            )
            results_rows.append(
                {
                    "event_id": event_id,
                    "participant_id": participant_id,
                    "rank": rank,
                    "medal": medal,
                    "score_raw": score_raw,
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
            "sports": sports_df,
            "disciplines": pd.DataFrame(disciplines_rows).drop_duplicates(subset=["discipline_id"]),
            "competitions": competitions_df,
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
        if participants_df.empty:
            return participants_df, results_df

        athlete_mask = participants_df["type"] == "athlete"
        incoming_athletes = participants_df.loc[athlete_mask].copy()
        if incoming_athletes.empty:
            return participants_df, results_df

        with db.connect() as conn:
            existing_athletes = pd.read_sql_query(
                "SELECT participant_id, display_name, country_id FROM participants WHERE type = 'athlete'",
                conn,
            )

        lookup: dict[tuple[str, str], str] = {}
        for row in existing_athletes.sort_values("participant_id").itertuples(index=False):
            country_id = str(getattr(row, "country_id", "")).upper().strip()
            if not country_id:
                continue
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
        remapped_results["participant_id"] = remapped_results["participant_id"].map(lambda pid: replacement.get(pid, pid))
        rank_sort = pd.to_numeric(remapped_results["rank"], errors="coerce").fillna(10**9)
        remapped_results = (
            remapped_results.assign(_rank_sort=rank_sort)
            .sort_values(["event_id", "participant_id", "_rank_sort"])
            .drop_duplicates(subset=["event_id", "participant_id"], keep="first")
            .drop(columns=["_rank_sort"])
        )

        filtered_participants = participants_df.loc[~participants_df["participant_id"].isin(replacement.keys())].copy()
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
            conn.execute("DELETE FROM disciplines WHERE mapping_source = 'connector_world_aquatics_championships_history'")
            conn.execute(
                """
                DELETE FROM participants
                WHERE participant_id NOT IN (SELECT DISTINCT participant_id FROM results)
                  AND (type = 'athlete' OR type = 'team')
                """
            )
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
