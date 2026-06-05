from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Any

import pandas as pd

from src.core.db import SQLiteDB
from src.core.utils import slugify, utc_now_iso

from .base import Connector


SEED_FILE = "uci_cyclocross_world_championships_top3_seed.csv"
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}
RANK_TO_POINTS = {1: 10.0, 2: 7.0, 3: 5.0}
ALLOWED_PROFILES = {(1, 2, 3)}
COUNTRY_NAME_OVERRIDES = {
    "GER": "Germany",
}


class UciCyclocrossWorldChampionshipsHistoryConnector(Connector):
    id = "uci_cyclocross_world_championships_history"
    name = "UCI Cyclo-cross World Championships Historical Elite Podiums"
    source_type = "csv"
    license_notes = (
        "Historical elite podium seed curated from public Wikipedia UCI Cyclo-cross World Championships "
        "men's and women's elite race pages. Verify downstream redistribution requirements."
    )
    base_url = "https://en.wikipedia.org/wiki/UCI_Cyclo-cross_World_Championships"

    def source_row(self) -> dict[str, str]:
        return {
            "source_id": self.id,
            "source_name": self.name,
            "source_type": self.source_type,
            "license_notes": (
                "Local seed data/raw/cycling/uci_cyclocross_world_championships_top3_seed.csv built from "
                "Wikipedia men's and women's elite race podium tables. Strict post-2000 scope; "
                "U23 and junior categories excluded."
            ),
            "base_url": self.base_url,
        }

    def _local_seed_path(self) -> Path:
        return Path(__file__).resolve().parents[2] / "data" / "raw" / "cycling" / SEED_FILE

    @staticmethod
    def _clean_participant_name_for_id(name: str) -> str:
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
    def _event_id(year: int, gender: str) -> str:
        return f"uci_cyclocross_world_championships_{year}_{slugify(gender)}_elite"

    def fetch(self, season_year: int, out_dir: Path) -> list[Path]:
        local_seed = self._local_seed_path()
        if not local_seed.exists():
            raise RuntimeError(f"Missing local seed for UCI Cyclo-cross worlds: {local_seed}")

        frame = pd.read_csv(local_seed)
        if "year" not in frame.columns:
            raise RuntimeError(f"Unsupported UCI Cyclo-cross seed format for {local_seed.name}: missing `year`.")

        frame = frame.copy()
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce")
        frame = frame.dropna(subset=["year"]).copy()
        frame["year"] = frame["year"].astype(int)

        post_2000 = frame.loc[frame["year"] > 2000].copy()
        filtered = post_2000.loc[post_2000["year"] <= int(season_year)].copy()
        if filtered.empty:
            raise RuntimeError(f"No UCI Cyclo-cross rows available for year <= {season_year} and year > 2000.")

        years_available = sorted(int(year) for year in filtered["year"].unique().tolist())
        missing_years = sorted(set(range(2001, int(season_year) + 1)) - set(years_available))

        out_file = out_dir / SEED_FILE
        filtered.to_csv(out_file, index=False)
        self._write_json(
            out_dir / "fetch_meta.json",
            {
                "mode": "local_seed_filtered",
                "seed_file": str(local_seed),
                "rows_total_seed": int(len(frame)),
                "rows_post_2000": int(len(post_2000)),
                "rows_written": int(len(filtered)),
                "year_min_written": int(min(years_available)),
                "year_max_written": int(max(years_available)),
                "available_years_written": years_available,
                "missing_years_post_2000_up_to_requested": missing_years,
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
            "discipline_key",
            "discipline_name",
            "event_name",
            "gender",
            "rank",
            "medal",
            "participant_type",
            "participant_name",
            "country_name",
            "country_code",
            "source_url",
        }
        if not required_cols.issubset(set(frame.columns)):
            raise RuntimeError(f"Unsupported UCI Cyclo-cross seed format for {seed_path.name}: {list(frame.columns)}")

        frame = frame.copy()
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce")
        frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce")
        frame["event_date"] = pd.to_datetime(frame["event_date"], errors="coerce")
        frame["year"] = frame["year"].fillna(frame["event_date"].dt.year)
        frame["competition_id"] = frame["competition_id"].fillna("").astype(str).str.strip().str.lower()
        frame["competition_name"] = frame["competition_name"].fillna("").astype(str).str.strip()
        frame["discipline_key"] = frame["discipline_key"].fillna("").astype(str).str.strip().str.lower()
        frame["discipline_name"] = frame["discipline_name"].fillna("").astype(str).str.strip()
        frame["event_name"] = frame["event_name"].fillna("").astype(str).str.strip()
        frame["gender"] = frame["gender"].fillna("").astype(str).str.strip().str.lower()
        frame["medal"] = frame["medal"].fillna("").astype(str).str.strip().str.lower()
        frame["participant_type"] = frame["participant_type"].fillna("").astype(str).str.strip().str.lower()
        frame["participant_name"] = frame["participant_name"].fillna("").astype(str).str.strip()
        frame["country_name"] = frame["country_name"].fillna("").astype(str).str.strip()
        frame["country_code"] = frame["country_code"].fillna("").astype(str).str.strip().str.upper()
        frame["source_url"] = frame["source_url"].fillna("").astype(str).str.strip()

        frame = frame.dropna(subset=["year", "rank", "event_date"])
        frame["year"] = frame["year"].astype(int)
        frame["rank"] = frame["rank"].astype(int)
        frame["event_date"] = frame["event_date"].dt.strftime("%Y-%m-%d")
        frame = frame.loc[
            (frame["year"] > 2000)
            & (frame["year"] <= int(season_year))
            & (frame["competition_id"] == "uci_cyclocross_world_championships")
            & (frame["discipline_key"] == "cycling-cyclo-cross")
            & frame["gender"].isin(["men", "women"])
            & (frame["participant_type"] == "athlete")
            & (frame["participant_name"] != "")
            & (frame["country_code"] != "")
            & frame["rank"].isin([1, 2, 3])
        ].copy()

        if (frame["year"] <= 2000).any():
            offenders = frame.loc[frame["year"] <= 2000, ["year", "event_name"]].head(10).to_dict("records")
            raise RuntimeError(f"Post-2000 guard violated for UCI Cyclo-cross seed: {offenders}")

        frame["medal"] = frame.apply(
            lambda row: row["medal"] if row["medal"] in {"gold", "silver", "bronze"} else RANK_TO_MEDAL[int(row["rank"])],
            axis=1,
        )
        frame = frame.drop_duplicates(
            subset=["year", "gender", "rank", "participant_name", "country_code"],
            keep="first",
        )
        frame = frame.sort_values(["year", "gender", "rank", "country_code", "participant_name"]).reset_index(drop=True)

        if frame.empty:
            raise RuntimeError(f"No UCI Cyclo-cross rows available for year <= {season_year} and year > 2000.")

        profiles = (
            frame.groupby(["year", "gender"])["rank"]
            .apply(lambda values: tuple(sorted(int(value) for value in values.tolist())))
            .to_dict()
        )
        bad_profiles = {key: value for key, value in profiles.items() if value not in ALLOWED_PROFILES}
        if bad_profiles:
            sample = dict(list(bad_profiles.items())[:30])
            raise RuntimeError(f"Unexpected UCI Cyclo-cross rank profiles: {sample}")

        timestamp = utc_now_iso()
        sports_df = pd.DataFrame(
            [
                {
                    "sport_id": "cycling",
                    "sport_name": "Cycling",
                    "sport_slug": "cycling",
                    "created_at_utc": timestamp,
                }
            ]
        )
        disciplines_df = pd.DataFrame(
            [
                {
                    "discipline_id": "cycling-cyclo-cross",
                    "discipline_name": "Cyclo-cross",
                    "discipline_slug": "cyclo-cross",
                    "sport_id": "cycling",
                    "confidence": 1.0,
                    "mapping_source": "connector_uci_cyclocross_world_championships_history",
                    "created_at_utc": timestamp,
                }
            ]
        )
        competitions_df = pd.DataFrame(
            [
                {
                    "competition_id": "uci_cyclocross_world_championships",
                    "sport_id": "cycling",
                    "name": "UCI Cyclo-cross World Championships",
                    "season_year": None,
                    "level": "international_championship",
                    "start_date": frame["event_date"].min(),
                    "end_date": frame["event_date"].max(),
                    "source_id": self.id,
                }
            ]
        )

        events_rows: list[dict[str, Any]] = []
        participants_rows: dict[str, dict[str, Any]] = {}
        countries_rows: dict[str, dict[str, Any]] = {}
        results_rows: list[dict[str, Any]] = []

        for year, gender, event_date in (
            frame[["year", "gender", "event_date"]]
            .drop_duplicates()
            .sort_values(["year", "gender"])
            .itertuples(index=False)
        ):
            events_rows.append(
                {
                    "event_id": self._event_id(int(year), str(gender)),
                    "competition_id": "uci_cyclocross_world_championships",
                    "discipline_id": "cycling-cyclo-cross",
                    "gender": str(gender),
                    "event_class": "podium_top3_elite",
                    "event_date": str(event_date),
                }
            )

        for row in frame.itertuples(index=False):
            country_code = str(row.country_code).upper()
            country_name = str(row.country_name).strip() or country_code
            participant_name = str(row.participant_name).strip()
            participant_id = f"athlete_{self._clean_participant_name_for_id(participant_name)}_{country_code}"

            participants_rows[participant_id] = {
                "participant_id": participant_id,
                "type": "athlete",
                "display_name": participant_name,
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
                    "name_en": COUNTRY_NAME_OVERRIDES.get(country_code)
                    or (getattr(country_obj, "name", country_name) if country_obj else country_name),
                    "name_fr": None,
                }

            rank = int(row.rank)
            event_id = self._event_id(int(row.year), str(row.gender))
            results_rows.append(
                {
                    "event_id": event_id,
                    "participant_id": participant_id,
                    "rank": rank,
                    "medal": str(row.medal),
                    "score_raw": f"discipline=cycling-cyclo-cross;category=elite;source_url={row.source_url}",
                    "points_awarded": RANK_TO_POINTS.get(rank),
                }
            )

        return {
            "countries": pd.DataFrame(countries_rows.values()).drop_duplicates(subset=["country_id"]),
            "sports": sports_df.drop_duplicates(subset=["sport_id"]),
            "disciplines": disciplines_df.drop_duplicates(subset=["discipline_id"]),
            "competitions": competitions_df.drop_duplicates(subset=["competition_id"]),
            "events": pd.DataFrame(events_rows).drop_duplicates(subset=["event_id"]),
            "participants": pd.DataFrame(participants_rows.values()).drop_duplicates(subset=["participant_id"]),
            "results": pd.DataFrame(results_rows).drop_duplicates(subset=["event_id", "participant_id"]),
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

    @staticmethod
    def _assert_result_profiles(results_df: pd.DataFrame) -> None:
        if results_df.empty:
            return
        profiles = (
            results_df.groupby("event_id")["rank"]
            .apply(lambda values: tuple(sorted(int(value) for value in values.tolist())))
            .to_dict()
        )
        bad_profiles = {key: value for key, value in profiles.items() if value not in ALLOWED_PROFILES}
        if bad_profiles:
            sample = dict(list(bad_profiles.items())[:30])
            raise RuntimeError(f"Unexpected UCI Cyclo-cross result profiles after participant remap: {sample}")

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
        self._assert_result_profiles(results_df)
        payload = {**payload, "participants": participants_df, "results": results_df}

        db.upsert_dataframe("countries", payload.get("countries", pd.DataFrame()), ["country_id"])
        db.upsert_dataframe("sports", payload.get("sports", pd.DataFrame()), ["sport_id"])
        db.upsert_dataframe("disciplines", payload.get("disciplines", pd.DataFrame()), ["discipline_id"])
        db.upsert_dataframe("competitions", payload.get("competitions", pd.DataFrame()), ["competition_id"])
        db.upsert_dataframe("events", payload.get("events", pd.DataFrame()), ["event_id"])
        db.upsert_dataframe("participants", payload.get("participants", pd.DataFrame()), ["participant_id"])
        db.upsert_dataframe("results", payload.get("results", pd.DataFrame()), ["event_id", "participant_id"])
