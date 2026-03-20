from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd

from src.core.db import SQLiteDB
from src.core.utils import slugify, utc_now_iso

from .base import Connector


SEED_FILE = "world_wrestling_championships_top3_seed.csv"
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}
RANK_TO_POINTS = {1: 3.0, 2: 2.0, 3: 1.0}
COMPETITION_IDS = {
    "world_wrestling_championships_freestyle",
    "world_wrestling_championships_greco_roman",
}
DISCIPLINE_IDS = {"wrestling-freestyle", "wrestling-greco-roman"}


class WorldWrestlingChampionshipsHistoryConnector(Connector):
    id = "world_wrestling_championships_history"
    name = "World Wrestling Championships Historical Podiums (Top 3 by weight class)"
    source_type = "csv"
    license_notes = (
        "Historical podium seed by weight class curated from public Wikipedia medalist lists "
        "(men freestyle, men Greco-Roman, women freestyle). Verify downstream redistribution requirements."
    )
    base_url = "https://en.wikipedia.org/wiki/World_Wrestling_Championships"

    def source_row(self) -> dict[str, str]:
        return {
            "source_id": self.id,
            "source_name": self.name,
            "source_type": self.source_type,
            "license_notes": (
                "Local seed data/raw/wrestling/world_wrestling_championships_top3_seed.csv built from "
                "Wikipedia medalist lists by style and weight class (strict post-2000 scope). "
                "Allowed rank profiles: 1,2,3 ; 1,2,3,3 ; 1,1,3,3."
            ),
            "base_url": self.base_url,
        }

    def _local_seed_path(self) -> Path:
        return Path(__file__).resolve().parents[2] / "data" / "raw" / "wrestling" / SEED_FILE

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
    def _event_id(competition_id: str, year: int, gender: str, weight_class: str) -> str:
        return f"{competition_id}_{year}_{slugify(gender)}_{slugify(weight_class)}"

    def fetch(self, season_year: int, out_dir: Path) -> list[Path]:
        local_seed = self._local_seed_path()
        if not local_seed.exists():
            raise RuntimeError(f"Missing local seed for world wrestling championships history: {local_seed}")

        frame = pd.read_csv(local_seed)
        if "year" not in frame.columns:
            raise RuntimeError(f"Unsupported world wrestling seed format for {local_seed.name}: missing `year`.")

        frame = frame.copy()
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce")
        frame = frame.dropna(subset=["year"]).copy()
        frame["year"] = frame["year"].astype(int)

        post_2000 = frame.loc[frame["year"] > 2000].copy()
        filtered = post_2000.loc[post_2000["year"] <= int(season_year)].copy()
        if filtered.empty:
            raise RuntimeError(f"No world wrestling rows available for year <= {season_year} and year > 2000.")

        years_available = sorted(int(year) for year in filtered["year"].unique().tolist())
        expected_years = set(range(2001, int(season_year) + 1))
        missing_years = sorted(expected_years - set(years_available))

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
            "weight_class",
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
            raise RuntimeError(
                f"Unsupported World Wrestling seed format for {seed_path.name}: {list(frame.columns)}"
            )

        frame = frame.copy()
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce")
        frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce")
        frame["event_date"] = pd.to_datetime(frame["event_date"], errors="coerce")

        derived_year = frame["event_date"].dt.year
        frame["year"] = frame["year"].fillna(derived_year)

        frame["competition_id"] = frame["competition_id"].fillna("").astype(str).str.strip().str.lower()
        frame["competition_name"] = frame["competition_name"].fillna("").astype(str).str.strip()
        frame["discipline_key"] = frame["discipline_key"].fillna("").astype(str).str.strip().str.lower()
        frame["discipline_name"] = frame["discipline_name"].fillna("").astype(str).str.strip()
        frame["weight_class"] = frame["weight_class"].fillna("").astype(str).str.strip()
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
        frame = frame.loc[(frame["year"] > 2000) & (frame["year"] <= int(season_year))].copy()

        frame = frame.loc[
            frame["competition_id"].isin(COMPETITION_IDS)
            & frame["discipline_key"].isin(DISCIPLINE_IDS)
            & frame["gender"].isin(["men", "women"])
            & (frame["participant_type"] == "athlete")
            & (frame["weight_class"] != "")
            & (frame["event_name"] != "")
            & (frame["participant_name"] != "")
            & (frame["country_code"] != "")
            & frame["rank"].isin([1, 2, 3])
        ].copy()

        if (frame["year"] <= 2000).any():
            offenders = frame.loc[frame["year"] <= 2000, ["competition_id", "year"]].head(10).to_dict("records")
            raise RuntimeError(f"Post-2000 guard violated for world wrestling seed: {offenders}")

        frame["medal"] = frame.apply(
            lambda row: row["medal"] if row["medal"] in {"gold", "silver", "bronze"} else RANK_TO_MEDAL[int(row["rank"])],
            axis=1,
        )
        frame = frame.drop_duplicates(
            subset=[
                "competition_id",
                "year",
                "discipline_key",
                "gender",
                "weight_class",
                "rank",
                "participant_name",
                "country_code",
            ]
        )
        frame = frame.sort_values(
            ["competition_id", "year", "gender", "weight_class", "rank", "country_code", "participant_name"]
        ).reset_index(drop=True)

        if frame.empty:
            raise RuntimeError(f"No world wrestling rows available for year <= {season_year} and year > 2000.")

        profiles = (
            frame.groupby(["competition_id", "year", "discipline_key", "gender", "weight_class"])["rank"]
            .apply(lambda s: tuple(sorted(int(v) for v in s.tolist())))
            .to_dict()
        )
        allowed_profiles = {(1, 1, 3, 3), (1, 2, 3), (1, 2, 3, 3)}
        bad_profiles = {key: value for key, value in profiles.items() if value not in allowed_profiles}
        if bad_profiles:
            sample = dict(list(bad_profiles.items())[:30])
            raise RuntimeError(f"Unexpected rank profiles for world wrestling seed: {sample}")

        timestamp = utc_now_iso()
        sports_df = pd.DataFrame(
            [
                {
                    "sport_id": "wrestling",
                    "sport_name": "Wrestling",
                    "sport_slug": "wrestling",
                    "created_at_utc": timestamp,
                }
            ]
        )

        disciplines_df = (
            frame[["discipline_key", "discipline_name"]]
            .drop_duplicates()
            .sort_values(["discipline_key"])
            .rename(columns={"discipline_key": "discipline_id"})
        )
        disciplines_df["discipline_slug"] = disciplines_df["discipline_name"].map(slugify)
        disciplines_df["sport_id"] = "wrestling"
        disciplines_df["confidence"] = 1.0
        disciplines_df["mapping_source"] = "connector_world_wrestling_championships_history"
        disciplines_df["created_at_utc"] = timestamp

        competitions_df = (
            frame.groupby(["competition_id", "competition_name"], as_index=False)["event_date"]
            .agg(start_date="min", end_date="max")
            .rename(columns={"competition_name": "name"})
        )
        competitions_df["sport_id"] = "wrestling"
        competitions_df["season_year"] = None
        competitions_df["level"] = "international_championship"
        competitions_df["source_id"] = self.id
        competitions_df = competitions_df[
            ["competition_id", "sport_id", "name", "season_year", "level", "start_date", "end_date", "source_id"]
        ]

        events_rows: list[dict[str, Any]] = []
        participants_rows: dict[str, dict[str, Any]] = {}
        countries_rows: dict[str, dict[str, Any]] = {}
        results_rows: list[dict[str, Any]] = []

        for competition_id, year, discipline_key, gender, weight_class, event_date in (
            frame[["competition_id", "year", "discipline_key", "gender", "weight_class", "event_date"]]
            .drop_duplicates()
            .sort_values(["competition_id", "year", "gender", "weight_class"])
            .itertuples(index=False)
        ):
            events_rows.append(
                {
                    "event_id": self._event_id(str(competition_id), int(year), str(gender), str(weight_class)),
                    "competition_id": str(competition_id),
                    "discipline_id": str(discipline_key),
                    "gender": str(gender),
                    "event_class": "podium_top3_by_weight_class",
                    "event_date": str(event_date),
                }
            )

        for row in frame.itertuples(index=False):
            country_code = str(row.country_code).upper()
            country_name = str(row.country_name).strip() or country_code
            athlete_name = str(row.participant_name).strip()
            participant_id = f"athlete_{self._clean_person_name_for_id(athlete_name)}_{country_code}"

            participants_rows[participant_id] = {
                "participant_id": participant_id,
                "type": "athlete",
                "display_name": athlete_name,
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
            event_id = self._event_id(str(row.competition_id), int(row.year), str(row.gender), str(row.weight_class))
            score_raw = (
                f"discipline={row.discipline_key};weight_class={row.weight_class};"
                f"event={row.event_name};country={country_code};source_url={row.source_url}"
            )
            results_rows.append(
                {
                    "event_id": event_id,
                    "participant_id": participant_id,
                    "rank": rank,
                    "medal": "bronze" if rank == 3 else RANK_TO_MEDAL[rank],
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
            "sports": sports_df.drop_duplicates(subset=["sport_id"]),
            "disciplines": disciplines_df.drop_duplicates(subset=["discipline_id"]),
            "competitions": competitions_df.drop_duplicates(subset=["competition_id"]),
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
            conn.execute(
                """
                DELETE FROM disciplines
                WHERE mapping_source = 'connector_world_wrestling_championships_history'
                  AND discipline_id NOT IN (SELECT DISTINCT discipline_id FROM events)
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
