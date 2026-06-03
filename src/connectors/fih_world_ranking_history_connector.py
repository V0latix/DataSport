from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.core.db import SQLiteDB
from src.core.utils import utc_now_iso

from .base import Connector


SEED_FILE = "fih_world_rankings_top10_seed.csv"
COMPETITIONS = {
    "fih_men_world_ranking": {
        "name": "FIH Men's World Ranking",
        "gender": "men",
    },
    "fih_women_world_ranking": {
        "name": "FIH Women's World Ranking",
        "gender": "women",
    },
}
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}
SOURCE_ARCHIVE_URL = "https://www.fih.hockey/outdoor-hockey-rankings/archive"
SOURCE_ZIP_URL = "https://www.fih.hockey/static-assets/pdf/rankings-archive-2003-2024.zip"
SOURCE_CURRENT_URL = "https://www.fih.hockey/outdoor-hockey-rankings"


class FihWorldRankingHistoryConnector(Connector):
    id = "fih_world_ranking_history"
    name = "FIH World Ranking History (Men/Women Top 10)"
    source_type = "csv"
    license_notes = (
        "Historical top10 seed parsed from official FIH archived Outdoor World Ranking PDFs. "
        "Verify downstream redistribution requirements."
    )
    base_url = SOURCE_ARCHIVE_URL

    def source_row(self) -> dict[str, str]:
        return {
            "source_id": self.id,
            "source_name": self.name,
            "source_type": self.source_type,
            "license_notes": (
                "Local seed data/raw/hockey/fih_world_rankings_top10_seed.csv built from "
                "official FIH rankings archive ZIP (31 December 2003-2024 PDFs, men/women top10) "
                "plus 2025-12-31 snapshot derived from official team ranking pages."
            ),
            "base_url": self.base_url,
        }

    def _local_seed_path(self) -> Path:
        return Path(__file__).resolve().parents[2] / "data" / "raw" / "hockey" / SEED_FILE

    @staticmethod
    def _event_id(competition_id: str, year: int) -> str:
        return f"{competition_id}_{year}"

    @staticmethod
    def _points_from_rank(rank: int) -> float:
        return float(11 - rank)

    def fetch(self, season_year: int, out_dir: Path) -> list[Path]:
        local_seed = self._local_seed_path()
        if not local_seed.exists():
            raise RuntimeError(f"Missing local seed for FIH World Ranking history: {local_seed}")

        frame = pd.read_csv(local_seed)
        if "year" not in frame.columns:
            raise RuntimeError(f"Unsupported FIH ranking seed format for {local_seed.name}: missing `year`.")

        frame = frame.copy()
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce")
        frame = frame.dropna(subset=["year"]).copy()
        frame["year"] = frame["year"].astype(int)

        post_2000 = frame.loc[frame["year"] > 2000].copy()
        filtered = post_2000.loc[post_2000["year"] <= int(season_year)].copy()
        if filtered.empty:
            raise RuntimeError(f"No FIH ranking rows available for year <= {season_year} and year > 2000.")

        years_available = sorted(int(year) for year in filtered["year"].unique().tolist())
        out_file = out_dir / SEED_FILE
        filtered.to_csv(out_file, index=False)
        self._write_json(
            out_dir / "fetch_meta.json",
            {
                "mode": "local_seed_filtered",
                "seed_file": str(local_seed),
                "source_archive_page": SOURCE_ARCHIVE_URL,
                "source_zip": SOURCE_ZIP_URL,
                "rows_total_seed": int(len(frame)),
                "rows_post_2000": int(len(post_2000)),
                "rows_written": int(len(filtered)),
                "year_min_written": int(min(years_available)),
                "year_max_written": int(max(years_available)),
                "available_years_written": years_available,
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
            "gender",
            "rank",
            "participant_type",
            "participant_name",
            "country_name",
            "country_code",
            "source_url",
        }
        if not required_cols.issubset(set(frame.columns)):
            raise RuntimeError(f"Unsupported FIH ranking seed format for {seed_path.name}: {list(frame.columns)}")

        frame = frame.copy()
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce")
        frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce")
        frame["event_date"] = pd.to_datetime(frame["event_date"], errors="coerce")
        frame["competition_id"] = frame["competition_id"].fillna("").astype(str).str.strip().str.lower()
        frame["competition_name"] = frame["competition_name"].fillna("").astype(str).str.strip()
        frame["discipline_key"] = frame["discipline_key"].fillna("").astype(str).str.strip().str.lower()
        frame["discipline_name"] = frame["discipline_name"].fillna("").astype(str).str.strip()
        frame["gender"] = frame["gender"].fillna("").astype(str).str.strip().str.lower()
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
            & frame["competition_id"].isin(set(COMPETITIONS.keys()))
            & (frame["discipline_key"] == "hockey")
            & frame["gender"].isin(["men", "women"])
            & (frame["participant_type"] == "team")
            & frame["rank"].between(1, 10)
            & (frame["participant_name"] != "")
            & (frame["country_code"] != "")
        ].copy()

        if frame.empty:
            raise RuntimeError(f"No FIH ranking rows available for year <= {season_year} and year > 2000.")

        frame = frame.drop_duplicates(
            subset=["competition_id", "year", "rank", "participant_name", "country_code"]
        )
        frame = frame.sort_values(["competition_id", "year", "rank", "country_code"]).reset_index(drop=True)

        expected_profile = tuple(range(1, 11))
        rank_profiles = (
            frame.groupby(["competition_id", "year"])["rank"]
            .apply(lambda ranks: tuple(sorted(int(rank) for rank in ranks.tolist())))
            .to_dict()
        )
        bad_profiles = {key: value for key, value in rank_profiles.items() if value != expected_profile}
        if bad_profiles:
            sample = dict(list(bad_profiles.items())[:20])
            raise RuntimeError(f"Unexpected FIH ranking profiles (expected ranks 1-10): {sample}")

        timestamp = utc_now_iso()
        sports_df = pd.DataFrame(
            [
                {
                    "sport_id": "hockey",
                    "sport_name": "Hockey",
                    "sport_slug": "hockey",
                    "created_at_utc": timestamp,
                }
            ]
        )
        disciplines_df = pd.DataFrame(
            [
                {
                    "discipline_id": "hockey",
                    "discipline_name": "Hockey",
                    "discipline_slug": "hockey",
                    "sport_id": "hockey",
                    "confidence": 1.0,
                    "mapping_source": "connector_fih_world_ranking_history",
                    "created_at_utc": timestamp,
                }
            ]
        )

        competitions_df = (
            frame.groupby(["competition_id", "competition_name"], as_index=False)["event_date"]
            .agg(start_date="min", end_date="max")
            .rename(columns={"competition_name": "name"})
        )
        competitions_df["sport_id"] = "hockey"
        competitions_df["season_year"] = None
        competitions_df["level"] = "national_team_ranking"
        competitions_df["source_id"] = self.id
        competitions_df = competitions_df[
            ["competition_id", "sport_id", "name", "season_year", "level", "start_date", "end_date", "source_id"]
        ]

        events_rows: list[dict[str, Any]] = []
        participants_rows: dict[str, dict[str, Any]] = {}
        countries_rows: dict[str, dict[str, Any]] = {}
        results_rows: list[dict[str, Any]] = []

        for competition_id, year, gender, event_date in (
            frame[["competition_id", "year", "gender", "event_date"]]
            .drop_duplicates()
            .sort_values(["competition_id", "year"])
            .itertuples(index=False)
        ):
            events_rows.append(
                {
                    "event_id": self._event_id(str(competition_id), int(year)),
                    "competition_id": str(competition_id),
                    "discipline_id": "hockey",
                    "gender": str(gender),
                    "event_class": "ranking_release_top10",
                    "event_date": str(event_date),
                }
            )

        for row in frame.itertuples(index=False):
            country_code = str(row.country_code).upper()
            country_name = str(row.country_name).strip() or country_code
            participant_id = country_code

            participants_rows[participant_id] = {
                "participant_id": participant_id,
                "type": "team",
                "display_name": str(row.participant_name).strip() or country_name,
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
            event_id = self._event_id(str(row.competition_id), int(row.year))
            results_rows.append(
                {
                    "event_id": event_id,
                    "participant_id": participant_id,
                    "rank": rank,
                    "medal": RANK_TO_MEDAL.get(rank),
                    "score_raw": f"fih_world_ranking_rank={rank};source_url={row.source_url}",
                    "points_awarded": self._points_from_rank(rank),
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

    def upsert(self, db: SQLiteDB, payload: dict[str, pd.DataFrame]) -> None:
        sports_df = payload.get("sports", pd.DataFrame()).copy()
        disciplines_df = payload.get("disciplines", pd.DataFrame()).copy()
        countries_df = payload.get("countries", pd.DataFrame()).copy()
        participants_df = payload.get("participants", pd.DataFrame()).copy()

        with db.connect() as conn:
            existing_sport_ids = {row[0] for row in conn.execute("SELECT sport_id FROM sports").fetchall()}
            existing_country_ids = {row[0] for row in conn.execute("SELECT country_id FROM countries").fetchall()}
            existing_participant_ids = {
                row[0] for row in conn.execute("SELECT participant_id FROM participants").fetchall()
            }

        if not sports_df.empty:
            sports_df = sports_df.loc[~sports_df["sport_id"].isin(existing_sport_ids)].copy()
        if not disciplines_df.empty:
            with db.connect() as conn:
                existing_discipline_ids = {
                    row[0]
                    for row in conn.execute(
                        "SELECT discipline_id FROM disciplines WHERE discipline_id IN ({})".format(
                            ",".join("?" for _ in disciplines_df["discipline_id"].tolist())
                        ),
                        tuple(disciplines_df["discipline_id"].tolist()),
                    ).fetchall()
                }
            if existing_discipline_ids:
                disciplines_df = disciplines_df.loc[~disciplines_df["discipline_id"].isin(existing_discipline_ids)]
        if not countries_df.empty:
            countries_df = countries_df.loc[~countries_df["country_id"].isin(existing_country_ids)].copy()
        if not participants_df.empty:
            participants_df = participants_df.loc[
                ~participants_df["participant_id"].isin(existing_participant_ids)
            ].copy()

        payload = {
            **payload,
            "sports": sports_df,
            "disciplines": disciplines_df,
            "countries": countries_df,
            "participants": participants_df,
        }

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
                WHERE mapping_source = 'connector_fih_world_ranking_history'
                  AND discipline_id NOT IN (SELECT DISTINCT discipline_id FROM events)
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
