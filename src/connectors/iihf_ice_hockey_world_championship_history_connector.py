from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.core.db import SQLiteDB
from src.core.utils import utc_now_iso

from .base import Connector


SEED_FILE = "iihf_ice_hockey_world_championship_top4_seed.csv"
COMPETITION_IDS = {
    "iihf_ice_hockey_world_championship_men",
    "iihf_ice_hockey_world_championship_women",
}
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}
RANK_TO_POINTS = {1: 10.0, 2: 7.0, 3: 5.0, 4: 4.0}


class IihfIceHockeyWorldChampionshipHistoryConnector(Connector):
    id = "iihf_ice_hockey_world_championship_history"
    name = "IIHF Ice Hockey World Championship Historical Results (Men/Women Top 4)"
    source_type = "csv"
    license_notes = (
        "Historical top4 seed curated from public IIHF/Wikipedia championship tables. "
        "Verify downstream redistribution requirements."
    )
    base_url = (
        "https://en.wikipedia.org/wiki/List_of_IIHF_World_Championship_medalists | "
        "https://en.wikipedia.org/wiki/IIHF_Women%27s_World_Championship"
    )

    def source_row(self) -> dict[str, str]:
        return {
            "source_id": self.id,
            "source_name": self.name,
            "source_type": self.source_type,
            "license_notes": (
                "Local seed data/raw/ice_hockey/iihf_ice_hockey_world_championship_top4_seed.csv "
                "built from Wikipedia/IIHF men's medalists and women's championship result tables "
                "(top4, strict post-2000 scope)."
            ),
            "base_url": self.base_url,
        }

    def _local_seed_path(self) -> Path:
        return Path(__file__).resolve().parents[2] / "data" / "raw" / "ice_hockey" / SEED_FILE

    @staticmethod
    def _event_id(competition_id: str, year: int) -> str:
        return f"{competition_id}_{year}"

    def fetch(self, season_year: int, out_dir: Path) -> list[Path]:
        local_seed = self._local_seed_path()
        if not local_seed.exists():
            raise RuntimeError(f"Missing local seed for IIHF ice hockey world championship history: {local_seed}")

        frame = pd.read_csv(local_seed)
        if "year" not in frame.columns:
            raise RuntimeError(f"Unsupported IIHF seed format for {local_seed.name}: missing `year`.")

        frame = frame.copy()
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce")
        frame = frame.dropna(subset=["year"]).copy()
        frame["year"] = frame["year"].astype(int)

        post_2000 = frame.loc[frame["year"] > 2000].copy()
        filtered = post_2000.loc[post_2000["year"] <= int(season_year)].copy()
        if filtered.empty:
            raise RuntimeError(f"No IIHF rows available for year <= {season_year} and year > 2000.")

        years_available = sorted(int(year) for year in filtered["year"].unique().tolist())
        skipped_by_competition: dict[str, list[int]] = {}
        for competition_id, expected_years in {
            "iihf_ice_hockey_world_championship_men": [2020],
            "iihf_ice_hockey_world_championship_women": [2002, 2003, 2006, 2010, 2014, 2018, 2020, 2026],
        }.items():
            skipped_by_competition[competition_id] = [
                year for year in expected_years if 2000 < year <= int(season_year)
            ]

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
                "known_missing_or_unfinished_years_after_filtering": skipped_by_competition,
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
            "medal",
            "participant_type",
            "participant_name",
            "country_name",
            "country_code",
            "source_url",
        }
        if not required_cols.issubset(set(frame.columns)):
            raise RuntimeError(f"Unsupported IIHF seed format for {seed_path.name}: {list(frame.columns)}")

        frame = frame.copy()
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce")
        frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce")
        frame["event_date"] = pd.to_datetime(frame["event_date"], errors="coerce")

        derived_year = frame["event_date"].dt.year
        frame["year"] = frame["year"].fillna(derived_year)

        for column in [
            "competition_id",
            "competition_name",
            "discipline_key",
            "discipline_name",
            "gender",
            "medal",
            "participant_type",
            "participant_name",
            "country_name",
            "country_code",
            "source_url",
        ]:
            frame[column] = frame[column].fillna("").astype(str).str.strip()

        frame["competition_id"] = frame["competition_id"].str.lower()
        frame["discipline_key"] = frame["discipline_key"].str.lower()
        frame["gender"] = frame["gender"].str.lower()
        frame["participant_type"] = frame["participant_type"].str.lower()
        frame["country_code"] = frame["country_code"].str.upper()
        frame["medal"] = frame["medal"].str.lower()

        frame = frame.dropna(subset=["year", "rank", "event_date"])
        frame["year"] = frame["year"].astype(int)
        frame["rank"] = frame["rank"].astype(int)
        frame["event_date"] = frame["event_date"].dt.strftime("%Y-%m-%d")

        frame = frame.loc[
            (frame["year"] > 2000)
            & (frame["year"] <= int(season_year))
            & frame["competition_id"].isin(COMPETITION_IDS)
            & (frame["discipline_key"] == "ice-hockey")
            & frame["gender"].isin(["men", "women"])
            & (frame["participant_type"] == "team")
            & frame["rank"].isin([1, 2, 3, 4])
            & (frame["participant_name"] != "")
            & (frame["country_code"] != "")
        ].copy()

        if (frame["year"] <= 2000).any():
            offenders = frame.loc[frame["year"] <= 2000, ["competition_id", "year"]].head(10).to_dict("records")
            raise RuntimeError(f"Post-2000 guard violated for IIHF seed: {offenders}")

        frame = frame.drop_duplicates(
            subset=["competition_id", "year", "rank", "participant_name", "country_code"]
        )
        frame = frame.sort_values(["competition_id", "year", "rank", "country_code"]).reset_index(drop=True)

        if frame.empty:
            raise RuntimeError(f"No IIHF rows available for year <= {season_year} and year > 2000.")

        rank_profiles = (
            frame.groupby(["competition_id", "year"])["rank"]
            .apply(lambda s: tuple(sorted(int(v) for v in s.tolist())))
            .to_dict()
        )
        expected_profile = (1, 2, 3, 4)
        bad_profiles = {key: value for key, value in rank_profiles.items() if value != expected_profile}
        if bad_profiles:
            sample = dict(list(bad_profiles.items())[:20])
            raise RuntimeError(f"Unexpected IIHF rank profiles (expected 1,2,3,4): {sample}")

        timestamp = utc_now_iso()
        sports_df = pd.DataFrame(
            [
                {
                    "sport_id": "ice-hockey",
                    "sport_name": "Ice Hockey",
                    "sport_slug": "ice-hockey",
                    "created_at_utc": timestamp,
                }
            ]
        )

        disciplines_df = pd.DataFrame(
            [
                {
                    "discipline_id": "ice-hockey",
                    "discipline_name": "Ice Hockey",
                    "discipline_slug": "ice-hockey",
                    "sport_id": "ice-hockey",
                    "confidence": 1.0,
                    "mapping_source": "connector_iihf_ice_hockey_world_championship_history",
                    "created_at_utc": timestamp,
                }
            ]
        )

        competitions_df = (
            frame.groupby(["competition_id", "competition_name"], as_index=False)["event_date"]
            .agg(start_date="min", end_date="max")
            .rename(columns={"competition_name": "name"})
        )
        competitions_df["sport_id"] = "ice-hockey"
        competitions_df["season_year"] = None
        competitions_df["level"] = "national_team_tournament"
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
                    "discipline_id": "ice-hockey",
                    "gender": str(gender),
                    "event_class": "final_ranking_top4",
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
            score_raw = f"iihf_ice_hockey_world_championship_final_rank={rank};source_url={row.source_url}"
            results_rows.append(
                {
                    "event_id": event_id,
                    "participant_id": participant_id,
                    "rank": rank,
                    "medal": RANK_TO_MEDAL.get(rank),
                    "score_raw": score_raw,
                    "points_awarded": RANK_TO_POINTS.get(rank),
                }
            )

        results_df = pd.DataFrame(results_rows)
        if not results_df.empty:
            rank_sort = pd.to_numeric(results_df["rank"], errors="coerce").fillna(10**9)
            results_df = (
                results_df.assign(_rank_sort=rank_sort)
                .sort_values(["event_id", "_rank_sort", "participant_id"])
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
            existing_discipline_ids = {
                row[0] for row in conn.execute("SELECT discipline_id FROM disciplines").fetchall()
            }

        if not sports_df.empty:
            sports_df = sports_df.loc[~sports_df["sport_id"].isin(existing_sport_ids)].copy()
        if not disciplines_df.empty:
            disciplines_df = disciplines_df.loc[~disciplines_df["discipline_id"].isin(existing_discipline_ids)].copy()
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
                WHERE mapping_source = 'connector_iihf_ice_hockey_world_championship_history'
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
