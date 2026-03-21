from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.core.db import SQLiteDB
from src.core.utils import slugify, utc_now_iso

from .base import Connector


SEED_FILE = "formulae_world_standings_top10_seed.csv"
COMPETITION_IDS = {
    "formulae_drivers_world_championship",
    "formulae_teams_world_championship",
}
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}
SERIES_START_YEAR = 2015


class FormulaEWorldChampionshipHistoryConnector(Connector):
    id = "formulae_world_championship_history"
    name = "Formula E Championship Final Standings (Top 10 Drivers + Teams)"
    source_type = "csv"
    license_notes = (
        "Historical yearly final standings from public Wikipedia season pages, materialized as a local seed. "
        "Verify downstream redistribution requirements."
    )
    base_url = "https://en.wikipedia.org/wiki/Formula_E"

    def source_row(self) -> dict[str, str]:
        return {
            "source_id": self.id,
            "source_name": self.name,
            "source_type": self.source_type,
            "license_notes": (
                "Local seed data/raw/formulae/formulae_world_standings_top10_seed.csv "
                "built from Formula E season pages (drivers + teams standings tables). "
                "Top 10 strict per season/event (rank profile 1..10), scope year > 2000, completed seasons only."
            ),
            "base_url": self.base_url,
        }

    def _local_seed_path(self) -> Path:
        return Path(__file__).resolve().parents[2] / "data" / "raw" / "formulae" / SEED_FILE

    @staticmethod
    def _event_id(competition_id: str, year: int) -> str:
        return f"{competition_id}_{year}"

    @staticmethod
    def _participant_id(participant_type: str, participant_ref: str, participant_name: str) -> str:
        prefix = "driver" if participant_type == "athlete" else "team"
        reference = str(participant_ref or "").strip().lower()
        if reference:
            return f"{prefix}_{slugify(reference)}"
        return f"{prefix}_{slugify(str(participant_name))}"

    def fetch(self, season_year: int, out_dir: Path) -> list[Path]:
        local_seed = self._local_seed_path()
        if not local_seed.exists():
            raise RuntimeError(f"Missing local seed for Formula E championship history: {local_seed}")

        frame = pd.read_csv(local_seed)
        if "year" not in frame.columns:
            raise RuntimeError(f"Unsupported Formula E seed format for {local_seed.name}: missing `year`.")

        frame = frame.copy()
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce")
        frame = frame.dropna(subset=["year"]).copy()
        frame["year"] = frame["year"].astype(int)

        current_year = datetime.now(timezone.utc).year
        max_completed_season = min(int(season_year), current_year - 1)
        filtered = frame.loc[(frame["year"] > 2000) & (frame["year"] <= max_completed_season)].copy()
        if filtered.empty:
            raise RuntimeError(
                f"No Formula E rows available for completed seasons <= {max_completed_season} and year > 2000."
            )

        years_available = sorted(int(year) for year in filtered["year"].unique().tolist())
        expected_start = max(SERIES_START_YEAR, 2001)
        expected_years = set(range(expected_start, int(season_year) + 1))
        missing_years = sorted(expected_years - set(years_available))

        out_file = out_dir / SEED_FILE
        filtered.to_csv(out_file, index=False)
        self._write_json(
            out_dir / "fetch_meta.json",
            {
                "mode": "local_seed_filtered",
                "seed_file": str(local_seed),
                "rows_total_seed": int(len(frame)),
                "rows_written": int(len(filtered)),
                "year_min_written": int(min(years_available)),
                "year_max_written": int(max(years_available)),
                "missing_years_post_2000_up_to_requested": missing_years,
                "series_start_year": SERIES_START_YEAR,
                "current_year_utc": current_year,
                "max_completed_season_used": max_completed_season,
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
            "participant_ref",
            "participant_name",
            "country_code",
            "country_name",
            "nationality",
            "points",
            "wins",
            "team_name",
            "round",
            "season_label",
            "source_url",
        }
        if not required_cols.issubset(set(frame.columns)):
            raise RuntimeError(f"Unsupported Formula E seed format for {seed_path.name}: {list(frame.columns)}")

        frame = frame.copy()
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce")
        frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce")
        frame["points"] = pd.to_numeric(frame["points"], errors="coerce")
        frame["wins"] = pd.to_numeric(frame["wins"], errors="coerce")
        frame["event_date"] = pd.to_datetime(frame["event_date"], errors="coerce")

        derived_year = frame["event_date"].dt.year
        frame["year"] = frame["year"].fillna(derived_year)

        current_year = datetime.now(timezone.utc).year
        max_completed_season = min(int(season_year), current_year - 1)

        frame["competition_id"] = frame["competition_id"].fillna("").astype(str).str.strip().str.lower()
        frame["competition_name"] = frame["competition_name"].fillna("").astype(str).str.strip()
        frame["discipline_key"] = frame["discipline_key"].fillna("").astype(str).str.strip().str.lower()
        frame["discipline_name"] = frame["discipline_name"].fillna("").astype(str).str.strip()
        frame["gender"] = frame["gender"].fillna("").astype(str).str.strip().str.lower()
        frame["participant_type"] = frame["participant_type"].fillna("").astype(str).str.strip().str.lower()
        frame["participant_ref"] = frame["participant_ref"].fillna("").astype(str).str.strip().str.lower()
        frame["participant_name"] = frame["participant_name"].fillna("").astype(str).str.strip()
        frame["country_code"] = frame["country_code"].fillna("").astype(str).str.strip().str.upper()
        frame["country_name"] = frame["country_name"].fillna("").astype(str).str.strip()
        frame["nationality"] = frame["nationality"].fillna("").astype(str).str.strip()
        frame["team_name"] = frame["team_name"].fillna("").astype(str).str.strip()
        frame["round"] = frame["round"].fillna("").astype(str).str.strip()
        frame["season_label"] = frame["season_label"].fillna("").astype(str).str.strip()
        frame["source_url"] = frame["source_url"].fillna("").astype(str).str.strip()

        frame = frame.dropna(subset=["year", "rank", "event_date"])
        frame["year"] = frame["year"].astype(int)
        frame["rank"] = frame["rank"].astype(int)
        frame["event_date"] = frame["event_date"].dt.strftime("%Y-%m-%d")

        frame = frame.loc[
            (frame["year"] > 2000)
            & (frame["year"] <= max_completed_season)
            & frame["competition_id"].isin(COMPETITION_IDS)
            & (frame["discipline_key"] == "formula-e")
            & (frame["gender"] == "mixed")
            & frame["participant_type"].isin(["athlete", "team"])
            & frame["rank"].isin(list(range(1, 11)))
            & (frame["participant_name"] != "")
        ].copy()

        if (frame["year"] <= 2000).any():
            offenders = frame.loc[frame["year"] <= 2000, ["competition_id", "year"]].head(10).to_dict("records")
            raise RuntimeError(f"Post-2000 guard violated for Formula E seed: {offenders}")

        frame = frame.drop_duplicates(
            subset=[
                "competition_id",
                "year",
                "rank",
                "participant_type",
                "participant_name",
            ]
        )
        frame = frame.sort_values(
            ["competition_id", "year", "rank", "participant_type", "participant_name"]
        ).reset_index(drop=True)

        if frame.empty:
            raise RuntimeError(
                f"No Formula E rows available for completed seasons <= {max_completed_season} and year > 2000."
            )

        rank_profiles = (
            frame.groupby(["competition_id", "year"])["rank"]
            .apply(lambda s: tuple(sorted(int(v) for v in s.tolist())))
            .to_dict()
        )
        expected_profile = tuple(range(1, 11))
        bad_profiles = {key: value for key, value in rank_profiles.items() if value != expected_profile}
        if bad_profiles:
            sample = dict(list(bad_profiles.items())[:20])
            raise RuntimeError(f"Unexpected Formula E rank profiles (expected 1..10): {sample}")

        timestamp = utc_now_iso()
        sports_df = pd.DataFrame(
            [
                {
                    "sport_id": "motorsport",
                    "sport_name": "Motorsport",
                    "sport_slug": "motorsport",
                    "created_at_utc": timestamp,
                }
            ]
        )

        disciplines_df = pd.DataFrame(
            [
                {
                    "discipline_id": "formula-e",
                    "discipline_name": "Formula E",
                    "discipline_slug": "formula-e",
                    "sport_id": "motorsport",
                    "confidence": 1.0,
                    "mapping_source": "connector_formulae_world_championship_history",
                    "created_at_utc": timestamp,
                }
            ]
        )

        competitions_df = (
            frame.groupby(["competition_id", "competition_name"], as_index=False)["event_date"]
            .agg(start_date="min", end_date="max")
            .rename(columns={"competition_name": "name"})
        )
        competitions_df["sport_id"] = "motorsport"
        competitions_df["season_year"] = None
        competitions_df["level"] = "professional_championship"
        competitions_df["source_id"] = self.id
        competitions_df = competitions_df[
            ["competition_id", "sport_id", "name", "season_year", "level", "start_date", "end_date", "source_id"]
        ]

        events_rows: list[dict[str, Any]] = []
        participants_rows: dict[str, dict[str, Any]] = {}
        countries_rows: dict[str, dict[str, Any]] = {}
        results_rows: list[dict[str, Any]] = []

        for competition_id, year, event_date in (
            frame[["competition_id", "year", "event_date"]]
            .drop_duplicates()
            .sort_values(["competition_id", "year"])
            .itertuples(index=False)
        ):
            events_rows.append(
                {
                    "event_id": self._event_id(str(competition_id), int(year)),
                    "competition_id": str(competition_id),
                    "discipline_id": "formula-e",
                    "gender": "mixed",
                    "event_class": "final_ranking_top10",
                    "event_date": str(event_date),
                }
            )

        for row in frame.itertuples(index=False):
            country_code = str(row.country_code).upper()
            participant_id = self._participant_id(
                str(row.participant_type), str(row.participant_ref), str(row.participant_name)
            )

            participants_rows[participant_id] = {
                "participant_id": participant_id,
                "type": str(row.participant_type),
                "display_name": str(row.participant_name),
                "country_id": country_code or None,
            }

            if country_code:
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
                        "name_en": getattr(country_obj, "name", country_code) if country_obj else country_code,
                        "name_fr": None,
                    }

            rank = int(row.rank)
            medal = RANK_TO_MEDAL.get(rank)
            event_id = self._event_id(str(row.competition_id), int(row.year))
            score_raw = (
                f"points={row.points};wins={row.wins};team={row.team_name};"
                f"season={row.season_label};round={row.round};source_url={row.source_url}"
            )
            results_rows.append(
                {
                    "event_id": event_id,
                    "participant_id": participant_id,
                    "rank": rank,
                    "medal": medal,
                    "score_raw": score_raw,
                    "points_awarded": float(row.points) if pd.notna(row.points) else None,
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
                WHERE mapping_source = 'connector_formulae_world_championship_history'
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
