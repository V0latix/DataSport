from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.core.db import SQLiteDB
from src.core.utils import utc_now_iso

from .base import Connector


SEED_FILE = "world_games_medal_table_top10_seed.csv"
COMPETITION_ID = "world_games"
SPORT_ID = "world-games"
DISCIPLINE_ID = "world-games-overall-medal-table"
RANK_PROFILE = tuple(range(1, 11))
COUNTRY_NAME_OVERRIDES = {
    "GER": "Germany",
    "NED": "Netherlands",
    "RUS": "Russia",
    "TPE": "Chinese Taipei",
}


class WorldGamesHistoryConnector(Connector):
    id = "world_games_history"
    name = "World Games Historical Overall Medal Table Top 10"
    source_type = "csv"
    license_notes = (
        "Historical top10 seed curated from public Wikipedia World Games medal tables. "
        "Verify downstream redistribution requirements."
    )
    base_url = "https://en.wikipedia.org/wiki/World_Games"

    def source_row(self) -> dict[str, str]:
        return {
            "source_id": self.id,
            "source_name": self.name,
            "source_type": self.source_type,
            "license_notes": (
                "Local seed data/raw/world_games/world_games_medal_table_top10_seed.csv built from "
                "Wikipedia World Games edition medal tables. Strict post-2000 scope; stores overall "
                "national medal table top10 standings with rank profile 1..10."
            ),
            "base_url": self.base_url,
        }

    def _local_seed_path(self) -> Path:
        return Path(__file__).resolve().parents[2] / "data" / "raw" / "world_games" / SEED_FILE

    @staticmethod
    def _event_id(year: int) -> str:
        return f"{COMPETITION_ID}_overall_medal_table_{year}"

    def fetch(self, season_year: int, out_dir: Path) -> list[Path]:
        local_seed = self._local_seed_path()
        if not local_seed.exists():
            raise RuntimeError(f"Missing local seed for World Games: {local_seed}")

        frame = pd.read_csv(local_seed)
        if "year" not in frame.columns:
            raise RuntimeError(f"Unsupported World Games seed format for {local_seed.name}: missing `year`.")

        frame = frame.copy()
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce")
        frame = frame.dropna(subset=["year"]).copy()
        frame["year"] = frame["year"].astype(int)
        post_2000 = frame.loc[frame["year"] > 2000].copy()
        filtered = post_2000.loc[post_2000["year"] <= int(season_year)].copy()
        if filtered.empty:
            raise RuntimeError(f"No World Games rows for year <= {season_year} and year > 2000.")

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
                "non_world_games_years_up_to_requested": missing_years,
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
            "gold",
            "silver",
            "bronze",
            "total",
            "source_url",
        }
        if not required_cols.issubset(set(frame.columns)):
            raise RuntimeError(f"Unsupported World Games seed columns: {list(frame.columns)}")

        frame = frame.copy()
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce")
        frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce")
        frame["event_date"] = pd.to_datetime(frame["event_date"], errors="coerce")
        frame["year"] = frame["year"].fillna(frame["event_date"].dt.year)
        for column in [
            "competition_id",
            "competition_name",
            "discipline_key",
            "discipline_name",
            "gender",
            "participant_type",
            "participant_name",
            "country_name",
            "country_code",
            "source_url",
        ]:
            frame[column] = frame[column].fillna("").astype(str).str.strip()
        for column in ["gold", "silver", "bronze", "total"]:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

        frame["competition_id"] = frame["competition_id"].str.lower()
        frame["discipline_key"] = frame["discipline_key"].str.lower()
        frame["gender"] = frame["gender"].str.lower()
        frame["participant_type"] = frame["participant_type"].str.lower()
        frame["country_code"] = frame["country_code"].str.upper()
        frame = frame.dropna(subset=["year", "rank", "event_date", "gold", "silver", "bronze", "total"])
        frame["year"] = frame["year"].astype(int)
        frame["rank"] = frame["rank"].astype(int)
        for column in ["gold", "silver", "bronze", "total"]:
            frame[column] = frame[column].astype(int)
        frame["event_date"] = frame["event_date"].dt.strftime("%Y-%m-%d")
        frame = frame.loc[
            (frame["year"] > 2000)
            & (frame["year"] <= int(season_year))
            & (frame["competition_id"] == COMPETITION_ID)
            & (frame["discipline_key"] == DISCIPLINE_ID)
            & (frame["gender"] == "mixed")
            & (frame["participant_type"] == "team")
            & frame["rank"].between(1, 10)
            & (frame["participant_name"] != "")
            & (frame["country_code"] != "")
        ].copy()

        if (frame["year"] <= 2000).any():
            offenders = frame.loc[frame["year"] <= 2000, ["year", "participant_name"]].head(10).to_dict("records")
            raise RuntimeError(f"Post-2000 guard violated for World Games seed: {offenders}")

        frame = frame.drop_duplicates(subset=["year", "rank", "country_code"], keep="first")
        frame = frame.sort_values(["year", "rank", "country_code"]).reset_index(drop=True)
        if frame.empty:
            raise RuntimeError(f"No World Games rows for year <= {season_year} and year > 2000.")

        profiles = frame.groupby("year")["rank"].apply(lambda values: tuple(sorted(values.tolist()))).to_dict()
        bad_profiles = {key: value for key, value in profiles.items() if value != RANK_PROFILE}
        if bad_profiles:
            sample = dict(list(bad_profiles.items())[:20])
            raise RuntimeError(f"Unexpected World Games rank profiles: {sample}")

        timestamp = utc_now_iso()
        sports_df = pd.DataFrame(
            [{"sport_id": SPORT_ID, "sport_name": "World Games", "sport_slug": SPORT_ID, "created_at_utc": timestamp}]
        )
        disciplines_df = pd.DataFrame(
            [
                {
                    "discipline_id": DISCIPLINE_ID,
                    "discipline_name": "World Games overall medal table",
                    "discipline_slug": DISCIPLINE_ID,
                    "sport_id": SPORT_ID,
                    "confidence": 1.0,
                    "mapping_source": f"connector_{self.id}",
                    "created_at_utc": timestamp,
                }
            ]
        )
        competitions_df = pd.DataFrame(
            [
                {
                    "competition_id": COMPETITION_ID,
                    "sport_id": SPORT_ID,
                    "name": "World Games",
                    "season_year": None,
                    "level": "multi_sport_games",
                    "start_date": frame["event_date"].min(),
                    "end_date": frame["event_date"].max(),
                    "source_id": self.id,
                }
            ]
        )

        events_rows: list[dict[str, Any]] = []
        countries_rows: dict[str, dict[str, Any]] = {}
        participants_rows: dict[str, dict[str, Any]] = {}
        results_rows: list[dict[str, Any]] = []

        for year, event_date in (
            frame[["year", "event_date"]].drop_duplicates().sort_values("year").itertuples(index=False)
        ):
            events_rows.append(
                {
                    "event_id": self._event_id(int(year)),
                    "competition_id": COMPETITION_ID,
                    "discipline_id": DISCIPLINE_ID,
                    "gender": "mixed",
                    "event_class": "overall_medal_table_top10",
                    "event_date": str(event_date),
                }
            )

        for row in frame.itertuples(index=False):
            country_code = str(row.country_code).upper()
            country_name = str(row.country_name).strip() or country_code
            participants_rows[country_code] = {
                "participant_id": country_code,
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
                    "name_en": COUNTRY_NAME_OVERRIDES.get(country_code)
                    or (getattr(country_obj, "name", country_name) if country_obj else country_name),
                    "name_fr": None,
                }

            points = float((int(row.gold) * 3) + (int(row.silver) * 2) + int(row.bronze))
            results_rows.append(
                {
                    "event_id": self._event_id(int(row.year)),
                    "participant_id": country_code,
                    "rank": int(row.rank),
                    "medal": "",
                    "score_raw": (
                        "world_games_overall_medal_table;"
                        f"gold={int(row.gold)};silver={int(row.silver)};bronze={int(row.bronze)};"
                        f"total={int(row.total)};source_url={row.source_url}"
                    ),
                    "points_awarded": points,
                }
            )

        results_df = pd.DataFrame(results_rows)
        if not results_df.empty:
            results_df = results_df.sort_values(["event_id", "rank", "participant_id"]).drop_duplicates(
                subset=["event_id", "participant_id"], keep="first"
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

    @staticmethod
    def _assert_result_profiles(results_df: pd.DataFrame) -> None:
        if results_df.empty:
            return
        profiles = (
            results_df.groupby("event_id")["rank"]
            .apply(lambda values: tuple(sorted(int(value) for value in values.tolist())))
            .to_dict()
        )
        bad_profiles = {key: value for key, value in profiles.items() if value != RANK_PROFILE}
        if bad_profiles:
            sample = dict(list(bad_profiles.items())[:20])
            raise RuntimeError(f"Unexpected World Games result profiles: {sample}")

    def upsert(self, db: SQLiteDB, payload: dict[str, pd.DataFrame]) -> None:
        sports_df = payload.get("sports", pd.DataFrame()).copy()
        disciplines_df = payload.get("disciplines", pd.DataFrame()).copy()
        countries_df = payload.get("countries", pd.DataFrame()).copy()
        participants_df = payload.get("participants", pd.DataFrame()).copy()

        with db.connect() as conn:
            existing_sport_ids = {row[0] for row in conn.execute("SELECT sport_id FROM sports").fetchall()}
            existing_discipline_ids = {
                row[0] for row in conn.execute("SELECT discipline_id FROM disciplines").fetchall()
            }
            existing_country_ids = {row[0] for row in conn.execute("SELECT country_id FROM countries").fetchall()}
            existing_participant_ids = {
                row[0] for row in conn.execute("SELECT participant_id FROM participants").fetchall()
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
            conn.commit()

        self._assert_result_profiles(payload.get("results", pd.DataFrame()))
        db.upsert_dataframe("countries", payload.get("countries", pd.DataFrame()), ["country_id"])
        db.upsert_dataframe("sports", payload.get("sports", pd.DataFrame()), ["sport_id"])
        db.upsert_dataframe("disciplines", payload.get("disciplines", pd.DataFrame()), ["discipline_id"])
        db.upsert_dataframe("competitions", payload.get("competitions", pd.DataFrame()), ["competition_id"])
        db.upsert_dataframe("events", payload.get("events", pd.DataFrame()), ["event_id"])
        db.upsert_dataframe("participants", payload.get("participants", pd.DataFrame()), ["participant_id"])
        db.upsert_dataframe("results", payload.get("results", pd.DataFrame()), ["event_id", "participant_id"])
