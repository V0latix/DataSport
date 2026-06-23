from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd

from src.core.db import SQLiteDB
from src.core.utils import slugify, utc_now_iso

from .base import Connector


SEED_FILE = "world_figure_skating_championships_top3_seed.csv"
COMPETITION_ID = "world_figure_skating_championships"
SPORT_ID = "skating"
DISCIPLINE_ID = "figure-skating"
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}
RANK_TO_POINTS = {1: 10.0, 2: 7.0, 3: 5.0}
ALLOWED_PROFILES = {(1, 2, 3)}
COUNTRY_NAME_OVERRIDES = {
    "GBR": "Great Britain",
    "KOR": "South Korea",
    "RUS": "Russia",
}


class WorldFigureSkatingChampionshipsHistoryConnector(Connector):
    id = "world_figure_skating_championships_history"
    name = "World Figure Skating Championships Historical Podiums"
    source_type = "csv"
    license_notes = (
        "Historical podium seed curated from public Wikipedia annual World Figure Skating Championships "
        "medalist and results tables. Verify downstream redistribution requirements."
    )
    base_url = "https://en.wikipedia.org/wiki/World_Figure_Skating_Championships"

    def source_row(self) -> dict[str, str]:
        return {
            "source_id": self.id,
            "source_name": self.name,
            "source_type": self.source_type,
            "license_notes": (
                "Local seed data/raw/skating/world_figure_skating_championships_top3_seed.csv built from "
                "Wikipedia annual medalist tables, with legacy annual result tables used for 2001-2008. "
                "Strict post-2000 scope; 2020 skipped because the championships were cancelled."
            ),
            "base_url": self.base_url,
        }

    def _local_seed_path(self) -> Path:
        return Path(__file__).resolve().parents[2] / "data" / "raw" / "skating" / SEED_FILE

    @staticmethod
    def _clean_participant_name_for_id(name: str) -> str:
        normalized = re.sub(r"\s+", "_", str(name).strip())
        normalized = re.sub(r"[^0-9A-Za-zÀ-ÖØ-öø-ÿ_-]", "", normalized)
        return normalized or slugify(str(name))

    @staticmethod
    def _event_id(year: int, gender: str, event_key: str) -> str:
        return f"{COMPETITION_ID}_{year}_{slugify(gender)}_{slugify(event_key)}"

    def fetch(self, season_year: int, out_dir: Path) -> list[Path]:
        local_seed = self._local_seed_path()
        if not local_seed.exists():
            raise RuntimeError(f"Missing local seed for World Figure Skating Championships: {local_seed}")

        frame = pd.read_csv(local_seed)
        if "year" not in frame.columns:
            raise RuntimeError(f"Unsupported World Figure Skating seed format for {local_seed.name}: missing `year`.")

        frame = frame.copy()
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce")
        frame = frame.dropna(subset=["year"]).copy()
        frame["year"] = frame["year"].astype(int)

        post_2000 = frame.loc[frame["year"] > 2000].copy()
        filtered = post_2000.loc[post_2000["year"] <= int(season_year)].copy()
        if filtered.empty:
            raise RuntimeError(f"No World Figure Skating rows available for year <= {season_year} and year > 2000.")

        years_available = sorted(int(year) for year in filtered["year"].unique().tolist())
        cancelled_or_unavailable_years = sorted(set(range(2001, int(season_year) + 1)) - set(years_available))

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
                "cancelled_or_unavailable_years_up_to_requested": cancelled_or_unavailable_years,
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
            "event_key",
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
            raise RuntimeError(f"Unsupported World Figure Skating seed format for {seed_path.name}: {list(frame.columns)}")

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
            "event_key",
            "event_name",
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
        frame["event_key"] = frame["event_key"].str.lower()
        frame["gender"] = frame["gender"].str.lower()
        frame["medal"] = frame["medal"].str.lower()
        frame["participant_type"] = frame["participant_type"].str.lower()
        frame["country_code"] = frame["country_code"].str.upper()

        frame = frame.dropna(subset=["year", "rank", "event_date"])
        frame["year"] = frame["year"].astype(int)
        frame["rank"] = frame["rank"].astype(int)
        frame["event_date"] = frame["event_date"].dt.strftime("%Y-%m-%d")
        frame = frame.loc[
            (frame["year"] > 2000)
            & (frame["year"] <= int(season_year))
            & (frame["competition_id"] == COMPETITION_ID)
            & (frame["discipline_key"] == DISCIPLINE_ID)
            & frame["event_key"].isin(["men-singles", "women-singles", "pairs", "ice-dance"])
            & frame["gender"].isin(["men", "women", "mixed"])
            & frame["participant_type"].isin(["athlete", "team"])
            & (frame["participant_name"] != "")
            & (frame["country_code"] != "")
            & frame["rank"].isin([1, 2, 3])
        ].copy()

        if (frame["year"] <= 2000).any():
            offenders = frame.loc[frame["year"] <= 2000, ["year", "event_key", "gender"]].head(10).to_dict("records")
            raise RuntimeError(f"Post-2000 guard violated for World Figure Skating seed: {offenders}")

        frame["medal"] = frame.apply(
            lambda row: row["medal"] if row["medal"] in {"gold", "silver", "bronze"} else RANK_TO_MEDAL[int(row["rank"])],
            axis=1,
        )
        frame = frame.drop_duplicates(
            subset=["year", "gender", "event_key", "rank", "participant_name", "country_code"],
            keep="first",
        )
        frame = frame.sort_values(["year", "gender", "event_key", "rank", "country_code", "participant_name"])
        frame = frame.reset_index(drop=True)

        if frame.empty:
            raise RuntimeError(f"No World Figure Skating rows available for year <= {season_year} and year > 2000.")

        profiles = (
            frame.groupby(["year", "gender", "event_key"])["rank"]
            .apply(lambda values: tuple(sorted(int(value) for value in values.tolist())))
            .to_dict()
        )
        bad_profiles = {key: value for key, value in profiles.items() if value not in ALLOWED_PROFILES}
        if bad_profiles:
            sample = dict(list(bad_profiles.items())[:30])
            raise RuntimeError(f"Unexpected World Figure Skating rank profiles: {sample}")

        timestamp = utc_now_iso()
        sports_df = pd.DataFrame(
            [{"sport_id": SPORT_ID, "sport_name": "Skating", "sport_slug": SPORT_ID, "created_at_utc": timestamp}]
        )
        disciplines_df = pd.DataFrame(
            [
                {
                    "discipline_id": DISCIPLINE_ID,
                    "discipline_name": "Figure skating",
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
                    "name": "World Figure Skating Championships",
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

        for year, gender, event_key, event_date in (
            frame[["year", "gender", "event_key", "event_date"]]
            .drop_duplicates()
            .sort_values(["year", "gender", "event_key"])
            .itertuples(index=False)
        ):
            events_rows.append(
                {
                    "event_id": self._event_id(int(year), str(gender), str(event_key)),
                    "competition_id": COMPETITION_ID,
                    "discipline_id": DISCIPLINE_ID,
                    "gender": str(gender),
                    "event_class": "podium_top3",
                    "event_date": str(event_date),
                }
            )

        for row in frame.itertuples(index=False):
            country_code = str(row.country_code).upper()
            country_name = str(row.country_name).strip() or country_code
            participant_name = str(row.participant_name).strip()
            participant_type = str(row.participant_type).strip().lower()
            participant_prefix = "world_figure_team" if participant_type == "team" else "world_figure_athlete"
            participant_id = f"{participant_prefix}_{self._clean_participant_name_for_id(participant_name)}_{country_code}"

            participants_rows[participant_id] = {
                "participant_id": participant_id,
                "type": participant_type,
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
            event_id = self._event_id(int(row.year), str(row.gender), str(row.event_key))
            results_rows.append(
                {
                    "event_id": event_id,
                    "participant_id": participant_id,
                    "rank": rank,
                    "medal": str(row.medal),
                    "score_raw": (
                        f"discipline={row.discipline_name};event={row.event_name};"
                        f"entry_country={country_code};source_url={row.source_url}"
                    ),
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
            raise RuntimeError(f"Unexpected World Figure Skating result profiles after participant build: {sample}")

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
