from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import pandas as pd

from src.core.db import SQLiteDB
from src.core.utils import slugify, utc_now_iso

from .base import Connector


COUNTRY_OVERRIDES = {
    "Great Britain": "GBR",
    "Chinese Taipei": "TPE",
    "Republic of China": "TPE",
    "South Korea": "KOR",
    "North Korea": "PRK",
    "Soviet Union": "URS",
    "Yugoslavia": "YUG",
    "Czechoslovakia": "TCH",
    "East Germany": "GDR",
    "West Germany": "FRG",
    "Serbia and Montenegro": "SCG",
    "FR Yugoslavia": "YUG",
}

COMPETITIONS: dict[str, dict[str, str]] = {
    "baseball_world_cup_men": {
        "seed_file": "wbsc_baseball_world_cup_men_top4_seed.csv",
        "competition_id": "wbsc_baseball_world_cup_men",
        "competition_name": "WBSC Baseball World Cup (Men)",
        "sport_id": "baseball",
        "sport_name": "Baseball",
        "discipline_id": "baseball",
        "discipline_name": "Baseball",
        "gender": "men",
        "event_class": "final_ranking_top4",
        "score_prefix": "wbsc_baseball_world_cup_final_rank",
    },
    "womens_baseball_world_cup": {
        "seed_file": "wbsc_womens_baseball_world_cup_top4_seed.csv",
        "competition_id": "wbsc_womens_baseball_world_cup",
        "competition_name": "WBSC Women's Baseball World Cup",
        "sport_id": "baseball",
        "sport_name": "Baseball",
        "discipline_id": "baseball",
        "discipline_name": "Baseball",
        "gender": "women",
        "event_class": "final_ranking_top4",
        "score_prefix": "wbsc_womens_baseball_world_cup_final_rank",
    },
    "mens_softball_world_cup": {
        "seed_file": "wbsc_mens_softball_world_cup_top4_seed.csv",
        "competition_id": "wbsc_mens_softball_world_cup",
        "competition_name": "WBSC Men's Softball World Cup",
        "sport_id": "baseball",
        "sport_name": "Baseball",
        "discipline_id": "softball",
        "discipline_name": "Softball",
        "gender": "men",
        "event_class": "final_ranking_top4",
        "score_prefix": "wbsc_mens_softball_world_cup_final_rank",
    },
    "womens_softball_world_cup": {
        "seed_file": "wbsc_womens_softball_world_cup_top4_seed.csv",
        "competition_id": "wbsc_womens_softball_world_cup",
        "competition_name": "WBSC Women's Softball World Cup",
        "sport_id": "baseball",
        "sport_name": "Baseball",
        "discipline_id": "softball",
        "discipline_name": "Softball",
        "gender": "women",
        "event_class": "final_ranking_top4",
        "score_prefix": "wbsc_womens_softball_world_cup_final_rank",
    },
}


class WbscBaseballSoftballWorldChampionshipHistoryConnector(Connector):
    id = "wbsc_baseball_softball_world_championship_history"
    name = "WBSC Baseball/Softball World Championships Historical Results (Men/Women)"
    source_type = "csv"
    license_notes = (
        "Historical top4 seeds curated from public WBSC / Wikipedia references. "
        "Verify downstream redistribution requirements."
    )
    base_url = "https://www.wbsc.org | https://en.wikipedia.org/wiki/Baseball_World_Cup"

    def source_row(self) -> dict[str, str]:
        return {
            "source_id": self.id,
            "source_name": self.name,
            "source_type": self.source_type,
            "license_notes": (
                "Historical top4 seeds from local files "
                "data/raw/baseball/wbsc_baseball_world_cup_men_top4_seed.csv, "
                "data/raw/baseball/wbsc_womens_baseball_world_cup_top4_seed.csv, "
                "data/raw/baseball/wbsc_mens_softball_world_cup_top4_seed.csv and "
                "data/raw/baseball/wbsc_womens_softball_world_cup_top4_seed.csv "
                "(curated from WBSC / Wikipedia public information)."
            ),
            "base_url": self.base_url,
        }

    def _local_seed_path(self, seed_file: str) -> Path:
        return Path(__file__).resolve().parents[2] / "data" / "raw" / "baseball" / seed_file

    def fetch(self, season_year: int, out_dir: Path) -> list[Path]:
        del season_year
        raw_paths: list[Path] = []
        seed_sources: dict[str, str] = {}
        for meta in COMPETITIONS.values():
            seed_file = meta["seed_file"]
            local_seed = self._local_seed_path(seed_file)
            if not local_seed.exists():
                raise RuntimeError(f"Missing local seed for WBSC baseball/softball world championships: {local_seed}")

            out_file = out_dir / seed_file
            shutil.copy2(local_seed, out_file)
            raw_paths.append(out_file)
            seed_sources[seed_file] = str(local_seed)

        self._write_json(out_dir / "fetch_meta.json", {"mode": "local_seed", "sources": seed_sources})
        return raw_paths

    def _resolve_country_code(self, country_name: str) -> str:
        alias = COUNTRY_OVERRIDES.get(country_name)
        if alias:
            return alias
        try:
            import pycountry

            country = pycountry.countries.lookup(country_name)
            code = getattr(country, "alpha_3", None)
            if code:
                return code
        except Exception:
            pass
        return slugify(country_name)[:3].upper()

    def parse(self, raw_paths: list[Path], season_year: int) -> dict[str, pd.DataFrame]:
        raw_by_name = {path.name: path for path in raw_paths if path.suffix.lower() == ".csv"}
        parsed_by_competition: dict[str, pd.DataFrame] = {}

        for competition_key, meta in COMPETITIONS.items():
            seed_file = meta["seed_file"]
            seed_csv = raw_by_name.get(seed_file)
            if seed_csv is None:
                raise RuntimeError(f"Missing seed CSV in fetched paths: {seed_file}")

            annual_df = pd.read_csv(seed_csv)
            required_cols = {"year", "rank", "country_name", "event_date"}
            if not required_cols.issubset(set(annual_df.columns)):
                raise RuntimeError(f"Unsupported WBSC seed format for {seed_file}: {list(annual_df.columns)}")

            annual_df["year"] = pd.to_numeric(annual_df["year"], errors="coerce")
            annual_df["rank"] = pd.to_numeric(annual_df["rank"], errors="coerce")
            annual_df["event_date"] = pd.to_datetime(annual_df["event_date"], errors="coerce")
            annual_df["country_name"] = annual_df["country_name"].astype(str).str.replace(r"\[[^\]]+\]", "", regex=True)
            annual_df["country_name"] = annual_df["country_name"].str.strip()
            annual_df = annual_df.dropna(subset=["year", "rank", "country_name", "event_date"])
            annual_df = annual_df.loc[annual_df["year"] <= season_year].copy()
            annual_df["year"] = annual_df["year"].astype(int)
            annual_df["rank"] = annual_df["rank"].astype(int)
            annual_df["event_date"] = annual_df["event_date"].dt.strftime("%Y-%m-%d")
            annual_df = annual_df.loc[annual_df["rank"] <= 4].copy()
            annual_df = annual_df.drop_duplicates(subset=["year", "rank", "country_name"])
            annual_df = annual_df.sort_values(["year", "rank", "country_name"]).reset_index(drop=True)
            parsed_by_competition[competition_key] = annual_df

        if not parsed_by_competition:
            raise RuntimeError("WBSC baseball/softball world championships parsing returned no dataset.")

        timestamp = utc_now_iso()

        sports_rows: dict[str, dict[str, Any]] = {}
        disciplines_rows: dict[str, dict[str, Any]] = {}
        competitions_rows: list[dict[str, Any]] = []
        events_rows: list[dict[str, Any]] = []
        participants_rows: dict[str, dict[str, Any]] = {}
        countries_rows: dict[str, dict[str, Any]] = {}
        results_rows: list[dict[str, Any]] = []

        for competition_key, meta in COMPETITIONS.items():
            annual_df = parsed_by_competition.get(competition_key, pd.DataFrame())
            if annual_df.empty:
                continue

            sport_id = meta["sport_id"]
            sport_name = meta["sport_name"]
            discipline_id = meta["discipline_id"]
            discipline_name = meta["discipline_name"]
            competition_id = meta["competition_id"]

            if sport_id not in sports_rows:
                sports_rows[sport_id] = {
                    "sport_id": sport_id,
                    "sport_name": sport_name,
                    "sport_slug": sport_id,
                    "created_at_utc": timestamp,
                }

            if discipline_id not in disciplines_rows:
                disciplines_rows[discipline_id] = {
                    "discipline_id": discipline_id,
                    "discipline_name": discipline_name,
                    "discipline_slug": discipline_id,
                    "sport_id": sport_id,
                    "confidence": 1.0,
                    "mapping_source": "connector_wbsc_baseball_softball_world_championship_history",
                    "created_at_utc": timestamp,
                }

            competitions_rows.append(
                {
                    "competition_id": competition_id,
                    "sport_id": sport_id,
                    "name": meta["competition_name"],
                    "season_year": None,
                    "level": "national_team_tournament",
                    "start_date": annual_df["event_date"].min(),
                    "end_date": annual_df["event_date"].max(),
                    "source_id": self.id,
                }
            )

            for year, group in annual_df.groupby("year", sort=True):
                event_id = f"{competition_id}_{str(int(year))[-2:]}"
                event_date = group["event_date"].iloc[0]
                events_rows.append(
                    {
                        "event_id": event_id,
                        "competition_id": competition_id,
                        "discipline_id": discipline_id,
                        "gender": meta["gender"],
                        "event_class": meta["event_class"],
                        "event_date": event_date,
                    }
                )

                for _, row in group.sort_values(["rank", "country_name"]).iterrows():
                    country_name = str(row["country_name"]).strip()
                    country_id = self._resolve_country_code(country_name)
                    participant_id = country_id

                    participants_rows[participant_id] = {
                        "participant_id": participant_id,
                        "type": "team",
                        "display_name": country_name,
                        "country_id": country_id,
                    }

                    if country_id not in countries_rows:
                        try:
                            import pycountry

                            country = pycountry.countries.get(alpha_3=country_id)
                        except Exception:
                            country = None
                        countries_rows[country_id] = {
                            "country_id": country_id,
                            "iso2": getattr(country, "alpha_2", None) if country else None,
                            "iso3": country_id,
                            "name_en": getattr(country, "name", country_name) if country else country_name,
                            "name_fr": None,
                        }

                    rank = int(row["rank"])
                    points = {1: 10.0, 2: 7.0, 3: 5.0, 4: 4.0}.get(rank, None)
                    medal = "gold" if rank == 1 else "silver" if rank == 2 else "bronze" if rank == 3 else None
                    results_rows.append(
                        {
                            "event_id": event_id,
                            "participant_id": participant_id,
                            "rank": rank,
                            "medal": medal,
                            "score_raw": f"{meta['score_prefix']}={rank}",
                            "points_awarded": points,
                        }
                    )

        return {
            "countries": pd.DataFrame(countries_rows.values()).drop_duplicates(subset=["country_id"]),
            "sports": pd.DataFrame(sports_rows.values()).drop_duplicates(subset=["sport_id"]),
            "disciplines": pd.DataFrame(disciplines_rows.values()).drop_duplicates(subset=["discipline_id"]),
            "competitions": pd.DataFrame(competitions_rows).drop_duplicates(subset=["competition_id"]),
            "events": pd.DataFrame(events_rows).drop_duplicates(subset=["event_id"]),
            "participants": pd.DataFrame(participants_rows.values()).drop_duplicates(subset=["participant_id"]),
            "results": pd.DataFrame(results_rows).drop_duplicates(subset=["event_id", "participant_id"]),
            "sport_federations": pd.DataFrame(),
        }

    def upsert(self, db: SQLiteDB, payload: dict[str, pd.DataFrame]) -> None:
        countries_df = payload.get("countries", pd.DataFrame())
        participants_df = payload.get("participants", pd.DataFrame())

        with db.connect() as conn:
            existing_country_ids = {
                row[0] for row in conn.execute("SELECT country_id FROM countries").fetchall()
            }
            existing_participant_ids = {
                row[0] for row in conn.execute("SELECT participant_id FROM participants").fetchall()
            }

        if not countries_df.empty:
            countries_df = countries_df.loc[
                ~countries_df["country_id"].isin(existing_country_ids)
            ].copy()
        if not participants_df.empty:
            participants_df = participants_df.loc[
                ~participants_df["participant_id"].isin(existing_participant_ids)
            ].copy()

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
            conn.execute("DELETE FROM disciplines WHERE mapping_source = 'connector_wbsc_baseball_softball_world_championship_history'")
            conn.execute(
                """
                DELETE FROM sports
                WHERE sport_id = 'softball'
                  AND NOT EXISTS (SELECT 1 FROM competitions WHERE sport_id = 'softball')
                  AND NOT EXISTS (SELECT 1 FROM disciplines WHERE sport_id = 'softball')
                  AND NOT EXISTS (SELECT 1 FROM sport_federations WHERE sport_id = 'softball')
                """
            )
            conn.commit()

        db.upsert_dataframe("countries", countries_df, ["country_id"])
        db.upsert_dataframe("sports", payload.get("sports", pd.DataFrame()), ["sport_id"])
        db.upsert_dataframe("disciplines", payload.get("disciplines", pd.DataFrame()), ["discipline_id"])
        db.upsert_dataframe("competitions", payload.get("competitions", pd.DataFrame()), ["competition_id"])
        db.upsert_dataframe("events", payload.get("events", pd.DataFrame()), ["event_id"])
        db.upsert_dataframe("participants", participants_df, ["participant_id"])
        db.upsert_dataframe("results", payload.get("results", pd.DataFrame()), ["event_id", "participant_id"])
