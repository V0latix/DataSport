from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import pandas as pd

from src.core.db import SQLiteDB
from src.core.utils import slugify, utc_now_iso

from .base import Connector


COUNTRY_OVERRIDES = {
    "United States": "USA",
    "USA": "USA",
    "England": "ENG",
    "Wales": "WAL",
    "Scotland": "SCO",
    "Northern Ireland": "NIR",
    "Netherlands": "NLD",
    "China PR": "CHN",
}


class FifaWomenWorldCupHistoryConnector(Connector):
    id = "fifa_women_world_cup_history"
    name = "FIFA Women's World Cup Historical Results"
    source_type = "csv"
    license_notes = (
        "Historical top4 seed curated from public FIFA / Wikipedia references. "
        "Verify downstream redistribution requirements."
    )
    base_url = "https://en.wikipedia.org/wiki/FIFA_Women%27s_World_Cup"

    def source_row(self) -> dict[str, str]:
        return {
            "source_id": self.id,
            "source_name": self.name,
            "source_type": self.source_type,
            "license_notes": (
                "Historical top4 seed from local file data/raw/world_cup/womens_world_cup_top4_seed.csv "
                "(curated from FIFA / Wikipedia public information)."
            ),
            "base_url": self.base_url,
        }

    def _local_seed_path(self) -> Path:
        return Path(__file__).resolve().parents[2] / "data" / "raw" / "world_cup" / "womens_world_cup_top4_seed.csv"

    def fetch(self, season_year: int, out_dir: Path) -> list[Path]:
        del season_year
        local_seed = self._local_seed_path()
        if not local_seed.exists():
            raise RuntimeError(f"Missing local seed for women's world cup history: {local_seed}")
        out_file = out_dir / "womens_world_cup_top4_seed.csv"
        shutil.copy2(local_seed, out_file)
        self._write_json(out_dir / "fetch_meta.json", {"mode": "local_seed", "source": str(local_seed)})
        return [out_file]

    def _resolve_country_code(self, country_name: str) -> str:
        if country_name in COUNTRY_OVERRIDES:
            return COUNTRY_OVERRIDES[country_name]
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
        seed_csv = next((path for path in raw_paths if path.suffix.lower() == ".csv"), None)
        if seed_csv is None:
            raise RuntimeError("No seed CSV found for women's world cup history.")

        annual_df = pd.read_csv(seed_csv)
        required_cols = {"year", "rank", "country_name", "event_date"}
        if not required_cols.issubset(set(annual_df.columns)):
            raise RuntimeError(f"Unsupported women world cup seed format. columns={list(annual_df.columns)}")

        annual_df["year"] = pd.to_numeric(annual_df["year"], errors="coerce")
        annual_df["rank"] = pd.to_numeric(annual_df["rank"], errors="coerce")
        annual_df["event_date"] = pd.to_datetime(annual_df["event_date"], errors="coerce")
        annual_df = annual_df.dropna(subset=["year", "rank", "country_name", "event_date"])
        annual_df = annual_df.loc[annual_df["year"] <= season_year].copy()
        annual_df["year"] = annual_df["year"].astype(int)
        annual_df["rank"] = annual_df["rank"].astype(int)
        annual_df["event_date"] = annual_df["event_date"].dt.strftime("%Y-%m-%d")
        annual_df = annual_df.drop_duplicates(subset=["year", "rank", "country_name"])
        annual_df = annual_df.sort_values(["year", "rank", "country_name"]).reset_index(drop=True)
        annual_df = annual_df.loc[annual_df["rank"] <= 4].copy()

        timestamp = utc_now_iso()
        sport_id = slugify("Football")
        discipline_id = sport_id
        competition_id = "fifa_womens_world_cup"

        sports_df = pd.DataFrame(
            [
                {
                    "sport_id": sport_id,
                    "sport_name": "Football",
                    "sport_slug": sport_id,
                    "created_at_utc": timestamp,
                }
            ]
        )
        disciplines_df = pd.DataFrame(
            [
                {
                    "discipline_id": discipline_id,
                    "discipline_name": "Football",
                    "discipline_slug": discipline_id,
                    "sport_id": sport_id,
                    "confidence": 1.0,
                    "mapping_source": "connector_fifa_women_world_cup_history",
                    "created_at_utc": timestamp,
                }
            ]
        )

        min_date = annual_df["event_date"].min()
        max_date = annual_df["event_date"].max()
        competitions_df = pd.DataFrame(
            [
                {
                    "competition_id": competition_id,
                    "sport_id": sport_id,
                    "name": "FIFA Women's World Cup",
                    "season_year": None,
                    "level": "national_team_tournament",
                    "start_date": min_date,
                    "end_date": max_date,
                    "source_id": self.id,
                }
            ]
        )

        events_rows: list[dict[str, Any]] = []
        participants_rows: dict[str, dict[str, Any]] = {}
        countries_rows: dict[str, dict[str, Any]] = {}
        results_rows: list[dict[str, Any]] = []

        for year, group in annual_df.groupby("year", sort=True):
            event_id = f"fifa_womens_world_cup_{str(int(year))[-2:]}"
            event_date = group["event_date"].iloc[0]
            events_rows.append(
                {
                    "event_id": event_id,
                    "competition_id": competition_id,
                    "discipline_id": discipline_id,
                    "gender": "women",
                    "event_class": "final_ranking_top4",
                    "event_date": event_date,
                }
            )
            for _, row in group.sort_values("rank").iterrows():
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
                        "score_raw": f"womens_world_cup_final_rank={rank}",
                        "points_awarded": points,
                    }
                )

        return {
            "countries": pd.DataFrame(countries_rows.values()).drop_duplicates(subset=["country_id"]),
            "sports": sports_df,
            "disciplines": disciplines_df,
            "competitions": competitions_df,
            "events": pd.DataFrame(events_rows).drop_duplicates(subset=["event_id"]),
            "participants": pd.DataFrame(participants_rows.values()).drop_duplicates(subset=["participant_id"]),
            "results": pd.DataFrame(results_rows).drop_duplicates(subset=["event_id", "participant_id"]),
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
            conn.execute("DELETE FROM disciplines WHERE mapping_source = 'connector_fifa_women_world_cup_history'")
            conn.commit()

        db.upsert_dataframe("countries", payload.get("countries", pd.DataFrame()), ["country_id"])
        db.upsert_dataframe("sports", payload.get("sports", pd.DataFrame()), ["sport_id"])
        db.upsert_dataframe("disciplines", payload.get("disciplines", pd.DataFrame()), ["discipline_id"])
        db.upsert_dataframe("competitions", payload.get("competitions", pd.DataFrame()), ["competition_id"])
        db.upsert_dataframe("events", payload.get("events", pd.DataFrame()), ["event_id"])
        db.upsert_dataframe("participants", payload.get("participants", pd.DataFrame()), ["participant_id"])
        db.upsert_dataframe("results", payload.get("results", pd.DataFrame()), ["event_id", "participant_id"])
