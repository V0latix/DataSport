from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import pandas as pd

from src.core.db import SQLiteDB
from src.core.utils import slugify, utc_now_iso

from .base import Connector


SOURCE_URLS = [
    "https://raw.githubusercontent.com/Dato-Futbol/fifa-ranking/master/ranking_fifa_historical.csv",
    "https://raw.githubusercontent.com/cnc8/fifa-world-ranking/master/fifa_ranking-2020-12-10.csv",
]

COUNTRY_ALIASES = {
    "IR Iran": "IRN",
    "Korea Republic": "KOR",
    "Korea DPR": "PRK",
    "China PR": "CHN",
    "Congo DR": "COD",
    "Cape Verde Islands": "CPV",
    "Curacao": "CUW",
    "Czech Republic": "CZE",
    "Kyrgyz Republic": "KGZ",
    "Chinese Taipei": "TWN",
    "Brunei Darussalam": "BRN",
    "Republic of Ireland": "IRL",
    "United States": "USA",
    "USA": "USA",
    "Vietnam": "VNM",
}


class FifaRankingHistoryConnector(Connector):
    id = "fifa_ranking_history"
    name = "FIFA Men's Ranking Historical CSV"
    source_type = "csv"
    license_notes = (
        "Open-source mirror of FIFA ranking history (source repository attribution required; "
        "validate terms for redistribution)."
    )
    base_url = SOURCE_URLS[0]

    def _local_seed_path(self) -> Path:
        base = Path(__file__).resolve().parents[2] / "data" / "raw" / "fifa"
        preferred = [
            base / "fifa_ranking_historical_2024.csv",
            base / "fifa_ranking_full.csv",
        ]
        for path in preferred:
            if path.exists():
                return path
        return preferred[0]

    def fetch(self, season_year: int, out_dir: Path) -> list[Path]:
        del season_year
        out_file = out_dir / "fifa_ranking_full.csv"
        local_seed = self._local_seed_path()
        if local_seed.exists():
            shutil.copy2(local_seed, out_file)
            self._write_json(out_dir / "fetch_meta.json", {"mode": "local_seed", "source": str(local_seed)})
            return [out_file]

        last_error: Exception | None = None
        for url in SOURCE_URLS:
            try:
                import requests

                headers = {"User-Agent": "DataSportPipeline/0.1 (FIFA ranking history fetch)"}
                with requests.get(url, headers=headers, timeout=120) as response:
                    response.raise_for_status()
                    out_file.write_bytes(response.content)
                self._write_json(out_dir / "fetch_meta.json", {"mode": "download", "source": url})
                return [out_file]
            except Exception as exc:
                last_error = exc

        raise RuntimeError(f"Unable to fetch FIFA ranking source. Last error: {last_error}")

    @staticmethod
    def _medal_from_rank(rank: int | float | None) -> str | None:
        if rank == 1:
            return "gold"
        if rank == 2:
            return "silver"
        if rank == 3:
            return "bronze"
        return None

    def _resolve_country_code(
        self,
        country_name: str,
        country_abrv: str,
        known_codes: set[str],
    ) -> str:
        code = str(country_abrv or "").strip().upper()
        if len(code) == 3 and code in known_codes:
            return code

        alias_code = COUNTRY_ALIASES.get(country_name)
        if alias_code:
            return alias_code

        try:
            import pycountry

            country = pycountry.countries.lookup(country_name)
            candidate = getattr(country, "alpha_3", None)
            if candidate and candidate in known_codes:
                return candidate
            if candidate:
                return candidate
        except Exception:
            pass
        return code if len(code) == 3 else slugify(country_name)[:3].upper()

    def parse(self, raw_paths: list[Path], season_year: int) -> dict[str, pd.DataFrame]:
        csv_path = next(path for path in raw_paths if path.name.endswith(".csv"))
        raw = pd.read_csv(csv_path)
        if {"country_full", "country_abrv", "total_points", "rank_date"}.issubset(set(raw.columns)):
            frame = raw.rename(
                columns={
                    "country_full": "country_name",
                    "country_abrv": "country_code",
                    "rank_date": "rank_date",
                    "total_points": "total_points",
                    "rank": "source_rank",
                }
            )
        elif {"team", "team_short", "total_points", "date"}.issubset(set(raw.columns)):
            frame = raw.rename(
                columns={
                    "team": "country_name",
                    "team_short": "country_code",
                    "date": "rank_date",
                    "total_points": "total_points",
                }
            )
            frame["source_rank"] = None
        else:
            raise RuntimeError(f"Unsupported FIFA CSV format with columns: {list(raw.columns)}")

        frame["source_rank"] = pd.to_numeric(frame["source_rank"], errors="coerce")
        frame["total_points"] = pd.to_numeric(frame["total_points"], errors="coerce")
        frame["rank_date"] = pd.to_datetime(frame["rank_date"], errors="coerce")
        frame = frame.dropna(subset=["rank_date", "country_name", "country_code"])
        frame = frame.loc[frame["rank_date"].dt.year <= season_year].copy()
        frame["ranking_year"] = frame["rank_date"].dt.year

        latest_dates = frame.groupby("ranking_year", as_index=False)["rank_date"].max()
        latest_dates = latest_dates.rename(columns={"rank_date": "selected_rank_date"})
        annual = frame.merge(latest_dates, on="ranking_year", how="inner")
        annual = annual.loc[annual["rank_date"] == annual["selected_rank_date"]].copy()
        annual = annual.sort_values(
            ["ranking_year", "source_rank", "total_points", "country_name"],
            ascending=[True, True, False, True],
            na_position="last",
        )
        annual = annual.groupby("ranking_year", as_index=False).head(10).reset_index(drop=True)

        timestamp = utc_now_iso()
        sport_id = slugify("Football")
        discipline_name = "FIFA Men Ranking"
        discipline_id = slugify(discipline_name)
        competition_id = "fifa_ranking"

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
                    "discipline_name": discipline_name,
                    "discipline_slug": discipline_id,
                    "sport_id": sport_id,
                    "confidence": 1.0,
                    "mapping_source": "connector_fifa_ranking_history",
                    "created_at_utc": timestamp,
                }
            ]
        )

        known_country_codes: set[str] = set()
        pycountry_by_iso3: dict[str, Any] = {}
        try:
            import pycountry

            known_country_codes = {country.alpha_3 for country in pycountry.countries if hasattr(country, "alpha_3")}
            pycountry_by_iso3 = {
                country.alpha_3: country for country in pycountry.countries if hasattr(country, "alpha_3")
            }
        except Exception:
            pass

        competitions_rows: list[dict[str, Any]] = []
        events_rows: list[dict[str, Any]] = []
        participants_rows: dict[str, dict[str, Any]] = {}
        results_rows: list[dict[str, Any]] = []
        countries_rows: dict[str, dict[str, Any]] = {}

        min_date = annual["rank_date"].min().strftime("%Y-%m-%d")
        max_date = annual["rank_date"].max().strftime("%Y-%m-%d")
        competitions_rows.append(
            {
                "competition_id": competition_id,
                "sport_id": sport_id,
                "name": "FIFA Men's Ranking",
                "season_year": None,
                "level": "national_team_ranking",
                "start_date": min_date,
                "end_date": max_date,
                "source_id": self.id,
            }
        )

        for ranking_year, group in annual.groupby("ranking_year", sort=True):
            rank_date = group["rank_date"].iloc[0]
            date_text = rank_date.strftime("%Y-%m-%d")
            event_id = f"fifa_ranking_{str(int(ranking_year))[-2:]}"

            events_rows.append(
                {
                    "event_id": event_id,
                    "competition_id": competition_id,
                    "discipline_id": discipline_id,
                    "gender": "men",
                    "event_class": "ranking_release_top10",
                    "event_date": date_text,
                }
            )

            sorted_group = group.sort_values(
                ["source_rank", "total_points", "country_name"],
                ascending=[True, False, True],
                na_position="last",
            ).reset_index(drop=True)
            for position, row in enumerate(sorted_group.itertuples(index=False), start=1):
                country_name = str(getattr(row, "country_name", "") or "").strip()
                abrv = str(getattr(row, "country_code", "") or "").strip()
                country_id = self._resolve_country_code(country_name, abrv, known_country_codes)
                participant_id = country_id
                participants_rows[participant_id] = {
                    "participant_id": participant_id,
                    "type": "team",
                    "display_name": country_name,
                    "country_id": country_id,
                }

                if country_id not in countries_rows:
                    known_country = pycountry_by_iso3.get(country_id)
                    if known_country:
                        iso2 = getattr(known_country, "alpha_2", None)
                        iso3 = getattr(known_country, "alpha_3", country_id)
                        country_label = getattr(known_country, "name", country_name)
                    else:
                        iso2 = None
                        iso3 = country_id
                        country_label = country_name
                    countries_rows[country_id] = {
                        "country_id": country_id,
                        "iso2": iso2,
                        "iso3": iso3,
                        "name_en": country_label,
                        "name_fr": None,
                    }

                rank_value = position
                points = getattr(row, "total_points", None)
                results_rows.append(
                    {
                        "event_id": event_id,
                        "participant_id": participant_id,
                        "rank": rank_value,
                        "medal": self._medal_from_rank(rank_value),
                        "score_raw": f"fifa_points={points}",
                        "points_awarded": float(points) if pd.notna(points) else None,
                    }
                )

        payload = {
            "countries": pd.DataFrame(countries_rows.values()).drop_duplicates(subset=["country_id"]),
            "sports": sports_df,
            "disciplines": disciplines_df,
            "competitions": pd.DataFrame(competitions_rows).drop_duplicates(subset=["competition_id"]),
            "events": pd.DataFrame(events_rows).drop_duplicates(subset=["event_id"]),
            "participants": pd.DataFrame(participants_rows.values()).drop_duplicates(subset=["participant_id"]),
            "results": pd.DataFrame(results_rows).drop_duplicates(subset=["event_id", "participant_id"]),
            "sport_federations": pd.DataFrame(),
        }
        return payload

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
            conn.execute("DELETE FROM participants WHERE participant_id LIKE ?", (f"{self.id}_%",))
            conn.execute("DELETE FROM disciplines WHERE mapping_source = 'connector_fifa_ranking_history'")
            conn.commit()

        db.upsert_dataframe("countries", payload.get("countries", pd.DataFrame()), ["country_id"])
        db.upsert_dataframe("sports", payload.get("sports", pd.DataFrame()), ["sport_id"])
        db.upsert_dataframe("disciplines", payload.get("disciplines", pd.DataFrame()), ["discipline_id"])
        db.upsert_dataframe("competitions", payload.get("competitions", pd.DataFrame()), ["competition_id"])
        db.upsert_dataframe("events", payload.get("events", pd.DataFrame()), ["event_id"])
        db.upsert_dataframe("participants", payload.get("participants", pd.DataFrame()), ["participant_id"])
        db.upsert_dataframe("results", payload.get("results", pd.DataFrame()), ["event_id", "participant_id"])
        with db.connect() as conn:
            conn.execute(
                """
                DELETE FROM countries
                WHERE iso2 IS NULL
                  AND country_id NOT IN (
                      SELECT DISTINCT country_id
                      FROM participants
                      WHERE country_id IS NOT NULL
                  )
                """
            )
            conn.commit()
