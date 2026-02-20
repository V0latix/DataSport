from __future__ import annotations

import io
import shutil
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from src.core.db import SQLiteDB
from src.core.utils import slugify, utc_now_iso

from .base import Connector


API_BASE = "https://api.wr-rims-prod.pulselive.com/rugby/v3/rankings"
LEGACY_MEN_CSV_URL = "https://raw.githubusercontent.com/dfhampshire/irb_rank_scraper/master/rankings.csv"
SPORTS = {
    "mru": {"gender": "men", "label": "World Rugby Men's Rankings"},
    "wru": {"gender": "women", "label": "World Rugby Women's Rankings"},
}
DEFAULT_START_YEAR = 1990

COUNTRY_ALIASES = {
    "ENG": "ENG",
    "SCO": "SCO",
    "WAL": "WAL",
}
COUNTRY_NAME_ALIASES = {
    "England": "ENG",
    "Scotland": "SCO",
    "Wales": "WAL",
    "Western Samoa": "SAM",
    "Bosnia & Herzegovina": "BIH",
    "Czech Republic": "CZE",
    "USA": "USA",
    "Hong Kong": "HKG",
}


class WorldRugbyRankingHistoryConnector(Connector):
    id = "world_rugby_ranking_history"
    name = "World Rugby Men/Women Rankings History"
    source_type = "api"
    license_notes = (
        "World Rugby rankings API snapshots plus legacy men historical CSV mirror "
        "(respect platform terms and attribution)."
    )
    base_url = API_BASE

    def _local_seed_path(self) -> Path:
        return Path(__file__).resolve().parents[2] / "data" / "raw" / "world_rugby" / "world_rugby_rankings_history.csv"

    def fetch(self, season_year: int, out_dir: Path) -> list[Path]:
        out_file = out_dir / "world_rugby_rankings_history.csv"
        local_seed = self._local_seed_path()
        headers = {"User-Agent": "DataSportPipeline/0.1 (World Rugby rankings fetch)", "Accept": "application/json"}
        rows: list[dict[str, Any]] = []
        errors: list[str] = []
        sources: list[str] = []
        request_count = 0
        try:
            for sport in SPORTS:
                for year in range(DEFAULT_START_YEAR, season_year + 1):
                    request_count += 1
                    date_value = f"{year}-12-31"
                    url = f"{API_BASE}/{sport}"
                    response = requests.get(url, params={"date": date_value}, headers=headers, timeout=60)
                    if response.status_code == 404:
                        continue
                    response.raise_for_status()
                    payload = response.json()

                    effective = payload.get("effective", {})
                    effective_date = str(effective.get("label") or "").strip()
                    if len(effective_date) != 10:
                        continue
                    effective_year = int(effective_date[:4])
                    if effective_year > year:
                        continue

                    entries = payload.get("entries", [])
                    for entry in entries:
                        team = entry.get("team", {}) or {}
                        country_name = str(team.get("name") or "").strip()
                        country_code = str(team.get("countryCode") or team.get("abbreviation") or "").strip().upper()
                        rank_value = entry.get("pos")
                        points_value = entry.get("pts")
                        if not country_name:
                            continue
                        rows.append(
                            {
                                "sport": sport,
                                "requested_year": year,
                                "effective_date": effective_date,
                                "country_name": country_name,
                                "country_code": country_code,
                                "source_rank": rank_value,
                                "points": points_value,
                            }
                        )
            sources.append(API_BASE)
        except Exception as exc:
            errors.append(f"api_fetch_failed: {exc}")

        try:
            legacy_response = requests.get(LEGACY_MEN_CSV_URL, headers=headers, timeout=90)
            legacy_response.raise_for_status()
            legacy_raw = pd.read_csv(io.StringIO(legacy_response.text))
            normalized_columns = {column: column.strip().lower() for column in legacy_raw.columns}
            legacy = legacy_raw.rename(columns=normalized_columns)
            required = {"team", "date", "score"}
            if required.issubset(set(legacy.columns)):
                legacy["effective_date"] = pd.to_datetime(legacy["date"], dayfirst=True, errors="coerce")
                legacy["country_name"] = legacy["team"].astype(str).str.strip()
                legacy["points"] = pd.to_numeric(legacy["score"], errors="coerce")
                legacy = legacy.dropna(subset=["effective_date", "country_name", "points"]).copy()
                legacy["requested_year"] = legacy["effective_date"].dt.year
                legacy = legacy.loc[legacy["requested_year"] <= season_year].copy()
                legacy["effective_date"] = legacy["effective_date"].dt.strftime("%Y-%m-%d")
                legacy = legacy.sort_values(
                    ["effective_date", "points", "country_name"],
                    ascending=[True, False, True],
                )
                legacy["source_rank"] = legacy.groupby("effective_date").cumcount() + 1
                legacy["sport"] = "mru"
                legacy["country_code"] = ""
                rows.extend(
                    legacy[
                        ["sport", "requested_year", "effective_date", "country_name", "country_code", "source_rank", "points"]
                    ].to_dict(orient="records")
                )
                sources.append(LEGACY_MEN_CSV_URL)
            else:
                errors.append("legacy_csv_format_unsupported")
        except Exception as exc:
            errors.append(f"legacy_fetch_failed: {exc}")

        if not rows:
            if local_seed.exists():
                shutil.copy2(local_seed, out_file)
                self._write_json(
                    out_dir / "fetch_meta.json",
                    {
                        "mode": "local_seed_fallback",
                        "source": str(local_seed),
                        "errors": errors,
                    },
                )
                return [out_file]
            raise RuntimeError(f"World Rugby rankings fetch returned no rows. errors={errors}")

        frame = pd.DataFrame(rows).drop_duplicates(
            subset=["sport", "requested_year", "effective_date", "country_name"],
            keep="first",
        )
        frame.to_csv(out_file, index=False)

        local_seed.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(local_seed, index=False)

        self._write_json(
            out_dir / "fetch_meta.json",
            {
                "mode": "api_plus_legacy",
                "sources": sources,
                "requests": request_count,
                "rows": int(len(frame)),
                "sports": sorted(SPORTS),
                "years_requested": {"start": DEFAULT_START_YEAR, "end": season_year},
                "errors": errors,
            },
        )
        return [out_file]

    @staticmethod
    def _medal_from_rank(rank: int | float | None) -> str | None:
        if rank == 1:
            return "gold"
        if rank == 2:
            return "silver"
        if rank == 3:
            return "bronze"
        return None

    def _resolve_country_code(self, country_name: str, country_code: str, known_codes: set[str]) -> str:
        name_alias_code = COUNTRY_NAME_ALIASES.get(str(country_name or "").strip())
        if name_alias_code:
            return name_alias_code

        code = str(country_code or "").strip().upper()
        if len(code) == 3 and code in known_codes:
            return code

        alias_code = COUNTRY_ALIASES.get(code)
        if alias_code:
            return alias_code

        try:
            import pycountry

            country = pycountry.countries.lookup(country_name)
            candidate = getattr(country, "alpha_3", None)
            if candidate:
                return candidate
        except Exception:
            pass
        return code if len(code) == 3 else slugify(country_name)[:3].upper()

    def parse(self, raw_paths: list[Path], season_year: int) -> dict[str, pd.DataFrame]:
        csv_path = next(path for path in raw_paths if path.name.endswith(".csv"))
        frame = pd.read_csv(csv_path)

        required_cols = {
            "sport",
            "requested_year",
            "effective_date",
            "country_name",
            "country_code",
            "source_rank",
            "points",
        }
        if not required_cols.issubset(set(frame.columns)):
            raise RuntimeError(f"Unsupported World Rugby CSV format with columns: {list(frame.columns)}")

        frame["requested_year"] = pd.to_numeric(frame["requested_year"], errors="coerce")
        frame["source_rank"] = pd.to_numeric(frame["source_rank"], errors="coerce")
        frame["points"] = pd.to_numeric(frame["points"], errors="coerce")
        frame["effective_date"] = pd.to_datetime(frame["effective_date"], errors="coerce")
        frame = frame.dropna(subset=["sport", "requested_year", "effective_date", "country_name"])
        frame = frame.loc[frame["requested_year"] <= season_year].copy()
        frame["requested_year"] = frame["requested_year"].astype(int)

        selected_dates = frame.groupby(["sport", "requested_year"], as_index=False)["effective_date"].max()
        selected_dates = selected_dates.rename(columns={"effective_date": "selected_effective_date"})
        annual = frame.merge(selected_dates, on=["sport", "requested_year"], how="inner")
        annual = annual.loc[annual["effective_date"] == annual["selected_effective_date"]].copy()
        annual = annual.sort_values(
            ["sport", "requested_year", "source_rank", "points", "country_name"],
            ascending=[True, True, True, False, True],
            na_position="last",
        )
        annual = annual.groupby(["sport", "requested_year"], as_index=False).head(10).reset_index(drop=True)

        if annual.empty:
            raise RuntimeError("World Rugby ranking annual top 10 generation returned zero rows.")

        timestamp = utc_now_iso()
        sport_id = slugify("Rugby")

        sports_df = pd.DataFrame(
            [
                {
                    "sport_id": sport_id,
                    "sport_name": "Rugby",
                    "sport_slug": sport_id,
                    "created_at_utc": timestamp,
                }
            ]
        )
        discipline_rows = []
        for sport_code, meta in SPORTS.items():
            discipline_name = f"World Rugby {meta['gender'].title()} Ranking"
            discipline_rows.append(
                {
                    "discipline_id": slugify(discipline_name),
                    "discipline_name": discipline_name,
                    "discipline_slug": slugify(discipline_name),
                    "sport_id": sport_id,
                    "confidence": 1.0,
                    "mapping_source": "connector_world_rugby_ranking_history",
                    "created_at_utc": timestamp,
                    "sport_code": sport_code,
                }
            )
        discipline_lookup = {
            row["sport_code"]: {k: v for k, v in row.items() if k != "sport_code"} for row in discipline_rows
        }
        disciplines_df = pd.DataFrame([{k: v for k, v in row.items() if k != "sport_code"} for row in discipline_rows])

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

        for sport_code, meta in SPORTS.items():
            subset = annual.loc[annual["sport"] == sport_code].copy()
            if subset.empty:
                continue

            competition_id = f"world_rugby_{meta['gender']}_ranking"
            min_date = subset["effective_date"].min().strftime("%Y-%m-%d")
            max_date = subset["effective_date"].max().strftime("%Y-%m-%d")
            competitions_rows.append(
                {
                    "competition_id": competition_id,
                    "sport_id": sport_id,
                    "name": meta["label"],
                    "season_year": None,
                    "level": "national_team_ranking",
                    "start_date": min_date,
                    "end_date": max_date,
                    "source_id": self.id,
                }
            )

            for ranking_year, group in subset.groupby("requested_year", sort=True):
                rank_date = group["effective_date"].max()
                date_text = rank_date.strftime("%Y-%m-%d")
                event_id = f"world_rugby_{meta['gender']}_ranking_{str(int(ranking_year))[-2:]}"

                events_rows.append(
                    {
                        "event_id": event_id,
                        "competition_id": competition_id,
                        "discipline_id": discipline_lookup[sport_code]["discipline_id"],
                        "gender": meta["gender"],
                        "event_class": "ranking_release_top10",
                        "event_date": date_text,
                    }
                )

                sorted_group = group.sort_values(
                    ["source_rank", "points", "country_name"],
                    ascending=[True, False, True],
                    na_position="last",
                ).reset_index(drop=True)
                top10 = sorted_group.head(10).copy()
                for position, row in enumerate(top10.itertuples(index=False), start=1):
                    country_name = str(getattr(row, "country_name", "") or "").strip()
                    source_code = str(getattr(row, "country_code", "") or "").strip().upper()
                    country_id = self._resolve_country_code(country_name, source_code, known_country_codes)
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

                    points = getattr(row, "points", None)
                    results_rows.append(
                        {
                            "event_id": event_id,
                            "participant_id": participant_id,
                            "rank": position,
                            "medal": self._medal_from_rank(position),
                            "score_raw": f"world_rugby_points={points}",
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
            conn.execute("DELETE FROM disciplines WHERE mapping_source = 'connector_world_rugby_ranking_history'")
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
