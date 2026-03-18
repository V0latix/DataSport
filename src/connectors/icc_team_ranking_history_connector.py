from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.core.db import SQLiteDB
from src.core.utils import slugify, utc_now_iso

from .base import Connector


API_BASE = "https://assets-icc.sportz.io"
RANKING_API_PATH = "/cricket/v1/ranking"
CLIENT_ID = "tPZJbRgIub3Vua93/DWtyQ=="
HISTORY_START_YEAR = 2000

COMPETITIONS: dict[str, dict[str, str]] = {
    "test": {
        "comp_type": "test",
        "competition_id": "icc_men_test_team_ranking",
        "competition_name": "ICC Men's Test Team Ranking",
        "discipline_id": "cricket-test",
        "discipline_name": "Cricket Test",
        "gender": "men",
        "history_start_year": "2000",
    },
    "odi": {
        "comp_type": "odi",
        "competition_id": "icc_men_odi_team_ranking",
        "competition_name": "ICC Men's ODI Team Ranking",
        "discipline_id": "cricket-odi",
        "discipline_name": "Cricket ODI",
        "gender": "men",
        "history_start_year": "2000",
    },
    "t20": {
        "comp_type": "t20",
        "competition_id": "icc_men_t20i_team_ranking",
        "competition_name": "ICC Men's T20I Team Ranking",
        "discipline_id": "cricket-t20",
        "discipline_name": "Cricket T20",
        "gender": "men",
        "history_start_year": "2011",
    },
    "odiw": {
        "comp_type": "odiw",
        "competition_id": "icc_women_odi_team_ranking",
        "competition_name": "ICC Women's ODI Team Ranking",
        "discipline_id": "cricket-odi",
        "discipline_name": "Cricket ODI",
        "gender": "women",
        "history_start_year": "2018",
    },
    "t20w": {
        "comp_type": "t20w",
        "competition_id": "icc_women_t20i_team_ranking",
        "competition_name": "ICC Women's T20I Team Ranking",
        "discipline_id": "cricket-t20",
        "discipline_name": "Cricket T20",
        "gender": "women",
        "history_start_year": "2018",
    },
}

COUNTRY_NAME_ALIASES = {
    "England": "ENG",
    "Scotland": "SCO",
    "Wales": "WAL",
    "West Indies": "WIS",
    "United Arab Emirates": "ARE",
    "USA": "USA",
    "Hong Kong": "HKG",
    "Korea": "KOR",
    "Czechia": "CZE",
}

COUNTRY_CODE_ALIASES = {
    "WI": "WIS",
    "SA": "ZAF",
    "NZ": "NZL",
    "SL": "LKA",
    "BAN": "BGD",
    "UAE": "ARE",
    "HK": "HKG",
    "TL": "TLS",
}


class IccTeamRankingHistoryConnector(Connector):
    id = "icc_team_ranking_history"
    name = "ICC Team Rankings (Men Test/ODI/T20I + Women ODI/T20I)"
    source_type = "api"
    license_notes = (
        "Live ICC team rankings snapshots from ICC website API payloads. "
        "Verify downstream redistribution requirements."
    )
    base_url = "https://www.icc-cricket.com/rankings/team-rankings/mens/test"

    def source_row(self) -> dict[str, str]:
        return {
            "source_id": self.id,
            "source_name": self.name,
            "source_type": self.source_type,
            "license_notes": (
                "ICC rankings fetched from assets-icc.sportz.io ranking endpoint "
                "(comp_type=test|odi|t20|odiw|t20w, type=team) "
                "with client_id extracted from ICC rankings pages. "
                "Historical snapshots are requested per year with date=YYYY1231 "
                f"from {HISTORY_START_YEAR} to --year. "
                "Only top 10 is persisted in competition results."
            ),
            "base_url": self.base_url,
        }

    def _local_seed_path(self) -> Path:
        return Path(__file__).resolve().parents[2] / "data" / "raw" / "cricket" / "icc_team_rankings_history_seed.csv"

    @staticmethod
    def _extract_rank_block(payload: dict[str, Any]) -> dict[str, Any] | None:
        data = payload.get("data")
        if not isinstance(data, dict):
            return None
        for value in data.values():
            if isinstance(value, dict) and isinstance(value.get("rank"), list):
                return value
        return None

    def fetch(self, season_year: int, out_dir: Path) -> list[Path]:
        out_file = out_dir / "icc_team_rankings_history_seed.csv"
        local_seed = self._local_seed_path()
        rows: list[dict[str, Any]] = []
        errors: list[str] = []
        max_supported_year = datetime.utcnow().year
        end_year = min(int(season_year), max_supported_year)
        year_end_suffixes = ["1231", "1230", "1229", "1228", "1227", "1226", "1225", "1224", "1223", "1222", "1221", "1220"]

        for ranking_code, meta in COMPETITIONS.items():
            start_year = max(HISTORY_START_YEAR, int(meta.get("history_start_year", HISTORY_START_YEAR)))
            for ranking_year in range(start_year, end_year + 1):
                try:
                    rank_block = None
                    request_errors: list[str] = []
                    for suffix in year_end_suffixes:
                        try:
                            payload = self._request_json(
                                f"{API_BASE}{RANKING_API_PATH}",
                                params={
                                    "client_id": CLIENT_ID,
                                    "comp_type": meta["comp_type"],
                                    "lang": "en",
                                    "feed_format": "json",
                                    "type": "team",
                                    # ICC historical snapshots are exposed via YYYYMMDD.
                                    "date": f"{ranking_year}{suffix}",
                                },
                                timeout=30,
                                retries=1,
                            )
                        except Exception as req_exc:
                            request_errors.append(f"{suffix}:{req_exc}")
                            continue
                        rank_block = self._extract_rank_block(payload)
                        if rank_block:
                            break
                    if not rank_block:
                        if request_errors:
                            errors.append(
                                f"{ranking_code}:{ranking_year}:no_snapshot_after_fallback:{' | '.join(request_errors)}"
                            )
                        continue

                    last_updated = str(rank_block.get("last_updated") or "").strip()
                    rank_type = str(rank_block.get("rank-type") or "").strip()
                    entries = rank_block.get("rank") or []
                    if not isinstance(entries, list) or not entries:
                        continue
                    rank_date = str(rank_block.get("rank_date") or "").strip()[:10]
                    if len(rank_date) != 10:
                        rank_date = str(entries[0].get("rankdate") or "").strip()[:10]
                    if len(rank_date) != 10:
                        continue

                    for entry in entries:
                        country_name = str(entry.get("Country") or "").strip()
                        source_rank = entry.get("no")
                        if not country_name or source_rank is None:
                            continue

                        rows.append(
                            {
                                "ranking_code": ranking_code,
                                "comp_type": meta["comp_type"],
                                "effective_date": rank_date,
                                "last_updated": last_updated,
                                "rank_type": rank_type,
                                "country_name": country_name,
                                "country_code_source": str(entry.get("shortname") or "").strip().upper(),
                                "source_rank": source_rank,
                                "matches": entry.get("Matches"),
                                "points": entry.get("Points"),
                                "rating": entry.get("Rating"),
                            }
                        )
                except Exception as exc:
                    errors.append(f"{ranking_code}:{ranking_year}:fetch_failed:{exc}")

        if not rows:
            if not local_seed.exists():
                raise RuntimeError(f"ICC team ranking fetch returned no rows and no local seed exists. errors={errors}")
            shutil.copy2(local_seed, out_file)
            self._write_json(
                out_dir / "fetch_meta.json",
                {"mode": "local_seed_fallback", "source": str(local_seed), "errors": errors},
            )
            return [out_file]

        frame = pd.DataFrame(rows)
        if local_seed.exists():
            try:
                previous = pd.read_csv(local_seed)
                frame = pd.concat([previous, frame], ignore_index=True, sort=False)
            except Exception:
                pass

        frame["ranking_code"] = frame["ranking_code"].astype(str).str.strip().str.lower()
        frame["effective_date"] = frame["effective_date"].astype(str).str.strip().str[:10]
        frame["country_name"] = frame["country_name"].astype(str).str.strip()
        frame["country_code_source"] = frame["country_code_source"].astype(str).str.strip().str.upper()
        frame["source_rank"] = pd.to_numeric(frame["source_rank"], errors="coerce")
        frame["matches"] = pd.to_numeric(frame["matches"], errors="coerce")
        frame["points"] = pd.to_numeric(frame["points"], errors="coerce")
        frame["rating"] = pd.to_numeric(frame["rating"], errors="coerce")
        frame = frame.dropna(subset=["ranking_code", "effective_date", "country_name", "source_rank"])

        frame = frame.drop_duplicates(
            subset=["ranking_code", "effective_date", "country_name", "source_rank"],
            keep="last",
        ).reset_index(drop=True)
        frame.to_csv(out_file, index=False)
        local_seed.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(local_seed, index=False)

        self._write_json(
            out_dir / "fetch_meta.json",
            {
                "mode": "icc_api",
                "rows": int(len(frame)),
                "competitions": sorted(COMPETITIONS.keys()),
                "api_base": API_BASE,
                "history_start_year": HISTORY_START_YEAR,
                "history_end_year": end_year,
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

    def _resolve_country_code(self, country_name: str, source_code: str, known_codes: set[str]) -> str:
        name_alias = COUNTRY_NAME_ALIASES.get(str(country_name or "").strip())
        if name_alias:
            return name_alias

        source = str(source_code or "").strip().upper()
        source = COUNTRY_CODE_ALIASES.get(source, source)
        if len(source) == 3 and source in known_codes:
            return source

        try:
            import pycountry

            country = pycountry.countries.lookup(country_name)
            candidate = getattr(country, "alpha_3", None)
            if candidate:
                return candidate
        except Exception:
            pass
        return source if len(source) == 3 else slugify(country_name)[:3].upper()

    def parse(self, raw_paths: list[Path], season_year: int) -> dict[str, pd.DataFrame]:
        csv_path = next(path for path in raw_paths if path.name.endswith(".csv"))
        frame = pd.read_csv(csv_path)

        required_cols = {
            "ranking_code",
            "comp_type",
            "effective_date",
            "country_name",
            "country_code_source",
            "source_rank",
            "matches",
            "points",
            "rating",
        }
        if not required_cols.issubset(set(frame.columns)):
            raise RuntimeError(f"Unsupported ICC team rankings CSV format with columns: {list(frame.columns)}")

        frame["effective_date"] = pd.to_datetime(frame["effective_date"], errors="coerce")
        frame["source_rank"] = pd.to_numeric(frame["source_rank"], errors="coerce")
        frame["matches"] = pd.to_numeric(frame["matches"], errors="coerce")
        frame["points"] = pd.to_numeric(frame["points"], errors="coerce")
        frame["rating"] = pd.to_numeric(frame["rating"], errors="coerce")
        frame["ranking_code"] = frame["ranking_code"].astype(str).str.strip().str.lower()
        frame = frame.dropna(subset=["ranking_code", "effective_date", "country_name", "source_rank"])
        frame = frame.loc[frame["ranking_code"].isin(set(COMPETITIONS.keys()))].copy()
        frame = frame.sort_values(
            ["ranking_code", "effective_date", "country_name", "source_rank"],
            ascending=[True, True, True, True],
            na_position="last",
        ).drop_duplicates(subset=["ranking_code", "effective_date", "country_name"], keep="first")
        frame["ranking_year"] = frame["effective_date"].dt.year.astype(int)
        frame = frame.loc[frame["ranking_year"] <= season_year].copy()

        selected_dates = frame.groupby(["ranking_code", "ranking_year"], as_index=False)["effective_date"].max()
        selected_dates = selected_dates.rename(columns={"effective_date": "selected_effective_date"})
        annual = frame.merge(selected_dates, on=["ranking_code", "ranking_year"], how="inner")
        annual = annual.loc[annual["effective_date"] == annual["selected_effective_date"]].copy()
        annual = annual.sort_values(
            ["ranking_code", "ranking_year", "source_rank", "rating", "points", "country_name"],
            ascending=[True, True, True, False, False, True],
            na_position="last",
        )
        annual = annual.groupby(["ranking_code", "ranking_year"], as_index=False).head(10).reset_index(drop=True)

        if annual.empty:
            raise RuntimeError("ICC team ranking annual top10 generation returned zero rows.")

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

        timestamp = utc_now_iso()
        sport_id = slugify("Cricket")
        sports_df = pd.DataFrame(
            [
                {
                    "sport_id": sport_id,
                    "sport_name": "Cricket",
                    "sport_slug": sport_id,
                    "created_at_utc": timestamp,
                }
            ]
        )

        discipline_rows: list[dict[str, Any]] = []
        for ranking_code, meta in COMPETITIONS.items():
            discipline_rows.append(
                {
                    "discipline_id": meta["discipline_id"],
                    "discipline_name": meta["discipline_name"],
                    "discipline_slug": meta["discipline_id"],
                    "sport_id": sport_id,
                    "confidence": 1.0,
                    "mapping_source": "connector_icc_team_ranking_history",
                    "created_at_utc": timestamp,
                    "ranking_code": ranking_code,
                }
            )
        discipline_lookup = {
            row["ranking_code"]: {k: v for k, v in row.items() if k != "ranking_code"} for row in discipline_rows
        }
        disciplines_df = pd.DataFrame([{k: v for k, v in row.items() if k != "ranking_code"} for row in discipline_rows])

        competitions_rows: list[dict[str, Any]] = []
        events_rows: list[dict[str, Any]] = []
        participants_rows: dict[str, dict[str, Any]] = {}
        countries_rows: dict[str, dict[str, Any]] = {}
        results_rows: list[dict[str, Any]] = []

        for ranking_code, meta in COMPETITIONS.items():
            subset = annual.loc[annual["ranking_code"] == ranking_code].copy()
            if subset.empty:
                continue

            competition_id = meta["competition_id"]
            min_date = subset["effective_date"].min().strftime("%Y-%m-%d")
            max_date = subset["effective_date"].max().strftime("%Y-%m-%d")
            competitions_rows.append(
                {
                    "competition_id": competition_id,
                    "sport_id": sport_id,
                    "name": meta["competition_name"],
                    "season_year": None,
                    "level": "national_team_ranking",
                    "start_date": min_date,
                    "end_date": max_date,
                    "source_id": self.id,
                }
            )

            for ranking_year, group in subset.groupby("ranking_year", sort=True):
                rank_date = group["effective_date"].max()
                event_id = f"{competition_id}_{str(int(ranking_year))[-2:]}"
                events_rows.append(
                    {
                        "event_id": event_id,
                        "competition_id": competition_id,
                        "discipline_id": discipline_lookup[ranking_code]["discipline_id"],
                        "gender": meta["gender"],
                        "event_class": "ranking_release_top10",
                        "event_date": rank_date.strftime("%Y-%m-%d"),
                    }
                )

                sorted_group = group.sort_values(
                    ["source_rank", "rating", "points", "country_name"],
                    ascending=[True, False, False, True],
                    na_position="last",
                ).reset_index(drop=True)
                for position, row in enumerate(sorted_group.itertuples(index=False), start=1):
                    country_name = str(getattr(row, "country_name", "") or "").strip()
                    source_code = str(getattr(row, "country_code_source", "") or "").strip().upper()
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

                    rating = getattr(row, "rating", None)
                    points = getattr(row, "points", None)
                    matches = getattr(row, "matches", None)
                    results_rows.append(
                        {
                            "event_id": event_id,
                            "participant_id": participant_id,
                            "rank": position,
                            "medal": self._medal_from_rank(position),
                            "score_raw": (
                                f"icc_rating={rating};icc_points={points};icc_matches={matches}"
                                if pd.notna(rating) or pd.notna(points) or pd.notna(matches)
                                else None
                            ),
                            "points_awarded": float(rating) if pd.notna(rating) else None,
                        }
                    )

        return {
            "countries": pd.DataFrame(countries_rows.values()).drop_duplicates(subset=["country_id"]),
            "sports": sports_df,
            "disciplines": disciplines_df.drop_duplicates(subset=["discipline_id"]),
            "competitions": pd.DataFrame(competitions_rows).drop_duplicates(subset=["competition_id"]),
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
            conn.commit()

        db.upsert_dataframe("countries", payload.get("countries", pd.DataFrame()), ["country_id"])
        db.upsert_dataframe("sports", payload.get("sports", pd.DataFrame()), ["sport_id"])
        db.upsert_dataframe("disciplines", payload.get("disciplines", pd.DataFrame()), ["discipline_id"])
        db.upsert_dataframe("competitions", payload.get("competitions", pd.DataFrame()), ["competition_id"])
        db.upsert_dataframe("events", payload.get("events", pd.DataFrame()), ["event_id"])
        db.upsert_dataframe("participants", payload.get("participants", pd.DataFrame()), ["participant_id"])
        db.upsert_dataframe("results", payload.get("results", pd.DataFrame()), ["event_id", "participant_id"])
