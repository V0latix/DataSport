from __future__ import annotations

import csv
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from src.core.db import SQLiteDB
from src.core.utils import slugify, utc_now_iso

from .base import Connector


BASE_URL = "https://dataride.uci.org"
SEASONS_ENDPOINT = "/iframe/GetDisciplineSeasons/"
RANKINGS_DISCIPLINE_ENDPOINT = "/iframe/RankingsDiscipline/"
OBJECT_RANKINGS_ENDPOINT = "/iframe/ObjectRankings/"

DISCIPLINE_ID = 10  # Road cycling
CATEGORY_ID = 22  # Men Elite
RACE_TYPE_ID = 0  # All
WORLD_GROUP_ID = 1
NATION_RANKING_TYPE_ID = 3
MIN_HISTORY_YEAR = 2000


COUNTRY_NAME_ALIASES = {
    "GREAT BRITAIN": "GBR",
}


class UciRoadNationRankingHistoryConnector(Connector):
    id = "uci_road_nation_ranking_history"
    name = "UCI Road World Nation Ranking History (Top 10)"
    source_type = "api"
    license_notes = (
        "Historical snapshots from UCI DataRide road rankings iframe endpoints "
        "(GetDisciplineSeasons, RankingsDiscipline, ObjectRankings)."
    )
    base_url = f"{BASE_URL}/iframe/Rankings/{DISCIPLINE_ID}/"

    def source_row(self) -> dict[str, str]:
        return {
            "source_id": self.id,
            "source_name": self.name,
            "source_type": self.source_type,
            "license_notes": (
                "Official UCI DataRide Road (disciplineId=10) world nation ranking snapshots. "
                "Top 10 only, one latest snapshot retained per season year when available."
            ),
            "base_url": self.base_url,
        }

    def _local_seed_path(self) -> Path:
        return Path(__file__).resolve().parents[2] / "data" / "raw" / "cycling" / "uci_road_nation_rankings_history_seed.csv"

    @staticmethod
    def _parse_dotnet_date(value: Any) -> str | None:
        text = str(value or "").strip()
        if not text.startswith("/Date("):
            return None
        try:
            millis = int(text[6:].split(")", 1)[0])
        except Exception:
            return None
        dt = datetime.fromtimestamp(millis / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except Exception:
            return default

    def _get_json(self, path: str, params: dict[str, Any], timeout: int = 45) -> Any:
        response = requests.get(
            f"{BASE_URL}{path}",
            params=params,
            headers={"User-Agent": "DataSportPipeline/0.1 (UCI road nation rankings fetch)"},
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()

    def _post_json(self, path: str, data: dict[str, Any], timeout: int = 45) -> Any:
        response = requests.post(
            f"{BASE_URL}{path}",
            data=data,
            headers={
                "User-Agent": "DataSportPipeline/0.1 (UCI road nation rankings fetch)",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            },
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()

    def _fetch_seasons(self, season_year: int) -> list[dict[str, int]]:
        payload = self._get_json(SEASONS_ENDPOINT, {"disciplineId": DISCIPLINE_ID})
        seasons: list[dict[str, int]] = []
        for row in payload:
            year = self._safe_int(row.get("Year"))
            season_id = self._safe_int(row.get("Id"))
            if year < MIN_HISTORY_YEAR or year > int(season_year) or season_id <= 0:
                continue
            seasons.append({"year": year, "season_id": season_id})
        seasons.sort(key=lambda x: x["year"])
        return seasons

    def _fetch_rankings_discipline(self, season_id: int) -> list[dict[str, Any]]:
        payload = self._post_json(
            RANKINGS_DISCIPLINE_ENDPOINT,
            {
                "disciplineId": DISCIPLINE_ID,
                "take": 40,
                "skip": 0,
                "page": 1,
                "pageSize": 40,
                "filter[filters][0][field]": "RaceTypeId",
                "filter[filters][0][value]": RACE_TYPE_ID,
                "filter[filters][1][field]": "CategoryId",
                "filter[filters][1][value]": CATEGORY_ID,
                "filter[filters][2][field]": "SeasonId",
                "filter[filters][2][value]": season_id,
            },
        )
        return payload if isinstance(payload, list) else []

    def _select_world_nation_entry(self, groups: list[dict[str, Any]]) -> dict[str, Any] | None:
        for group in groups:
            group_id = self._safe_int(group.get("GroupId"))
            group_name = str(group.get("GroupName") or "").strip()
            if group_id != WORLD_GROUP_ID or group_name != "World Ranking":
                continue

            for ranking in group.get("Rankings") or []:
                ranking_type_id = self._safe_int(ranking.get("RankingTypeId"))
                if ranking_type_id != NATION_RANKING_TYPE_ID:
                    continue
                return {
                    "group_id": group_id,
                    "group_name": group_name,
                    "ranking_id": self._safe_int(ranking.get("Id")),
                    "moment_id": self._safe_int(ranking.get("MomentId")),
                    "ranking_name": str(ranking.get("RankingName") or "").strip(),
                }
        return None

    def _fetch_object_rankings(self, ranking_id: int, season_id: int, moment_id: int) -> list[dict[str, Any]]:
        payload = self._post_json(
            OBJECT_RANKINGS_ENDPOINT,
            {
                "rankingId": ranking_id,
                "disciplineId": DISCIPLINE_ID,
                "rankingTypeId": NATION_RANKING_TYPE_ID,
                "take": 40,
                "skip": 0,
                "page": 1,
                "pageSize": 40,
                "filter[filters][0][field]": "RaceTypeId",
                "filter[filters][0][value]": RACE_TYPE_ID,
                "filter[filters][1][field]": "CategoryId",
                "filter[filters][1][value]": CATEGORY_ID,
                "filter[filters][2][field]": "SeasonId",
                "filter[filters][2][value]": season_id,
                "filter[filters][3][field]": "MomentId",
                "filter[filters][3][value]": moment_id,
                "filter[filters][4][field]": "CountryId",
                "filter[filters][4][value]": 0,
                "filter[filters][5][field]": "IndividualName",
                "filter[filters][5][value]": "",
                "filter[filters][6][field]": "TeamName",
                "filter[filters][6][value]": "",
            },
        )
        if isinstance(payload, dict):
            return list(payload.get("data") or [])
        return []

    def _build_rows(self, season_year: int) -> tuple[list[dict[str, Any]], list[int]]:
        rows: list[dict[str, Any]] = []
        missing_world_nation_years: list[int] = []

        for season in self._fetch_seasons(season_year):
            year = season["year"]
            season_id = season["season_id"]

            groups = self._fetch_rankings_discipline(season_id)
            selected = self._select_world_nation_entry(groups)
            if not selected:
                missing_world_nation_years.append(year)
                continue

            ranking_rows = self._fetch_object_rankings(
                ranking_id=selected["ranking_id"],
                season_id=season_id,
                moment_id=selected["moment_id"],
            )
            ranking_rows = sorted(
                ranking_rows,
                key=lambda row: (
                    self._safe_int(row.get("Rank"), default=9999),
                    str(row.get("NationFullName") or row.get("FullName") or ""),
                ),
            )

            top10 = [row for row in ranking_rows if self._safe_int(row.get("Rank")) > 0][:10]
            if len(top10) < 10:
                missing_world_nation_years.append(year)
                continue

            for row in top10:
                country_name = str(row.get("NationFullName") or row.get("FullName") or "").strip()
                country_code = str(row.get("NationName") or "").strip().upper()
                points_value = row.get("Points")
                rows.append(
                    {
                        "requested_year": year,
                        "discipline_season_id": season_id,
                        "ranking_id": selected["ranking_id"],
                        "group_id": selected["group_id"],
                        "group_name": selected["group_name"],
                        "ranking_name": selected["ranking_name"],
                        "moment_id": selected["moment_id"],
                        "effective_date": self._parse_dotnet_date(row.get("ComputationDate")),
                        "country_name": country_name,
                        "country_code": country_code,
                        "source_rank": self._safe_int(row.get("Rank")),
                        "points": float(points_value) if points_value is not None else None,
                        "source_url": (
                            f"{BASE_URL}/iframe/RankingDetails/{selected['ranking_id']}"
                            f"?disciplineId={DISCIPLINE_ID}"
                            f"&groupId={selected['group_id']}"
                            f"&momentId={selected['moment_id']}"
                            f"&disciplineSeasonId={season_id}"
                            f"&rankingTypeId={NATION_RANKING_TYPE_ID}"
                            f"&categoryId={CATEGORY_ID}"
                            f"&raceTypeId={RACE_TYPE_ID}"
                        ),
                    }
                )

        rows.sort(key=lambda r: (int(r["requested_year"]), int(r["source_rank"]), str(r["country_code"])))
        return rows, sorted(set(missing_world_nation_years))

    @staticmethod
    def _write_seed(path: Path, rows: list[dict[str, Any]]) -> None:
        headers = [
            "requested_year",
            "discipline_season_id",
            "ranking_id",
            "group_id",
            "group_name",
            "ranking_name",
            "moment_id",
            "effective_date",
            "country_name",
            "country_code",
            "source_rank",
            "points",
            "source_url",
        ]
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)

    def fetch(self, season_year: int, out_dir: Path) -> list[Path]:
        out_file = out_dir / "uci_road_nation_rankings_history_seed.csv"
        local_seed = self._local_seed_path()
        errors: list[str] = []

        try:
            rows, missing_years = self._build_rows(season_year=season_year)
            if not rows:
                raise RuntimeError("UCI road world nation ranking fetch returned zero rows.")

            self._write_seed(out_file, rows)
            self._write_seed(local_seed, rows)
            available_years = sorted({int(row["requested_year"]) for row in rows})

            self._write_json(
                out_dir / "fetch_meta.json",
                {
                    "mode": "uci_api",
                    "base_url": BASE_URL,
                    "discipline_id": DISCIPLINE_ID,
                    "category_id": CATEGORY_ID,
                    "race_type_id": RACE_TYPE_ID,
                    "rows": len(rows),
                    "years_available": {
                        "min": available_years[0],
                        "max": available_years[-1],
                    },
                    "missing_world_nation_years": missing_years,
                    "errors": errors,
                },
            )
            return [out_file]
        except Exception as exc:
            errors.append(str(exc))
            if not local_seed.exists():
                raise RuntimeError(
                    f"UCI road world nation ranking fetch failed and no local seed exists: {exc}"
                ) from exc

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
        alias = COUNTRY_NAME_ALIASES.get(str(country_name or "").strip().upper())
        if alias:
            return alias

        code = str(country_code or "").strip().upper()
        if len(code) == 3 and code in known_codes:
            return code

        try:
            import pycountry

            country = pycountry.countries.lookup(country_name)
            alpha3 = getattr(country, "alpha_3", None)
            if alpha3:
                return alpha3
        except Exception:
            pass

        return code if len(code) == 3 else slugify(country_name)[:3].upper()

    def parse(self, raw_paths: list[Path], season_year: int) -> dict[str, pd.DataFrame]:
        csv_path = next(path for path in raw_paths if path.name.endswith(".csv"))
        frame = pd.read_csv(csv_path)

        required_cols = {
            "requested_year",
            "discipline_season_id",
            "ranking_id",
            "group_id",
            "group_name",
            "ranking_name",
            "moment_id",
            "effective_date",
            "country_name",
            "country_code",
            "source_rank",
            "points",
            "source_url",
        }
        if not required_cols.issubset(set(frame.columns)):
            raise RuntimeError(f"Unsupported UCI road nation ranking seed format: {list(frame.columns)}")

        frame["requested_year"] = pd.to_numeric(frame["requested_year"], errors="coerce")
        frame["discipline_season_id"] = pd.to_numeric(frame["discipline_season_id"], errors="coerce")
        frame["ranking_id"] = pd.to_numeric(frame["ranking_id"], errors="coerce")
        frame["group_id"] = pd.to_numeric(frame["group_id"], errors="coerce")
        frame["moment_id"] = pd.to_numeric(frame["moment_id"], errors="coerce")
        frame["source_rank"] = pd.to_numeric(frame["source_rank"], errors="coerce")
        frame["points"] = pd.to_numeric(frame["points"], errors="coerce")
        frame["effective_date"] = pd.to_datetime(frame["effective_date"], errors="coerce")

        frame = frame.dropna(
            subset=[
                "requested_year",
                "ranking_id",
                "group_id",
                "source_rank",
                "country_name",
                "country_code",
                "effective_date",
            ]
        )
        frame["requested_year"] = frame["requested_year"].astype(int)
        frame = frame.loc[frame["requested_year"] <= int(season_year)].copy()

        frame = frame.sort_values(
            ["requested_year", "effective_date", "source_rank", "country_code"],
            ascending=[True, True, True, True],
            na_position="last",
        ).drop_duplicates(subset=["requested_year", "country_code", "source_rank"], keep="last")

        selected_dates = frame.groupby("requested_year", as_index=False)["effective_date"].max()
        selected_dates = selected_dates.rename(columns={"effective_date": "selected_effective_date"})
        annual = frame.merge(selected_dates, on="requested_year", how="inner")
        annual = annual.loc[annual["effective_date"] == annual["selected_effective_date"]].copy()

        annual = annual.sort_values(
            ["requested_year", "source_rank", "points", "country_name"],
            ascending=[True, True, False, True],
            na_position="last",
        )
        annual = annual.groupby("requested_year", as_index=False).head(10).reset_index(drop=True)

        if annual.empty:
            raise RuntimeError("UCI road world nation ranking parse produced zero annual rows.")

        per_year_counts = annual.groupby("requested_year")["country_code"].count()
        bad_counts = per_year_counts[per_year_counts != 10]
        if not bad_counts.empty:
            raise RuntimeError(f"Unexpected top10 cardinality for years: {bad_counts.to_dict()}")

        rank_profiles = annual.groupby("requested_year")["source_rank"].apply(lambda s: tuple(sorted(int(x) for x in s.tolist())))
        invalid_profiles = {int(year): profile for year, profile in rank_profiles.items() if profile != tuple(range(1, 11))}
        if invalid_profiles:
            raise RuntimeError(f"Unexpected rank profiles for UCI road nation ranking: {invalid_profiles}")

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
        sports_df = pd.DataFrame(
            [
                {
                    "sport_id": "cycling",
                    "sport_name": "Cycling",
                    "sport_slug": "cycling",
                    "created_at_utc": timestamp,
                }
            ]
        )
        disciplines_df = pd.DataFrame(
            [
                {
                    "discipline_id": "road-race",
                    "discipline_name": "Road Race",
                    "discipline_slug": "road-race",
                    "sport_id": "cycling",
                    "confidence": 1.0,
                    "mapping_source": "connector_uci_road_nation_ranking_history",
                    "created_at_utc": timestamp,
                }
            ]
        )

        competition_id = "uci_road_world_nation_ranking"
        min_date = annual["effective_date"].min().strftime("%Y-%m-%d")
        max_date = annual["effective_date"].max().strftime("%Y-%m-%d")
        competitions_df = pd.DataFrame(
            [
                {
                    "competition_id": competition_id,
                    "sport_id": "cycling",
                    "name": "UCI Road World Nation Ranking",
                    "season_year": None,
                    "level": "national_team_ranking",
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

        for ranking_year, group in annual.groupby("requested_year", sort=True):
            rank_date = group["effective_date"].max()
            event_id = f"{competition_id}_{int(ranking_year)}"
            events_rows.append(
                {
                    "event_id": event_id,
                    "competition_id": competition_id,
                    "discipline_id": "road-race",
                    "gender": "men",
                    "event_class": "ranking_release_top10",
                    "event_date": rank_date.strftime("%Y-%m-%d"),
                }
            )

            sorted_group = group.sort_values(
                ["source_rank", "points", "country_name"],
                ascending=[True, False, True],
                na_position="last",
            )
            for _, row in sorted_group.iterrows():
                country_name = str(row["country_name"]).strip()
                country_code = str(row["country_code"]).strip().upper()
                country_id = self._resolve_country_code(country_name, country_code, known_country_codes)
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

                rank = int(row["source_rank"])
                points = float(row["points"]) if pd.notna(row["points"]) else None
                results_rows.append(
                    {
                        "event_id": event_id,
                        "participant_id": participant_id,
                        "rank": rank,
                        "medal": self._medal_from_rank(rank),
                        "score_raw": f"uci_points={points}" if points is not None else None,
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
        countries_df = payload.get("countries", pd.DataFrame())
        sports_df = payload.get("sports", pd.DataFrame())
        disciplines_df = payload.get("disciplines", pd.DataFrame())
        participants_df = payload.get("participants", pd.DataFrame())

        with db.connect() as conn:
            existing_country_ids = {row[0] for row in conn.execute("SELECT country_id FROM countries").fetchall()}
            existing_sport_ids = {row[0] for row in conn.execute("SELECT sport_id FROM sports").fetchall()}
            existing_discipline_ids = {
                row[0] for row in conn.execute("SELECT discipline_id FROM disciplines").fetchall()
            }
            existing_participant_ids = {
                row[0] for row in conn.execute("SELECT participant_id FROM participants").fetchall()
            }

        if not countries_df.empty:
            countries_df = countries_df.loc[~countries_df["country_id"].isin(existing_country_ids)].copy()
        if not sports_df.empty:
            sports_df = sports_df.loc[~sports_df["sport_id"].isin(existing_sport_ids)].copy()
        if not disciplines_df.empty:
            disciplines_df = disciplines_df.loc[
                ~disciplines_df["discipline_id"].isin(existing_discipline_ids)
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
            conn.commit()

        db.upsert_dataframe("countries", countries_df, ["country_id"])
        db.upsert_dataframe("sports", sports_df, ["sport_id"])
        db.upsert_dataframe("disciplines", disciplines_df, ["discipline_id"])
        db.upsert_dataframe("competitions", payload.get("competitions", pd.DataFrame()), ["competition_id"])
        db.upsert_dataframe("events", payload.get("events", pd.DataFrame()), ["event_id"])
        db.upsert_dataframe("participants", participants_df, ["participant_id"])
        db.upsert_dataframe("results", payload.get("results", pd.DataFrame()), ["event_id", "participant_id"])
