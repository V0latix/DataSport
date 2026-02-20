from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from src.core.db import SQLiteDB
from src.core.utils import slugify, utc_now_iso

from .base import Connector


COMPETITIONS: dict[str, dict[str, str]] = {
    "men": {
        "competition_id": "fiba_men_ranking",
        "competition_name": "FIBA Men's World Ranking",
        "discipline_name": "FIBA Men Ranking",
        "gender": "men",
    },
    "women": {
        "competition_id": "fiba_women_ranking",
        "competition_name": "FIBA Women's World Ranking",
        "discipline_name": "FIBA Women Ranking",
        "gender": "women",
    },
}

COUNTRY_NAME_ALIASES = {
    "United States": "USA",
    "USA": "USA",
    "Czech Republic": "CZE",
}

RANKING_PAGE_BY_GENDER = {
    "men": "https://www.fiba.basketball/en/ranking/men",
    "women": "https://www.fiba.basketball/en/ranking/women",
}
CLIENT_APIM_URL_PATTERN = re.compile(r'"NEXT_CLIENT_APIM_URL":"([^"]+)"')
CLIENT_APIM_KEY_PATTERN = re.compile(r'"NEXT_CLIENT_APIM_SUBSCRIPTION_KEY":"([^"]+)"')
RANKING_DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}T00:00:00")
WORLD_ZONE_ID = 1


class FibaRankingHistoryConnector(Connector):
    id = "fiba_ranking_history"
    name = "FIBA Men/Women World Ranking History"
    source_type = "csv"
    license_notes = (
        "Historical top10 snapshots curated from official FIBA ranking pages "
        "and public ranking references. Verify downstream redistribution requirements."
    )
    base_url = "https://www.fiba.basketball/en/ranking"

    def source_row(self) -> dict[str, str]:
        return {
            "source_id": self.id,
            "source_name": self.name,
            "source_type": self.source_type,
            "license_notes": (
                "Historical snapshots from official FIBA ranking pages and APIM endpoints "
                "(getgdapfederationsranking). Fallback to local seed if remote fetch fails."
            ),
            "base_url": self.base_url,
        }

    def _local_seed_path(self) -> Path:
        return Path(__file__).resolve().parents[2] / "data" / "raw" / "basketball" / "fiba_rankings_history_seed.csv"

    def _extract_client_config(self, html: str) -> tuple[str, str]:
        base_url_match = CLIENT_APIM_URL_PATTERN.search(html)
        key_match = CLIENT_APIM_KEY_PATTERN.search(html)
        if not base_url_match or not key_match:
            raise RuntimeError("Unable to extract APIM configuration from FIBA ranking page.")
        return base_url_match.group(1).rstrip("/"), key_match.group(1)

    @staticmethod
    def _extract_ranking_dates(html: str, season_year: int) -> list[str]:
        seen: set[str] = set()
        ordered_dates: list[str] = []
        for raw in sorted(set(RANKING_DATE_PATTERN.findall(html))):
            date_only = raw[:10]
            if len(date_only) != 10:
                continue
            try:
                year = int(date_only[:4])
            except ValueError:
                continue
            if year > season_year:
                continue
            if date_only in seen:
                continue
            seen.add(date_only)
            ordered_dates.append(date_only)
        return ordered_dates

    def fetch(self, season_year: int, out_dir: Path) -> list[Path]:
        local_seed = self._local_seed_path()
        out_file = out_dir / "fiba_rankings_history_seed.csv"

        headers = {"User-Agent": "DataSportPipeline/0.1 (FIBA rankings fetch)"}
        rows: list[dict[str, Any]] = []
        errors: list[str] = []
        request_count = 0
        dates_by_gender: dict[str, int] = {}

        for gender, page_url in RANKING_PAGE_BY_GENDER.items():
            try:
                response = requests.get(page_url, headers=headers, timeout=90)
                response.raise_for_status()
                html = response.text

                apim_url, apim_key = self._extract_client_config(html)
                ranking_dates = self._extract_ranking_dates(html, season_year)
                dates_by_gender[gender] = len(ranking_dates)
                if not ranking_dates:
                    continue

                for date_value in ranking_dates:
                    request_count += 1
                    payload = self._request_json(
                        f"{apim_url}/getgdapfederationsranking",
                        headers={"Ocp-Apim-Subscription-Key": apim_key},
                        params={
                            "gdapCategory": gender,
                            "zoneRankingId": WORLD_ZONE_ID,
                            "asOfDate": date_value,
                        },
                        timeout=90,
                        retries=3,
                    )
                    if not isinstance(payload, dict):
                        continue

                    effective_raw = str(payload.get("asOfDate") or date_value).strip()
                    effective_date = effective_raw[:10]
                    if len(effective_date) != 10:
                        effective_date = date_value

                    items = payload.get("items") or []
                    if not isinstance(items, list):
                        continue

                    for item in items:
                        country_name = str(item.get("countryName") or "").strip()
                        source_rank = item.get("worldRank")
                        if not country_name or source_rank is None:
                            continue

                        rows.append(
                            {
                                "sport": gender,
                                "effective_date": effective_date,
                                "country_name": country_name,
                                "country_code": str(item.get("iocCode") or item.get("fibaCode") or "").strip().upper(),
                                "source_rank": source_rank,
                                "points": item.get("currentPoints"),
                            }
                        )
            except Exception as exc:
                errors.append(f"{gender}_fetch_failed: {exc}")

        if not rows:
            if not local_seed.exists():
                raise RuntimeError(f"Missing local seed for FIBA rankings history: {local_seed}")
            shutil.copy2(local_seed, out_file)
            self._write_json(
                out_dir / "fetch_meta.json",
                {"mode": "local_seed_fallback", "source": str(local_seed), "errors": errors},
            )
            return [out_file]

        frame = pd.DataFrame(rows).drop_duplicates(
            subset=["sport", "effective_date", "country_name", "country_code", "source_rank"],
            keep="first",
        )
        frame.to_csv(out_file, index=False)

        local_seed.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(local_seed, index=False)

        self._write_json(
            out_dir / "fetch_meta.json",
            {
                "mode": "fiba_api",
                "sources": RANKING_PAGE_BY_GENDER,
                "rows": int(len(frame)),
                "requests": request_count,
                "dates_by_gender": dates_by_gender,
                "years_requested": {"end": season_year},
                "errors": errors,
            },
        )
        return [out_file]

    def _resolve_country_code(self, country_name: str, country_code: str) -> str:
        alias = COUNTRY_NAME_ALIASES.get(str(country_name or "").strip())
        if alias:
            return alias

        code = str(country_code or "").strip().upper()
        if len(code) == 3:
            return code

        try:
            import pycountry

            country = pycountry.countries.lookup(country_name)
            candidate = getattr(country, "alpha_3", None)
            if candidate:
                return candidate
        except Exception:
            pass

        return slugify(country_name)[:3].upper()

    @staticmethod
    def _medal_from_rank(rank: int | float | None) -> str | None:
        if rank == 1:
            return "gold"
        if rank == 2:
            return "silver"
        if rank == 3:
            return "bronze"
        return None

    def parse(self, raw_paths: list[Path], season_year: int) -> dict[str, pd.DataFrame]:
        csv_path = next(path for path in raw_paths if path.name.endswith(".csv"))
        frame = pd.read_csv(csv_path)

        required_cols = {"sport", "effective_date", "country_name", "country_code", "source_rank", "points"}
        if not required_cols.issubset(set(frame.columns)):
            raise RuntimeError(f"Unsupported FIBA ranking seed format with columns: {list(frame.columns)}")

        frame["sport"] = frame["sport"].astype(str).str.strip().str.lower()
        frame = frame.loc[frame["sport"].isin(set(COMPETITIONS.keys()))].copy()
        frame["source_rank"] = pd.to_numeric(frame["source_rank"], errors="coerce")
        frame["points"] = pd.to_numeric(frame["points"], errors="coerce")
        frame["effective_date"] = pd.to_datetime(frame["effective_date"], errors="coerce")
        frame = frame.dropna(subset=["sport", "effective_date", "country_name", "source_rank"])
        frame["requested_year"] = frame["effective_date"].dt.year.astype(int)
        frame = frame.loc[frame["requested_year"] <= season_year].copy()

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
            raise RuntimeError("FIBA annual top10 generation returned zero rows.")

        timestamp = utc_now_iso()
        sport_id = slugify("Basketball")
        sports_df = pd.DataFrame(
            [
                {
                    "sport_id": sport_id,
                    "sport_name": "Basketball",
                    "sport_slug": sport_id,
                    "created_at_utc": timestamp,
                }
            ]
        )

        discipline_row = {
            "discipline_id": sport_id,
            "discipline_name": "Basketball",
            "discipline_slug": sport_id,
            "sport_id": sport_id,
            "confidence": 1.0,
            "mapping_source": "connector_fiba_ranking_history",
            "created_at_utc": timestamp,
        }
        discipline_lookup = {sport_code: discipline_row for sport_code in COMPETITIONS}
        disciplines_df = pd.DataFrame([discipline_row])

        competitions_rows: list[dict[str, Any]] = []
        events_rows: list[dict[str, Any]] = []
        participants_rows: dict[str, dict[str, Any]] = {}
        countries_rows: dict[str, dict[str, Any]] = {}
        results_rows: list[dict[str, Any]] = []

        for sport_code, meta in COMPETITIONS.items():
            subset = annual.loc[annual["sport"] == sport_code].copy()
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

            for ranking_year, group in subset.groupby("requested_year", sort=True):
                rank_date = group["effective_date"].max()
                date_text = rank_date.strftime("%Y-%m-%d")
                event_id = f"{competition_id}_{str(int(ranking_year))[-2:]}"
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
                )
                for _, row in sorted_group.iterrows():
                    country_name = str(row["country_name"]).strip()
                    country_id = self._resolve_country_code(country_name, str(row.get("country_code") or ""))
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

                    rank = int(row["source_rank"])
                    points = float(row["points"]) if pd.notna(row["points"]) else None
                    results_rows.append(
                        {
                            "event_id": event_id,
                            "participant_id": participant_id,
                            "rank": rank,
                            "medal": self._medal_from_rank(rank),
                            "score_raw": f"fiba_points={points}" if points is not None else None,
                            "points_awarded": points,
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
            conn.execute("DELETE FROM disciplines WHERE mapping_source = 'connector_fiba_ranking_history'")
            conn.commit()

        db.upsert_dataframe("countries", payload.get("countries", pd.DataFrame()), ["country_id"])
        db.upsert_dataframe("sports", payload.get("sports", pd.DataFrame()), ["sport_id"])
        db.upsert_dataframe("disciplines", payload.get("disciplines", pd.DataFrame()), ["discipline_id"])
        db.upsert_dataframe("competitions", payload.get("competitions", pd.DataFrame()), ["competition_id"])
        db.upsert_dataframe("events", payload.get("events", pd.DataFrame()), ["event_id"])
        db.upsert_dataframe("participants", payload.get("participants", pd.DataFrame()), ["participant_id"])
        db.upsert_dataframe("results", payload.get("results", pd.DataFrame()), ["event_id", "participant_id"])
