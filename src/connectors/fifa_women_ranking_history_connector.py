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


GITHUB_LISTING_URL = "https://api.github.com/repos/hericlibong/FifaWomen_Ranking_ApiScrapy/contents/soccerwomen/spiders"
DIRECT_SOURCE_URLS = [
    "https://raw.githubusercontent.com/hericlibong/FifaWomen_Ranking_ApiScrapy/main/soccerwomen/spiders/data.csv",
    "https://raw.githubusercontent.com/alxotte/fifa-world-ranking/master/fifa_w_ranking_historical.csv",
    "https://raw.githubusercontent.com/Dato-Futbol/fifa-ranking/master/ranking_fifa_w_historical.csv",
]
CSV_NAME_PATTERN = re.compile(r".*\.csv$", re.IGNORECASE)
FILENAME_DATE_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})")

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


class FifaWomenRankingHistoryConnector(Connector):
    id = "fifa_women_ranking_history"
    name = "FIFA Women's Ranking Historical CSV"
    source_type = "csv"
    license_notes = (
        "Open-source mirrors of FIFA women's ranking snapshots (source repository attribution required; "
        "validate terms for redistribution)."
    )
    base_url = GITHUB_LISTING_URL

    def _local_seed_paths(self) -> list[Path]:
        base = Path(__file__).resolve().parents[2] / "data" / "raw" / "fifa_women"
        preferred = [
            base / "fifa_w_ranking_historical.csv",
            base / "fifa_w_ranking_full.csv",
            base / "fifa_women_ranking_history.csv",
            base / "data.csv",
        ]
        ordered: list[Path] = [path for path in preferred if path.exists()]
        ordered.extend(sorted(base.glob("fifa_w_ranking-*.csv")))
        return [path for index, path in enumerate(ordered) if path.exists() and path not in ordered[:index]]

    def _fetch_from_github_listing(self, out_dir: Path) -> list[Path]:
        rows = self._request_json(
            GITHUB_LISTING_URL,
            headers={"Accept": "application/vnd.github+json"},
            timeout=90,
            retries=2,
        )
        if not isinstance(rows, list):
            return []
        candidates = sorted(
            [
                row
                for row in rows
                if isinstance(row, dict)
                and CSV_NAME_PATTERN.match(str(row.get("name", "")))
                and row.get("download_url")
            ],
            key=lambda row: str(row.get("name")),
        )
        out_paths: list[Path] = []
        headers = {"User-Agent": "DataSportPipeline/0.1 (FIFA women ranking fetch)"}
        for row in candidates:
            file_name = str(row["name"])
            download_url = str(row["download_url"])
            target = out_dir / file_name
            with requests.get(download_url, headers=headers, timeout=120) as response:
                response.raise_for_status()
                target.write_bytes(response.content)
            out_paths.append(target)
        return out_paths

    def fetch(self, season_year: int, out_dir: Path) -> list[Path]:
        del season_year
        local_seeds = self._local_seed_paths()
        if local_seeds:
            copied_paths: list[Path] = []
            for source in local_seeds:
                target = out_dir / source.name
                shutil.copy2(source, target)
                copied_paths.append(target)
            self._write_json(
                out_dir / "fetch_meta.json",
                {"mode": "local_seed", "sources": [str(path) for path in local_seeds], "files": len(copied_paths)},
            )
            return copied_paths

        last_error: Exception | None = None
        try:
            github_paths = self._fetch_from_github_listing(out_dir)
            if github_paths:
                self._write_json(
                    out_dir / "fetch_meta.json",
                    {
                        "mode": "download_github_listing",
                        "source": GITHUB_LISTING_URL,
                        "files": [path.name for path in github_paths],
                    },
                )
                return github_paths
        except Exception as exc:
            last_error = exc

        headers = {"User-Agent": "DataSportPipeline/0.1 (FIFA women ranking fetch)"}
        for url in DIRECT_SOURCE_URLS:
            try:
                target = out_dir / Path(url).name
                with requests.get(url, headers=headers, timeout=120) as response:
                    response.raise_for_status()
                    target.write_bytes(response.content)
                self._write_json(out_dir / "fetch_meta.json", {"mode": "download_direct", "source": url})
                return [target]
            except Exception as exc:
                last_error = exc

        raise RuntimeError(
            "Unable to fetch FIFA women ranking source. "
            "Provide local seed(s) in data/raw/fifa_women/ "
            "(fifa_w_ranking_historical.csv or fifa_w_ranking-YYYY-MM-DD.csv). "
            f"Last error: {last_error}"
        )

    @staticmethod
    def _medal_from_rank(rank: int | float | None) -> str | None:
        if rank == 1:
            return "gold"
        if rank == 2:
            return "silver"
        if rank == 3:
            return "bronze"
        return None

    @staticmethod
    def _rank_date_from_filename(path: Path) -> pd.Timestamp | None:
        match = FILENAME_DATE_PATTERN.search(path.name)
        if not match:
            return None
        parsed = pd.to_datetime(match.group(1), errors="coerce")
        if pd.isna(parsed):
            return None
        return parsed

    @staticmethod
    def _first_existing_column(raw: pd.DataFrame, candidates: list[str]) -> str | None:
        columns = set(raw.columns)
        for candidate in candidates:
            if candidate in columns:
                return candidate
        return None

    @staticmethod
    def _extract_country_code(value: Any) -> str | None:
        text = str(value or "").strip().upper()
        if len(text) == 3 and text.isalpha():
            return text
        match = re.search(r"/([A-Z]{3})(?:$|[/?#])", text)
        if match:
            return match.group(1)
        return None

    def _normalize_frame(self, raw: pd.DataFrame, source_path: Path) -> pd.DataFrame:
        rename_to_normalized = {column: column.strip().lower().replace(" ", "_") for column in raw.columns}
        frame = raw.rename(columns=rename_to_normalized).copy()
        raw_columns = set(frame.columns)
        if {"country_full", "country_abrv", "total_points", "rank_date"}.issubset(raw_columns):
            frame = frame.rename(
                columns={
                    "country_full": "country_name",
                    "country_abrv": "country_code",
                    "rank_date": "rank_date",
                    "total_points": "total_points",
                    "rank": "source_rank",
                }
            )
        elif {"team", "team_short", "total_points", "date"}.issubset(raw_columns):
            frame = frame.rename(
                columns={
                    "team": "country_name",
                    "team_short": "country_code",
                    "date": "rank_date",
                    "total_points": "total_points",
                    "rank": "source_rank",
                }
            )
        elif {"name", "points", "date"}.issubset(raw_columns):
            frame = frame.rename(
                columns={
                    "name": "country_name",
                    "points": "total_points",
                    "date": "rank_date",
                    "rank": "source_rank",
                }
            )
            if "countryurl" in frame.columns:
                frame["country_code"] = frame["countryurl"].map(self._extract_country_code)
            elif "flag" in frame.columns:
                frame["country_code"] = frame["flag"].map(self._extract_country_code)
        else:
            country_col = self._first_existing_column(frame, ["country_full", "country", "team", "name"])
            code_col = self._first_existing_column(
                frame,
                ["country_abrv", "team_short", "code", "country_code", "countryurl", "country_url", "flag"],
            )
            points_col = self._first_existing_column(frame, ["total_points", "points", "pts"])
            date_col = self._first_existing_column(frame, ["rank_date", "date", "ranking_date"])
            rank_col = self._first_existing_column(frame, ["rank", "position", "ranking"])
            if not country_col or not points_col:
                raise RuntimeError(
                    f"Unsupported FIFA women CSV format in {source_path.name} with columns: {list(frame.columns)}"
                )
            frame = frame.rename(
                columns={
                    country_col: "country_name",
                    points_col: "total_points",
                }
            )
            if code_col:
                if code_col == "country_code":
                    pass
                elif code_col in ("countryurl", "country_url", "flag"):
                    frame["country_code"] = frame[code_col].map(self._extract_country_code)
                else:
                    frame["country_code"] = frame[code_col]
            if date_col:
                frame = frame.rename(columns={date_col: "rank_date"})
            if rank_col:
                frame = frame.rename(columns={rank_col: "source_rank"})

        if "source_rank" not in frame.columns:
            frame["source_rank"] = None
        if "rank_date" not in frame.columns:
            frame["rank_date"] = None
        if "country_code" not in frame.columns:
            frame["country_code"] = None

        fallback_date = self._rank_date_from_filename(source_path)
        if fallback_date is not None:
            frame["rank_date"] = frame["rank_date"].fillna(fallback_date.strftime("%Y-%m-%d"))
        frame["country_code"] = frame["country_code"].map(self._extract_country_code)

        keep_cols = ["country_name", "country_code", "total_points", "rank_date", "source_rank"]
        missing_keep_cols = [column for column in keep_cols if column not in frame.columns]
        for column in missing_keep_cols:
            frame[column] = None
        return frame[keep_cols].copy()

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
        csv_paths = [path for path in raw_paths if path.suffix.lower() == ".csv"]
        if not csv_paths:
            raise RuntimeError("No CSV files found in fetched paths for FIFA women ranking.")

        normalized_frames: list[pd.DataFrame] = []
        for csv_path in csv_paths:
            raw = pd.read_csv(csv_path)
            normalized = self._normalize_frame(raw, csv_path)
            if not normalized.empty:
                normalized_frames.append(normalized)

        if not normalized_frames:
            raise RuntimeError("FIFA women ranking source produced no rows after normalization.")

        frame = pd.concat(normalized_frames, ignore_index=True)
        frame["source_rank"] = pd.to_numeric(frame["source_rank"], errors="coerce")
        frame["total_points"] = pd.to_numeric(frame["total_points"], errors="coerce")
        frame["rank_date"] = pd.to_datetime(frame["rank_date"], errors="coerce")
        frame = frame.dropna(subset=["rank_date", "country_name", "country_code"]).copy()
        frame["country_name"] = frame["country_name"].astype(str).str.strip()
        frame["country_code"] = frame["country_code"].astype(str).str.strip().str.upper()
        frame = frame.loc[frame["rank_date"].dt.year <= season_year].copy()

        if frame.empty:
            raise RuntimeError(
                f"No FIFA women ranking rows available up to year={season_year}. "
                "Check source coverage or use a newer local seed."
            )

        frame = frame.sort_values(
            ["rank_date", "source_rank", "total_points", "country_name"],
            ascending=[True, True, False, True],
            na_position="last",
        )
        frame = frame.drop_duplicates(subset=["rank_date", "country_name", "country_code"], keep="first")

        missing_rank = frame["source_rank"].isna()
        if missing_rank.any():
            frame.loc[missing_rank, "source_rank"] = (
                frame.loc[missing_rank]
                .sort_values(["rank_date", "total_points", "country_name"], ascending=[True, False, True])
                .groupby("rank_date")
                .cumcount()
                + 1
            )

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

        if annual.empty:
            raise RuntimeError("FIFA women ranking annual top 10 generation returned zero rows.")

        timestamp = utc_now_iso()
        sport_id = slugify("Football")
        discipline_name = "Football"
        discipline_id = sport_id
        competition_id = "fifa_women_ranking"

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
                    "mapping_source": "connector_fifa_women_ranking_history",
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
                "name": "FIFA Women's Ranking",
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
            event_id = f"fifa_women_ranking_{str(int(ranking_year))[-2:]}"

            events_rows.append(
                {
                    "event_id": event_id,
                    "competition_id": competition_id,
                    "discipline_id": discipline_id,
                    "gender": "women",
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
                        "score_raw": f"fifa_women_points={points}",
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
            conn.execute("DELETE FROM disciplines WHERE mapping_source = 'connector_fifa_women_ranking_history'")
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
