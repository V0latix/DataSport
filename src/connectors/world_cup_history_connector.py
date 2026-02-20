from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Any

import pandas as pd

from src.core.db import SQLiteDB
from src.core.utils import slugify, utc_now_iso

from .base import Connector


OPENFOOTBALL_RAW_BASE = "https://raw.githubusercontent.com/openfootball/world-cup/master"

WORLD_CUP_FOLDERS = [
    (1930, "1930--uruguay"),
    (1934, "1934--italy"),
    (1938, "1938--france"),
    (1950, "1950--brazil"),
    (1954, "1954--switzerland"),
    (1958, "1958--sweden"),
    (1962, "1962--chile"),
    (1966, "1966--england"),
    (1970, "1970--mexico"),
    (1974, "1974--west-germany"),
    (1978, "1978--argentina"),
    (1982, "1982--spain"),
    (1986, "1986--mexico"),
    (1990, "1990--italy"),
    (1994, "1994--united-states"),
    (1998, "1998--france"),
    (2002, "2002--south-korea-n-japan"),
    (2006, "2006--germany"),
    (2010, "2010--south-africa"),
    (2014, "2014--brazil"),
    (2018, "2018--russia"),
    (2022, "2022--qatar"),
    (2026, "2026--usa"),
]

COUNTRY_OVERRIDES = {
    "West Germany": "DEU",
    "East Germany": "DDR",
    "Czechoslovakia": "TCH",
    "Soviet Union": "URS",
    "Yugoslavia": "YUG",
    "Netherlands": "NLD",
    "United States": "USA",
    "South Korea": "KOR",
    "Korea Republic": "KOR",
    "North Korea": "PRK",
    "Iran": "IRN",
    "England": "ENG",
    "Wales": "WAL",
    "Scotland": "SCO",
    "Northern Ireland": "NIR",
    "Czech Republic": "CZE",
    "Turkey": "TUR",
}

SPECIAL_TOP4 = {
    1930: [("Uruguay", 1), ("Argentina", 2), ("United States", 3), ("Yugoslavia", 4)],
    1950: [("Uruguay", 1), ("Brazil", 2), ("Sweden", 3), ("Spain", 4)],
}

MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}


class WorldCupHistoryConnector(Connector):
    id = "world_cup_history"
    name = "FIFA World Cup Historical Results"
    source_type = "text"
    license_notes = "OpenFootball open data. Verify downstream redistribution requirements."
    base_url = "https://raw.githubusercontent.com/openfootball/world-cup/master"

    def source_row(self) -> dict[str, str]:
        return {
            "source_id": self.id,
            "source_name": self.name,
            "source_type": self.source_type,
            "license_notes": (
                "OpenFootball open data. Parsed from raw GitHub files (cup_finals.txt/cup.txt) "
                "with optional local seed fallback data/raw/world_cup/world_cup_top4_seed.csv."
            ),
            "base_url": self.base_url,
        }

    def _local_seed_path(self) -> Path:
        return Path(__file__).resolve().parents[2] / "data" / "raw" / "world_cup" / "world_cup_top4_seed.csv"

    def fetch(self, season_year: int, out_dir: Path) -> list[Path]:
        local_seed = self._local_seed_path()
        if local_seed.exists():
            out_file = out_dir / "world_cup_top4_seed.csv"
            shutil.copy2(local_seed, out_file)
            self._write_json(out_dir / "fetch_meta.json", {"mode": "local_seed", "source": str(local_seed)})
            return [out_file]

        raw_paths: list[Path] = []
        downloaded_years: list[int] = []
        for year, folder_name in WORLD_CUP_FOLDERS:
            if year > season_year:
                continue
            chosen_content: str | None = None
            try:
                import requests

                headers = {"User-Agent": "DataSportPipeline/0.1 (World Cup history fetch)"}
                for candidate in ("cup_finals.txt", "cup.txt"):
                    url = f"{OPENFOOTBALL_RAW_BASE}/{folder_name}/{candidate}"
                    response = requests.get(url, headers=headers, timeout=60)
                    if response.status_code == 200 and response.text.strip():
                        chosen_content = response.text
                        break
            except Exception:
                chosen_content = None

            if chosen_content:
                target = out_dir / f"cup_{year}.txt"
                target.write_text(chosen_content, encoding="utf-8")
                raw_paths.append(target)
                downloaded_years.append(year)

        if not raw_paths:
            raise RuntimeError("No World Cup source files could be downloaded.")

        self._write_json(
            out_dir / "fetch_meta.json",
            {
                "mode": "download",
                "source": self.base_url,
                "downloaded_years": downloaded_years,
            },
        )
        return raw_paths

    @staticmethod
    def _parse_match_line(line: str) -> tuple[str, str, int, int] | None:
        if not line.strip().startswith("("):
            return None
        no_venue = line.split("@", 1)[0]
        score_match = re.search(r"(\d+)\s*-\s*(\d+)", no_venue)
        if not score_match:
            return None
        score1 = int(score_match.group(1))
        score2 = int(score_match.group(2))

        before = no_venue[: score_match.start()].strip()
        after = no_venue[score_match.end() :].strip()
        team1_parts = [part.strip() for part in re.split(r"\s{2,}", before) if part.strip()]
        team2_parts = [part.strip() for part in re.split(r"\s{2,}", after) if part.strip()]
        if not team1_parts or not team2_parts:
            return None
        team1_candidate = WorldCupHistoryConnector._strip_leading_match_metadata(team1_parts[-1])
        team1 = WorldCupHistoryConnector._extract_team_name(team1_candidate)
        team2 = WorldCupHistoryConnector._extract_team_name(team2_parts[-1])
        if not team1 or not team2:
            return None
        return team1, team2, score1, score2

    @staticmethod
    def _strip_leading_match_metadata(text: str) -> str:
        cleaned = str(text).strip()
        cleaned = re.sub(r"^\(\d+\)\s*", "", cleaned)
        month_pattern = (
            r"(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
            r"jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
        )
        cleaned = re.sub(rf"^\d{{1,2}}\s+{month_pattern}\s+", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(rf"^{month_pattern}\s+\d{{1,2}}\s+", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(rf"^{month_pattern}/\d{{1,2}}\s+", "", cleaned, flags=re.IGNORECASE)
        return cleaned.strip()

    @staticmethod
    def _extract_team_name(text: str) -> str | None:
        cleaned = str(text)
        cleaned = re.sub(r"\([^)]*\)", " ", cleaned)
        cleaned = re.sub(r"a\.?\s*e\.?\s*t\.?", " ", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"pen(?:s|alties)?\.?", " ", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\b(agg\.?|after extra time|after penalties|won|win)\b", " ", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        match = re.search(r"([A-Za-z][A-Za-z .'\-]*[A-Za-z])$", cleaned)
        if not match:
            return None
        return match.group(1).strip()

    @staticmethod
    def _parse_event_date(year: int, final_line: str | None) -> str:
        if not final_line:
            return f"{year}-12-31"
        line = final_line.lower()
        slash_pattern = re.search(r"(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*/(\d{1,2})", line)
        if slash_pattern:
            month = MONTHS[slash_pattern.group(1)]
            day = int(slash_pattern.group(2))
            return f"{year}-{month:02d}-{day:02d}"
        day_month_pattern = re.search(
            r"\b(\d{1,2})\s+("
            r"january|february|march|april|may|june|july|august|september|october|november|december|"
            r"jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec"
            r")\b",
            line,
        )
        if day_month_pattern:
            day = int(day_month_pattern.group(1))
            month = MONTHS[day_month_pattern.group(2)]
            return f"{year}-{month:02d}-{day:02d}"
        return f"{year}-12-31"

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

    def _extract_top4_from_cup_text(self, year: int, text: str) -> tuple[list[tuple[str, int]], str]:
        if year in SPECIAL_TOP4:
            return SPECIAL_TOP4[year], f"{year}-12-31"

        lines = text.splitlines()
        final_line: str | None = None
        third_line: str | None = None
        in_final = False
        in_third = False
        for line in lines:
            stripped = line.strip()
            lowered = stripped.lower()
            heading = re.sub(r"\s+", " ", lowered).strip()
            heading = re.sub(r"^#+\s*", "", heading).strip()
            heading = heading.split("##", 1)[0].strip()
            heading = heading.split(" #", 1)[0].strip()
            heading_norm = heading.replace("-", " ")

            # Skip schedule/header rows like "Final | Sun Dec/18".
            if "|" in heading:
                continue

            if heading_norm == "final":
                in_final = True
                in_third = False
                continue
            is_third_heading = ("third" in heading_norm or "3rd" in heading_norm) and (
                "place" in heading_norm or "play off" in heading_norm or "playoff" in heading_norm
            )
            if is_third_heading:
                in_third = True
                in_final = False
                continue
            if stripped.startswith("("):
                if in_final and not final_line:
                    final_line = stripped
                    in_final = False
                    continue
                if in_third and not third_line:
                    third_line = stripped
                    in_third = False
                    continue

        if not final_line:
            return [], f"{year}-12-31"

        parsed_final = self._parse_match_line(final_line)
        if not parsed_final:
            return [], self._parse_event_date(year, final_line)

        final_team1, final_team2, final_score1, final_score2 = parsed_final
        winner = final_team1 if final_score1 > final_score2 else final_team2
        runner_up = final_team2 if winner == final_team1 else final_team1

        top4: list[tuple[str, int]] = [(winner, 1), (runner_up, 2)]
        if third_line:
            parsed_third = self._parse_match_line(third_line)
            if parsed_third:
                team1, team2, score1, score2 = parsed_third
                third = team1 if score1 > score2 else team2
                fourth = team2 if third == team1 else team1
                top4.extend([(third, 3), (fourth, 4)])

        event_date = self._parse_event_date(year, final_line)
        return top4, event_date

    def parse(self, raw_paths: list[Path], season_year: int) -> dict[str, pd.DataFrame]:
        del season_year
        seed_csv = next((path for path in raw_paths if path.suffix.lower() == ".csv"), None)
        if seed_csv:
            seed = pd.read_csv(seed_csv)
            annual_rows = seed.to_dict(orient="records")
        else:
            annual_rows: list[dict[str, Any]] = []
            for path in sorted(raw_paths):
                match = re.match(r"cup_(\d{4})\.txt", path.name)
                if not match:
                    continue
                year = int(match.group(1))
                text = path.read_text(encoding="utf-8")
                top4, event_date = self._extract_top4_from_cup_text(year, text)
                for country_name, rank in top4:
                    annual_rows.append(
                        {
                            "year": year,
                            "rank": rank,
                            "country_name": country_name,
                            "event_date": event_date,
                        }
                    )

        annual_df = pd.DataFrame(annual_rows)
        annual_df = annual_df.drop_duplicates(subset=["year", "rank", "country_name"])
        annual_df = annual_df.sort_values(["year", "rank", "country_name"]).reset_index(drop=True)
        annual_df = annual_df.loc[annual_df["rank"] <= 4].copy()

        timestamp = utc_now_iso()
        sport_id = slugify("Football")
        discipline_id = slugify("FIFA World Cup Final Ranking")
        competition_id = "fifa_world_cup"

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
                    "discipline_name": "FIFA World Cup Final Ranking",
                    "discipline_slug": discipline_id,
                    "sport_id": sport_id,
                    "confidence": 1.0,
                    "mapping_source": "connector_world_cup_history",
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
                    "name": "FIFA World Cup",
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
            event_id = f"fifa_world_cup_{str(int(year))[-2:]}"
            event_date = group["event_date"].iloc[0]
            events_rows.append(
                {
                    "event_id": event_id,
                    "competition_id": competition_id,
                    "discipline_id": discipline_id,
                    "gender": "men",
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
                        "score_raw": f"world_cup_final_rank={rank}",
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
            conn.execute("DELETE FROM disciplines WHERE mapping_source = 'connector_world_cup_history'")
            conn.commit()

        db.upsert_dataframe("countries", payload.get("countries", pd.DataFrame()), ["country_id"])
        db.upsert_dataframe("sports", payload.get("sports", pd.DataFrame()), ["sport_id"])
        db.upsert_dataframe("disciplines", payload.get("disciplines", pd.DataFrame()), ["discipline_id"])
        db.upsert_dataframe("competitions", payload.get("competitions", pd.DataFrame()), ["competition_id"])
        db.upsert_dataframe("events", payload.get("events", pd.DataFrame()), ["event_id"])
        db.upsert_dataframe("participants", payload.get("participants", pd.DataFrame()), ["participant_id"])
        db.upsert_dataframe("results", payload.get("results", pd.DataFrame()), ["event_id", "participant_id"])
