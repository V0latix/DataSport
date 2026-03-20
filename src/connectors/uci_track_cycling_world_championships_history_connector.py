from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Any

import pandas as pd

from src.core.db import SQLiteDB
from src.core.utils import slugify, utc_now_iso

from .base import Connector


SEED_FILE = "uci_track_world_championships_top3_seed.csv"
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}
RANK_TO_POINTS = {1: 10.0, 2: 7.0, 3: 5.0}
ALLOWED_PROFILES = {(1, 2, 3), (1, 2), (1, 3)}
COUNTRY_OVERRIDES = {
    "great britain": "GBR",
    "united kingdom": "GBR",
    "russian federation": "RUS",
    "russian cycling federation": "RUS",
    "individual neutral athletes": "AIN",
    "neutral athletes": "AIN",
}


class UciTrackCyclingWorldChampionshipsHistoryConnector(Connector):
    id = "uci_track_cycling_world_championships_history"
    name = "UCI Track Cycling World Championships Historical Podium by Discipline (2000+)"
    source_type = "csv"
    license_notes = (
        "Historical podium seeds curated from public UCI track world championships Wikipedia pages. "
        "Verify downstream redistribution requirements."
    )
    base_url = "https://en.wikipedia.org/wiki/UCI_Track_Cycling_World_Championships"

    def source_row(self) -> dict[str, str]:
        return {
            "source_id": self.id,
            "source_name": self.name,
            "source_type": self.source_type,
            "license_notes": (
                "Historical podium seed from local file "
                "data/raw/cycling/uci_track_world_championships_top3_seed.csv "
                "(curated from yearly UCI track world championships pages since 2000; "
                "disciplines can appear/disappear by year and are preserved as provided)."
            ),
            "base_url": self.base_url,
        }

    def _local_seed_path(self) -> Path:
        return Path(__file__).resolve().parents[2] / "data" / "raw" / "cycling" / SEED_FILE

    @staticmethod
    def _clean_participant_name_for_id(name: str) -> str:
        normalized = re.sub(r"\s+", "_", str(name).strip())
        normalized = re.sub(r"[^0-9A-Za-zÀ-ÖØ-öø-ÿ_-]", "", normalized)
        return normalized or slugify(str(name))

    @staticmethod
    def _event_id(year: int, discipline_key: str, gender: str) -> str:
        return f"uci_track_world_championships_{year}_{discipline_key}_{gender}"

    def _resolve_country_code(self, country_name: str, country_code: str | None) -> str:
        code = str(country_code or "").strip().upper()
        if re.fullmatch(r"[A-Z]{3}", code):
            return code

        raw_name = str(country_name).strip()
        alias = COUNTRY_OVERRIDES.get(raw_name.lower())
        if alias:
            return alias

        try:
            import pycountry

            country = pycountry.countries.lookup(raw_name)
            resolved = getattr(country, "alpha_3", None)
            if resolved:
                return resolved
        except Exception:
            pass

        fallback = slugify(raw_name)[:3].upper()
        return fallback or "UNK"

    def fetch(self, season_year: int, out_dir: Path) -> list[Path]:
        del season_year
        local_seed = self._local_seed_path()
        if not local_seed.exists():
            raise RuntimeError(f"Missing local seed for UCI track worlds: {local_seed}")

        out_file = out_dir / SEED_FILE
        shutil.copy2(local_seed, out_file)
        self._write_json(out_dir / "fetch_meta.json", {"mode": "local_seed", "seed_file": str(local_seed)})
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
            "gender",
            "rank",
            "medal",
            "participant_type",
            "participant_name",
            "country_name",
            "country_code",
        }
        if not required_cols.issubset(set(frame.columns)):
            raise RuntimeError(f"Unsupported UCI track seed format: {list(frame.columns)}")

        frame = frame.copy()
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce")
        frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce")
        frame["event_date"] = pd.to_datetime(frame["event_date"], errors="coerce")
        frame["competition_id"] = frame["competition_id"].fillna("").astype(str).str.strip()
        frame["competition_name"] = frame["competition_name"].fillna("").astype(str).str.strip()
        frame["discipline_key"] = frame["discipline_key"].fillna("").astype(str).str.strip()
        frame["discipline_name"] = frame["discipline_name"].fillna("").astype(str).str.strip()
        frame["gender"] = frame["gender"].fillna("").astype(str).str.strip().str.lower()
        frame["medal"] = frame["medal"].fillna("").astype(str).str.strip().str.lower()
        frame["participant_type"] = frame["participant_type"].fillna("").astype(str).str.strip().str.lower()
        frame["participant_name"] = frame["participant_name"].fillna("").astype(str).str.replace(r"\[[^\]]+\]", "", regex=True)
        frame["participant_name"] = frame["participant_name"].str.strip()
        frame["country_name"] = frame["country_name"].fillna("").astype(str).str.replace(r"\[[^\]]+\]", "", regex=True)
        frame["country_name"] = frame["country_name"].str.strip()
        frame["country_code"] = frame["country_code"].fillna("").astype(str).str.strip().str.upper()

        frame = frame.dropna(subset=["year", "rank", "event_date"])
        frame = frame.loc[frame["year"] <= season_year].copy()
        frame = frame.loc[frame["rank"].between(1, 3)].copy()
        frame = frame.loc[
            (frame["competition_id"] == "uci_track_world_championships")
            & (frame["discipline_key"] != "")
            & (frame["gender"].isin({"men", "women"}))
            & (frame["participant_name"] != "")
            & (frame["country_name"] != "")
        ].copy()

        frame["year"] = frame["year"].astype(int)
        frame["rank"] = frame["rank"].astype(int)
        frame["event_date"] = frame["event_date"].dt.strftime("%Y-%m-%d")
        frame["discipline_id"] = frame["discipline_key"]
        frame["country_id"] = frame.apply(
            lambda row: self._resolve_country_code(str(row["country_name"]), str(row["country_code"])),
            axis=1,
        )
        frame["medal"] = frame.apply(
            lambda row: row["medal"] if row["medal"] in {"gold", "silver", "bronze"} else RANK_TO_MEDAL[int(row["rank"])],
            axis=1,
        )
        frame = frame.drop_duplicates(
            subset=[
                "year",
                "discipline_key",
                "gender",
                "rank",
                "participant_name",
                "country_id",
            ]
        ).sort_values(["year", "discipline_key", "gender", "rank", "participant_name"]).reset_index(drop=True)

        if frame.empty:
            raise RuntimeError("UCI track cycling parsing returned no rows.")

        rank_profiles = (
            frame.groupby(["year", "discipline_key", "gender"])["rank"]
            .apply(lambda series: tuple(sorted(series.tolist())))
            .to_dict()
        )
        invalid_profiles = {
            f"{year}_{discipline_key}_{gender}": profile
            for (year, discipline_key, gender), profile in rank_profiles.items()
            if profile not in ALLOWED_PROFILES
        }
        if invalid_profiles:
            sample = dict(list(invalid_profiles.items())[:40])
            raise RuntimeError(f"Unexpected rank profile(s) for UCI track worlds: {sample}")

        timestamp = utc_now_iso()

        sports_rows = [
            {
                "sport_id": "cycling",
                "sport_name": "Cycling",
                "sport_slug": "cycling",
                "created_at_utc": timestamp,
            }
        ]
        disciplines_rows = []
        for discipline_id, discipline_name in (
            frame[["discipline_id", "discipline_name"]]
            .drop_duplicates()
            .sort_values(["discipline_id"])
            .itertuples(index=False)
        ):
            disciplines_rows.append(
                {
                    "discipline_id": str(discipline_id),
                    "discipline_name": str(discipline_name),
                    "discipline_slug": slugify(str(discipline_name)),
                    "sport_id": "cycling",
                    "confidence": 1.0,
                    "mapping_source": "connector_uci_track_cycling_world_championships_history",
                    "created_at_utc": timestamp,
                }
            )

        competitions_rows = [
            {
                "competition_id": "uci_track_world_championships",
                "sport_id": "cycling",
                "name": frame["competition_name"].iloc[0] if not frame.empty else "UCI Track Cycling World Championships",
                "season_year": None,
                "level": "international_championship",
                "start_date": frame["event_date"].min(),
                "end_date": frame["event_date"].max(),
                "source_id": self.id,
            }
        ]

        events_rows: list[dict[str, Any]] = []
        participants_rows: dict[str, dict[str, Any]] = {}
        countries_rows: dict[str, dict[str, Any]] = {}
        results_rows: list[dict[str, Any]] = []

        for year, discipline_key, discipline_id, gender, event_date in (
            frame[["year", "discipline_key", "discipline_id", "gender", "event_date"]]
            .drop_duplicates()
            .sort_values(["year", "discipline_key", "gender"])
            .itertuples(index=False)
        ):
            events_rows.append(
                {
                    "event_id": self._event_id(int(year), str(discipline_key), str(gender)),
                    "competition_id": "uci_track_world_championships",
                    "discipline_id": str(discipline_id),
                    "gender": str(gender),
                    "event_class": "final_ranking_top3",
                    "event_date": str(event_date),
                }
            )

        for row in frame.itertuples(index=False):
            year = int(row.year)
            discipline_key = str(row.discipline_key)
            gender = str(row.gender)
            event_id = self._event_id(year, discipline_key, gender)
            country_name = str(row.country_name).strip()
            country_id = str(row.country_id).strip().upper()
            participant_name = str(row.participant_name).strip()
            participant_type = str(row.participant_type).strip().lower()
            if participant_type not in {"athlete", "team"}:
                participant_type = "athlete"

            if participant_type == "team":
                participant_id = country_id
                display_name = country_name
            else:
                participant_key = self._clean_participant_name_for_id(participant_name)
                participant_id = f"athlete_{participant_key}_{country_id}"
                display_name = participant_name

            participants_rows[participant_id] = {
                "participant_id": participant_id,
                "type": participant_type,
                "display_name": display_name,
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

            rank = int(row.rank)
            results_rows.append(
                {
                    "event_id": event_id,
                    "participant_id": participant_id,
                    "rank": rank,
                    "medal": str(row.medal),
                    "score_raw": f"discipline={discipline_key};rank={rank}",
                    "points_awarded": RANK_TO_POINTS.get(rank),
                }
            )

        return {
            "countries": pd.DataFrame(countries_rows.values()).drop_duplicates(subset=["country_id"]),
            "sports": pd.DataFrame(sports_rows).drop_duplicates(subset=["sport_id"]),
            "disciplines": pd.DataFrame(disciplines_rows).drop_duplicates(subset=["discipline_id"]),
            "competitions": pd.DataFrame(competitions_rows).drop_duplicates(subset=["competition_id"]),
            "events": pd.DataFrame(events_rows).drop_duplicates(subset=["event_id"]),
            "participants": pd.DataFrame(participants_rows.values()).drop_duplicates(subset=["participant_id"]),
            "results": pd.DataFrame(results_rows).drop_duplicates(subset=["event_id", "participant_id"]),
            "sport_federations": pd.DataFrame(),
        }

    def upsert(self, db: SQLiteDB, payload: dict[str, pd.DataFrame]) -> None:
        countries_df = payload.get("countries", pd.DataFrame())
        participants_df = payload.get("participants", pd.DataFrame())
        disciplines_df = payload.get("disciplines", pd.DataFrame())

        with db.connect() as conn:
            existing_country_ids = {row[0] for row in conn.execute("SELECT country_id FROM countries").fetchall()}
            existing_participant_ids = {
                row[0] for row in conn.execute("SELECT participant_id FROM participants").fetchall()
            }
            existing_discipline_ids = {
                row[0] for row in conn.execute("SELECT discipline_id FROM disciplines").fetchall()
            }

        if not countries_df.empty:
            countries_df = countries_df.loc[~countries_df["country_id"].isin(existing_country_ids)].copy()
        if not participants_df.empty:
            participants_df = participants_df.loc[
                ~participants_df["participant_id"].isin(existing_participant_ids)
            ].copy()

        connector_discipline_ids = sorted(
            set(payload.get("disciplines", pd.DataFrame()).get("discipline_id", pd.Series(dtype=str)).tolist())
        )
        if not disciplines_df.empty:
            disciplines_df = disciplines_df.loc[
                ~disciplines_df["discipline_id"].isin(existing_discipline_ids)
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
            if connector_discipline_ids:
                placeholders = ", ".join("?" for _ in connector_discipline_ids)
                conn.execute(
                    f"""
                    DELETE FROM disciplines
                    WHERE mapping_source = 'connector_uci_track_cycling_world_championships_history'
                      AND discipline_id NOT IN ({placeholders})
                      AND NOT EXISTS (
                          SELECT 1 FROM events e WHERE e.discipline_id = disciplines.discipline_id
                      )
                    """,
                    tuple(connector_discipline_ids),
                )
            conn.commit()

        db.upsert_dataframe("countries", countries_df, ["country_id"])
        db.upsert_dataframe("sports", payload.get("sports", pd.DataFrame()), ["sport_id"])
        db.upsert_dataframe("disciplines", disciplines_df, ["discipline_id"])
        db.upsert_dataframe("competitions", payload.get("competitions", pd.DataFrame()), ["competition_id"])
        db.upsert_dataframe("events", payload.get("events", pd.DataFrame()), ["event_id"])
        db.upsert_dataframe("participants", participants_df, ["participant_id"])
        db.upsert_dataframe("results", payload.get("results", pd.DataFrame()), ["event_id", "participant_id"])
