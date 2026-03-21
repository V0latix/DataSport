from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import time
from typing import Any

import pandas as pd
import requests


API_BASE = "https://api.jolpi.ca/ergast/f1"
SEED_PATH = Path(__file__).resolve().parent / "formula1_world_standings_top10_seed.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder)"}

DRIVERS_COMPETITION_ID = "formula1_drivers_world_championship"
CONSTRUCTORS_COMPETITION_ID = "formula1_constructors_world_championship"
DISCIPLINE_ID = "formula-one"
DISCIPLINE_NAME = "Formula One"

NATIONALITY_TO_ISO3 = {
    "american": "USA",
    "argentine": "ARG",
    "australian": "AUS",
    "austrian": "AUT",
    "belgian": "BEL",
    "brazilian": "BRA",
    "british": "GBR",
    "canadian": "CAN",
    "chinese": "CHN",
    "colombian": "COL",
    "danish": "DNK",
    "dutch": "NLD",
    "finnish": "FIN",
    "french": "FRA",
    "german": "DEU",
    "indian": "IND",
    "irish": "IRL",
    "italian": "ITA",
    "japanese": "JPN",
    "malaysian": "MYS",
    "mexican": "MEX",
    "monegasque": "MCO",
    "new zealander": "NZL",
    "polish": "POL",
    "russian": "RUS",
    "spanish": "ESP",
    "swiss": "CHE",
    "thai": "THA",
    "venezuelan": "VEN",
}


def _country_name_from_iso3(code: str) -> str:
    try:
        import pycountry

        country = pycountry.countries.get(alpha_3=code)
        if country is not None:
            return str(getattr(country, "name", code))
    except Exception:
        pass
    return code


def _resolve_country(nationality: str) -> tuple[str | None, str]:
    key = str(nationality or "").strip().lower()
    if not key:
        return None, ""

    code = NATIONALITY_TO_ISO3.get(key)
    if code:
        return code, _country_name_from_iso3(code)

    try:
        import pycountry

        country = pycountry.countries.lookup(nationality)
        iso3 = str(getattr(country, "alpha_3", "")).upper().strip() or None
        if iso3:
            return iso3, _country_name_from_iso3(iso3)
    except Exception:
        pass

    return None, str(nationality).strip()


def _request_json(url: str, retries: int = 5) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=45)
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait_seconds = float(retry_after) if retry_after else min(2.0 * (attempt + 1), 12.0)
                time.sleep(wait_seconds)
                continue
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise RuntimeError(f"Unexpected payload format from {url}")
            return payload
        except Exception as exc:  # noqa: PERF203
            last_error = exc
            time.sleep(min(2.0 * (attempt + 1), 12.0))
    raise RuntimeError(f"Failed to fetch JSON from {url}: {last_error}")


def _extract_driver_rows(year: int) -> list[dict[str, Any]]:
    payload = _request_json(f"{API_BASE}/{year}/driverstandings/?format=json")
    standings_lists = (
        payload.get("MRData", {})
        .get("StandingsTable", {})
        .get("StandingsLists", [])
    )
    if not standings_lists:
        return []

    standings = standings_lists[0]
    round_value = str(standings.get("round") or "").strip()
    entries = standings.get("DriverStandings", [])
    rows: list[dict[str, Any]] = []

    for entry in entries:
        rank = pd.to_numeric(entry.get("position"), errors="coerce")
        if pd.isna(rank):
            continue
        rank_int = int(rank)
        if rank_int < 1 or rank_int > 10:
            continue

        driver = entry.get("Driver") or {}
        participant_ref = str(driver.get("driverId") or "").strip().lower()
        given_name = str(driver.get("givenName") or "").strip()
        family_name = str(driver.get("familyName") or "").strip()
        display_name = f"{given_name} {family_name}".strip()
        if not participant_ref or not display_name:
            continue

        nationality = str(driver.get("nationality") or "").strip()
        country_code, country_name = _resolve_country(nationality)
        constructors = entry.get("Constructors") or []
        constructor_name = ""
        if constructors and isinstance(constructors, list):
            constructor_name = str((constructors[0] or {}).get("name") or "").strip()

        rows.append(
            {
                "competition_id": DRIVERS_COMPETITION_ID,
                "competition_name": "Formula 1 World Drivers' Championship",
                "year": year,
                "event_date": f"{year}-12-31",
                "discipline_key": DISCIPLINE_ID,
                "discipline_name": DISCIPLINE_NAME,
                "gender": "mixed",
                "rank": rank_int,
                "participant_type": "athlete",
                "participant_ref": participant_ref,
                "participant_name": display_name,
                "country_code": country_code,
                "country_name": country_name,
                "nationality": nationality,
                "points": pd.to_numeric(entry.get("points"), errors="coerce"),
                "wins": pd.to_numeric(entry.get("wins"), errors="coerce"),
                "team_name": constructor_name,
                "round": round_value,
                "source_url": str(driver.get("url") or "").strip(),
            }
        )
    return rows


def _extract_constructor_rows(year: int) -> list[dict[str, Any]]:
    payload = _request_json(f"{API_BASE}/{year}/constructorstandings/?format=json")
    standings_lists = (
        payload.get("MRData", {})
        .get("StandingsTable", {})
        .get("StandingsLists", [])
    )
    if not standings_lists:
        return []

    standings = standings_lists[0]
    round_value = str(standings.get("round") or "").strip()
    entries = standings.get("ConstructorStandings", [])
    rows: list[dict[str, Any]] = []

    for entry in entries:
        rank = pd.to_numeric(entry.get("position"), errors="coerce")
        if pd.isna(rank):
            continue
        rank_int = int(rank)
        if rank_int < 1 or rank_int > 10:
            continue

        constructor = entry.get("Constructor") or {}
        participant_ref = str(constructor.get("constructorId") or "").strip().lower()
        display_name = str(constructor.get("name") or "").strip()
        if not participant_ref or not display_name:
            continue

        nationality = str(constructor.get("nationality") or "").strip()
        country_code, country_name = _resolve_country(nationality)

        rows.append(
            {
                "competition_id": CONSTRUCTORS_COMPETITION_ID,
                "competition_name": "Formula 1 World Constructors' Championship",
                "year": year,
                "event_date": f"{year}-12-31",
                "discipline_key": DISCIPLINE_ID,
                "discipline_name": DISCIPLINE_NAME,
                "gender": "mixed",
                "rank": rank_int,
                "participant_type": "team",
                "participant_ref": participant_ref,
                "participant_name": display_name,
                "country_code": country_code,
                "country_name": country_name,
                "nationality": nationality,
                "points": pd.to_numeric(entry.get("points"), errors="coerce"),
                "wins": pd.to_numeric(entry.get("wins"), errors="coerce"),
                "team_name": display_name,
                "round": round_value,
                "source_url": str(constructor.get("url") or "").strip(),
            }
        )
    return rows


def build_seed(start_year: int, max_year: int, output: Path) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for year in range(start_year, max_year + 1):
        rows.extend(_extract_driver_rows(year))
        rows.extend(_extract_constructor_rows(year))

    frame = pd.DataFrame(rows)
    if frame.empty:
        raise RuntimeError("Formula 1 standings seed extraction produced no rows.")

    frame = frame.drop_duplicates(
        subset=["competition_id", "year", "rank", "participant_type", "participant_ref"],
        keep="last",
    )
    frame = frame.sort_values(
        ["competition_id", "year", "rank", "participant_name"]
    ).reset_index(drop=True)

    profiles = (
        frame.groupby(["competition_id", "year"])["rank"]
        .apply(lambda s: tuple(sorted(int(v) for v in s.tolist())))
        .to_dict()
    )
    expected_profile = tuple(range(1, 11))
    bad_profiles = {key: value for key, value in profiles.items() if value != expected_profile}
    if bad_profiles:
        sample = dict(list(bad_profiles.items())[:10])
        raise RuntimeError(f"Unexpected F1 rank profiles (expected 1..10): {sample}")

    output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output, index=False)
    return frame


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Formula 1 top-10 annual standings seed (drivers + constructors).")
    parser.add_argument("--start-year", type=int, default=2001)
    parser.add_argument(
        "--max-year",
        type=int,
        default=max(2001, datetime.now(timezone.utc).year - 1),
    )
    parser.add_argument("--output", type=Path, default=SEED_PATH)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    seed = build_seed(start_year=int(args.start_year), max_year=int(args.max_year), output=Path(args.output))
    print(
        f"[f1-seed] wrote {args.output} rows={len(seed)} "
        f"years={seed['year'].min()}-{seed['year'].max()}"
    )
    print("[f1-seed] rows by competition:")
    print(seed.groupby("competition_id").size())


if __name__ == "__main__":
    main()
