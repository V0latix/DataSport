from __future__ import annotations

import argparse
import re
import unicodedata
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag


BASE_DIR = Path(__file__).resolve().parent
SEED_PATH = BASE_DIR / "uci_track_world_championships_top3_seed.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder)"}
COMPETITION_ID = "uci_track_world_championships"
COMPETITION_NAME = "UCI Track Cycling World Championships"
START_YEAR = 2000
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}

COUNTRY_OVERRIDES = {
    "great britain": "GBR",
    "united kingdom": "GBR",
    "south korea": "KOR",
    "north korea": "PRK",
    "chinese taipei": "TPE",
    "soviet union": "URS",
    "east germany": "GDR",
    "west germany": "FRG",
    "czechoslovakia": "TCH",
    "yugoslavia": "YUG",
    "russia": "RUS",
    "russian cycling federation": "RUS",
    "individual neutral athletes": "AIN",
    "neutral athletes": "AIN",
}
COUNTRY_CODE_NORMALIZATION = {
    "CRO": "HRV",
    "DEN": "DNK",
    "GRE": "GRC",
    "LAT": "LVA",
    "NED": "NLD",
    "SLO": "SVN",
    "SUI": "CHE",
}

DISCIPLINE_CATALOG = {
    "track-sprint": {"name": "Track Sprint", "participant_type": "athlete"},
    "track-team-sprint": {"name": "Track Team Sprint", "participant_type": "team"},
    "track-keirin": {"name": "Track Keirin", "participant_type": "athlete"},
    "track-individual-pursuit": {"name": "Track Individual Pursuit", "participant_type": "athlete"},
    "track-team-pursuit": {"name": "Track Team Pursuit", "participant_type": "team"},
    "track-points-race": {"name": "Track Points Race", "participant_type": "athlete"},
    "track-scratch": {"name": "Track Scratch", "participant_type": "athlete"},
    "track-madison": {"name": "Track Madison", "participant_type": "team"},
    "track-omnium": {"name": "Track Omnium", "participant_type": "athlete"},
    "track-elimination-race": {"name": "Track Elimination Race", "participant_type": "athlete"},
    "track-time-trial-1km": {"name": "Track 1 km Time Trial", "participant_type": "athlete"},
    "track-time-trial-500m": {"name": "Track 500 m Time Trial", "participant_type": "athlete"},
}


def normalize(text: str) -> str:
    value = unicodedata.normalize("NFKD", str(text))
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = value.replace("’", "'")
    value = re.sub(r"\s+", " ", value).strip().lower()
    return value


def clean_text(text: str) -> str:
    cleaned = re.sub(r"\[[^\]]+\]", "", str(text))
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def resolve_country_code(name: str) -> str | None:
    candidate = clean_text(name)
    if not candidate:
        return None
    if re.fullmatch(r"[A-Z]{3}", candidate):
        return COUNTRY_CODE_NORMALIZATION.get(candidate, candidate)

    norm = normalize(candidate)
    alias = COUNTRY_OVERRIDES.get(norm)
    if alias:
        return COUNTRY_CODE_NORMALIZATION.get(alias, alias)

    try:
        import pycountry

        country = pycountry.countries.lookup(candidate)
        code = getattr(country, "alpha_3", None)
        if code:
            return COUNTRY_CODE_NORMALIZATION.get(code, code)
    except Exception:
        pass

    return None


def canonical_country_name(country_code: str, fallback_name: str) -> str:
    try:
        import pycountry

        country = pycountry.countries.get(alpha_3=country_code)
        if country:
            return str(getattr(country, "name"))
    except Exception:
        pass
    return clean_text(fallback_name) or country_code


def detect_gender(text: str) -> str | None:
    lowered = normalize(text)
    if "women" in lowered:
        return "women"
    if "men" in lowered:
        return "men"
    return None


def normalize_event_label(raw_event: str) -> str:
    label = clean_text(raw_event)
    label = re.sub(r"\s*details$", "", label, flags=re.IGNORECASE)
    label = re.sub(r"^Men'?s\s+", "", label, flags=re.IGNORECASE)
    label = re.sub(r"^Women'?s\s+", "", label, flags=re.IGNORECASE)
    label = re.sub(r"^Men\s+", "", label, flags=re.IGNORECASE)
    label = re.sub(r"^Women\s+", "", label, flags=re.IGNORECASE)
    return clean_text(label)


def map_event_to_discipline(event_label: str, gender: str) -> tuple[str, str, str] | None:
    normalized = normalize(event_label)

    if "team sprint" in normalized:
        key = "track-team-sprint"
    elif "team pursuit" in normalized:
        key = "track-team-pursuit"
    elif "individual pursuit" in normalized:
        key = "track-individual-pursuit"
    elif normalized == "pursuit":
        key = "track-individual-pursuit"
    elif "keirin" in normalized:
        key = "track-keirin"
    elif normalized == "sprint" or normalized.endswith(" sprint"):
        key = "track-sprint"
    elif "points race" in normalized:
        key = "track-points-race"
    elif normalized == "scratch" or " scratch " in f" {normalized} ":
        key = "track-scratch"
    elif "madison" in normalized:
        key = "track-madison"
    elif "omnium" in normalized:
        key = "track-omnium"
    elif "elimination race" in normalized or normalized == "elimination":
        key = "track-elimination-race"
    elif "500 m time trial" in normalized or "500m time trial" in normalized:
        key = "track-time-trial-500m"
    elif "1 km time trial" in normalized or "1km time trial" in normalized or "kilometre time trial" in normalized:
        key = "track-time-trial-1km"
    elif "time trial" in normalized:
        key = "track-time-trial-500m" if gender == "women" else "track-time-trial-1km"
    else:
        return None

    meta = DISCIPLINE_CATALOG[key]
    return key, meta["name"], meta["participant_type"]


def extract_country_from_medal_cell(cell: Tag) -> tuple[str | None, str | None]:
    for link in cell.find_all("a"):
        for candidate in (link.get("title", ""), link.get_text(" ", strip=True)):
            code = resolve_country_code(candidate)
            if code:
                return canonical_country_name(code, str(candidate)), code

    raw = clean_text(cell.get_text(" ", strip=True))
    parts = raw.split(" ")
    for idx in range(len(parts)):
        suffix = " ".join(parts[idx:])
        code = resolve_country_code(suffix)
        if code:
            return canonical_country_name(code, suffix), code
    for idx in range(len(parts), 0, -1):
        prefix = " ".join(parts[:idx])
        code = resolve_country_code(prefix)
        if code:
            return canonical_country_name(code, prefix), code
    return None, None


def extract_individual_name(cell: Tag, country_code: str | None) -> str | None:
    for link in cell.find_all("a"):
        text = clean_text(link.get_text(" ", strip=True))
        if not text:
            continue
        if country_code and resolve_country_code(link.get("title", "") or text) == country_code:
            continue
        return text

    raw = clean_text(cell.get_text(" ", strip=True))
    if not raw:
        return None
    if country_code:
        country_name = canonical_country_name(country_code, country_code)
        raw = re.sub(rf"\b{re.escape(country_name)}\b", "", raw, flags=re.IGNORECASE).strip()
    return clean_text(raw) or None


def extract_medal_entries(cell: Tag, participant_type: str) -> list[tuple[str, str, str]]:
    links = cell.find_all("a")

    if participant_type == "team":
        team_entries: list[tuple[str, str, str]] = []
        for link in links:
            for candidate in (link.get("title", ""), link.get_text(" ", strip=True)):
                code = resolve_country_code(candidate)
                if not code:
                    continue
                country_name = canonical_country_name(code, str(candidate))
                entry = (country_name, country_name, code)
                if entry not in team_entries:
                    team_entries.append(entry)
                break
        if team_entries:
            return team_entries

    else:
        entries: list[tuple[str, str, str]] = []
        current_name: str | None = None
        for link in links:
            link_text = clean_text(link.get_text(" ", strip=True))
            link_title = clean_text(link.get("title", ""))
            if not link_text and not link_title:
                continue

            code = resolve_country_code(link_title) or resolve_country_code(link_text)
            if code:
                country_name = canonical_country_name(code, link_title or link_text)
                participant_name = current_name or extract_individual_name(cell, code) or country_name
                entry = (participant_name, country_name, code)
                if entry not in entries:
                    entries.append(entry)
                current_name = None
            else:
                if link_text:
                    current_name = link_text
        if entries:
            return entries

    country_name, country_code = extract_country_from_medal_cell(cell)
    if not country_name or not country_code:
        return []

    if participant_type == "team":
        return [(country_name, country_name, country_code)]
    participant_name = extract_individual_name(cell, country_code)
    if not participant_name:
        return []
    return [(participant_name, country_name, country_code)]


def table_default_gender(table: Tag) -> str | None:
    prev_heading = table.find_previous(["h2", "h3", "h4"])
    if prev_heading is not None:
        text = clean_text(prev_heading.get_text(" ", strip=True))
        gender = detect_gender(text)
        if gender:
            return gender
    return None


def parse_edition(year: int, html: str, source_url: str) -> tuple[list[dict[str, Any]], set[str]]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict[str, Any]] = []
    unknown_events: set[str] = set()

    for table in soup.find_all("table"):
        headers = [clean_text(th.get_text(" ", strip=True)) for th in table.select("tr th")[:4]]
        if headers != ["Event", "Gold", "Silver", "Bronze"]:
            continue

        current_gender = table_default_gender(table)
        for row in table.find_all("tr")[1:]:
            cells = row.find_all(["th", "td"], recursive=False)
            if not cells:
                continue

            row_label = clean_text(cells[0].get_text(" ", strip=True))
            if len(cells) < 4:
                section_gender = detect_gender(row_label)
                if section_gender:
                    current_gender = section_gender
                continue

            row_gender = detect_gender(row_label) or current_gender
            if row_gender not in {"men", "women"}:
                continue

            event_label = normalize_event_label(row_label)
            mapped = map_event_to_discipline(event_label, row_gender)
            if mapped is None:
                unknown_events.add(event_label)
                continue
            discipline_key, discipline_name, participant_type = mapped

            if len(cells) >= 7:
                medal_cells = ((1, cells[1]), (2, cells[3]), (3, cells[5]))
            else:
                medal_cells = ((1, cells[1]), (2, cells[2]), (3, cells[3]))

            for rank, medal_cell in medal_cells:
                medal_entries = extract_medal_entries(medal_cell, participant_type)
                for participant_name, country_name, country_code in medal_entries:
                    rows.append(
                        {
                            "competition_id": COMPETITION_ID,
                            "competition_name": COMPETITION_NAME,
                            "year": year,
                            "event_date": f"{year}-12-31",
                            "discipline_key": discipline_key,
                            "discipline_name": discipline_name,
                            "gender": row_gender,
                            "rank": rank,
                            "medal": RANK_TO_MEDAL[rank],
                            "participant_type": participant_type,
                            "participant_name": participant_name,
                            "country_name": country_name,
                            "country_code": country_code,
                            "source_url": source_url,
                        }
                    )

    return rows, unknown_events


def main() -> None:
    parser = argparse.ArgumentParser(description="Build UCI track cycling world championships top-3 seed.")
    parser.add_argument("--year", type=int, default=2025, help="Last year to include (default: 2025)")
    args = parser.parse_args()
    max_year = max(START_YEAR, int(args.year))

    all_rows: list[dict[str, Any]] = []
    all_unknown_events: set[str] = set()
    fetched_years: list[int] = []

    for year in range(START_YEAR, max_year + 1):
        source_url = f"https://en.wikipedia.org/wiki/{year}_UCI_Track_Cycling_World_Championships"
        response = requests.get(source_url, timeout=40, headers=HEADERS)
        if response.status_code == 404:
            continue
        response.raise_for_status()
        rows, unknown_events = parse_edition(year, response.text, source_url)
        if not rows:
            raise RuntimeError(f"No podium rows parsed for {year} ({source_url})")
        all_rows.extend(rows)
        all_unknown_events.update(unknown_events)
        fetched_years.append(year)

    if all_unknown_events:
        sample = sorted(all_unknown_events)[:50]
        raise RuntimeError(f"Unmapped track events detected: {sample}")

    frame = pd.DataFrame(all_rows)
    if frame.empty:
        raise RuntimeError("No rows parsed for UCI track cycling world championships seed.")

    frame["year"] = pd.to_numeric(frame["year"], errors="coerce")
    frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce")
    frame = frame.dropna(subset=["year", "rank", "participant_name", "country_code"]).copy()
    frame["year"] = frame["year"].astype(int)
    frame["rank"] = frame["rank"].astype(int)

    frame = frame.drop_duplicates(
        subset=[
            "competition_id",
            "year",
            "discipline_key",
            "gender",
            "rank",
            "participant_name",
            "country_code",
        ]
    )
    frame = frame.sort_values(
        ["year", "gender", "discipline_key", "rank", "participant_name"]
    ).reset_index(drop=True)

    profiles = (
        frame.groupby(["year", "discipline_key", "gender"])["rank"]
        .apply(lambda s: tuple(sorted(s.tolist())))
        .to_dict()
    )
    allowed_profiles = {
        (1, 2, 3),
        (1, 2),
        (1, 2, 2),
        (1, 3),
        (1, 2, 3, 3),
    }
    invalid_profiles = {k: v for k, v in profiles.items() if v not in allowed_profiles}
    if invalid_profiles:
        sample = dict(list(invalid_profiles.items())[:40])
        raise RuntimeError(f"Unexpected rank profile(s), expected strict top3: {sample}")

    frame.to_csv(SEED_PATH, index=False)

    events = frame[["year", "discipline_key", "gender"]].drop_duplicates().shape[0]
    print(f"[seed] years={min(fetched_years)}-{max(fetched_years)} fetched={len(fetched_years)}")
    print(f"[seed] rows={len(frame)} events={events} disciplines={frame['discipline_key'].nunique()}")
    print(f"[seed] wrote {SEED_PATH}")


if __name__ == "__main__":
    main()
