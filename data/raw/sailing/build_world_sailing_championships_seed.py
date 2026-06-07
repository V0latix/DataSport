from __future__ import annotations

import argparse
import io
import re
from pathlib import Path
from typing import Any

import pandas as pd
import requests


BASE_DIR = Path(__file__).resolve().parent
SEED_PATH = BASE_DIR / "world_sailing_championships_top3_seed.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder)"}

COMPETITION_ID = "world_sailing_championships"
COMPETITION_NAME = "Sailing World Championships"
START_YEAR = 2001
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}

PAGES = {
    2003: "https://en.wikipedia.org/wiki/2003_ISAF_Sailing_World_Championships",
    2007: "https://en.wikipedia.org/wiki/2007_ISAF_Sailing_World_Championships",
    2011: "https://en.wikipedia.org/wiki/2011_ISAF_Sailing_World_Championships",
    2014: "https://en.wikipedia.org/wiki/2014_ISAF_Sailing_World_Championships",
    2018: "https://en.wikipedia.org/wiki/2018_Sailing_World_Championships",
    2023: "https://en.wikipedia.org/wiki/2023_Sailing_World_Championships",
}

EVENT_CONFIG = {
    "Men's 470": ("470", "470", "men"),
    "Women's 470": ("470", "470", "women"),
    "470": ("470", "470", "mixed"),
    "49er": ("49er", "49er", "men"),
    "49er FX": ("49er FX", "49er FX", "women"),
    "Europe": ("Europe", "Europe", "women"),
    "Finn": ("Finn", "Finn", "men"),
    "Laser": ("Laser", "Laser", "men"),
    "Laser Radial": ("Laser Radial", "Laser Radial", "women"),
    "ILCA 7": ("ILCA 7", "ILCA 7", "men"),
    "ILCA 6": ("ILCA 6", "ILCA 6", "women"),
    "Men's Mistral": ("Mistral", "Mistral", "men"),
    "Women's Mistral": ("Mistral", "Mistral", "women"),
    "Men's RS:X": ("RS:X", "RS:X", "men"),
    "Women's RS:X": ("RS:X", "RS:X", "women"),
    "Men's iQFoil": ("iQFoil", "iQFoil", "men"),
    "Women's iQFoil": ("iQFoil", "iQFoil", "women"),
    "Star": ("Star", "Star", "men"),
    "Tornado": ("Tornado", "Tornado", "mixed"),
    "Yngling": ("Yngling", "Yngling", "women"),
    "Elliott 6m": ("Elliott 6m", "Elliott 6m", "women"),
    "Nacra 17": ("Nacra 17", "Nacra 17", "mixed"),
    "Men's Formula Kite": ("Formula Kite", "Formula Kite", "men"),
    "Women's Formula Kite": ("Formula Kite", "Formula Kite", "women"),
    "Men's Hansa 303": ("Hansa 303", "Hansa 303", "men"),
    "Women's Hansa 303": ("Hansa 303", "Hansa 303", "women"),
    "2.4 Metre": ("2.4 Metre", "2.4 Metre", "mixed"),
    "RS Venture Connect": ("RS Venture Connect", "RS Venture Connect", "mixed"),
}

COUNTRY_CODE_NORMALIZATION = {
    "CRO": "HRV",
    "DEN": "DNK",
    "GER": "DEU",
    "GRE": "GRC",
    "NED": "NLD",
    "POR": "PRT",
    "SLO": "SVN",
    "SUI": "CHE",
}
COUNTRY_NAME_ALIASES = {
    "Great Britain": "GBR",
    "Russia": "RUS",
    "Czech Republic": "CZE",
    "Hong Kong": "HKG",
    "Chinese Taipei": "TPE",
    "Singapore": "SGP",
    "United States": "USA",
    "United States of America": "USA",
    "South Korea": "KOR",
}
COUNTRY_NAME_OVERRIDES = {
    "GBR": "United Kingdom",
    "HKG": "Hong Kong",
    "TPE": "Chinese Taipei",
}


def clean_text(value: object) -> str:
    text = str(value or "")
    text = text.replace("\xa0", " ")
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = text.replace("*", "")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_country_code(code: str) -> str:
    value = clean_text(code).upper()
    return COUNTRY_CODE_NORMALIZATION.get(value, value)


def country_aliases() -> dict[str, str]:
    aliases = dict(COUNTRY_NAME_ALIASES)
    try:
        import pycountry

        for country in pycountry.countries:
            aliases[str(country.name)] = normalize_country_code(str(country.alpha_3))
            common_name = getattr(country, "common_name", None)
            if common_name:
                aliases[str(common_name)] = normalize_country_code(str(country.alpha_3))
    except Exception:
        pass
    return aliases


COUNTRY_ALIASES = country_aliases()
COUNTRY_NAMES_BY_LENGTH = sorted(COUNTRY_ALIASES, key=len, reverse=True)


def country_name(country_code: str) -> str:
    code = normalize_country_code(country_code)
    override = COUNTRY_NAME_OVERRIDES.get(code)
    if override:
        return override
    try:
        import pycountry

        country = pycountry.countries.get(alpha_3=code)
        if country is not None:
            return str(getattr(country, "name"))
    except Exception:
        pass
    return code


def discipline_id(class_name: str) -> str:
    return f"sailing-{slug_token(class_name)}"


def slug_token(value: str) -> str:
    text = clean_text(value).lower()
    text = text.replace(":", "")
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text


def event_name(raw_event: object) -> str:
    text = clean_text(raw_event)
    text = text.replace(" details", "").replace("details", "")
    return clean_text(text)


def parse_entry(value: object) -> tuple[str, str, str]:
    text = clean_text(value)
    iso_match = re.match(r"^(?P<entry>.+?)\s*\((?P<code>[A-Z]{3})\)$", text)
    if iso_match:
        code = normalize_country_code(iso_match.group("code"))
        return clean_text(iso_match.group("entry")), country_name(code), code

    candidates: list[tuple[int, str, str]] = []
    for candidate_country in COUNTRY_NAMES_BY_LENGTH:
        if text.startswith(f"{candidate_country} "):
            candidates.append((len(candidate_country), candidate_country, clean_text(text[len(candidate_country) :])))
        if text.endswith(f" {candidate_country}"):
            candidates.append((len(candidate_country), candidate_country, clean_text(text[: -len(candidate_country)])))
    candidates = [candidate for candidate in candidates if candidate[2]]
    if not candidates:
        raise RuntimeError(f"Could not parse sailing entry/country value: {text!r}")

    _, matched_country, entry = sorted(candidates, reverse=True)[0]
    code = normalize_country_code(COUNTRY_ALIASES[matched_country])
    return entry, country_name(code), code


def fetch_medal_table(source_url: str) -> pd.DataFrame:
    response = requests.get(source_url, headers=HEADERS, timeout=45)
    response.raise_for_status()
    tables = pd.read_html(io.StringIO(response.text))
    for table in tables:
        if {"Event", "Gold", "Silver", "Bronze"}.issubset(set(table.columns)):
            return table[["Event", "Gold", "Silver", "Bronze"]].copy()
    raise RuntimeError(f"No event medalists table found at {source_url}")


def build_rows(max_year: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for year, source_url in PAGES.items():
        if year < START_YEAR or year > int(max_year):
            continue
        table = fetch_medal_table(source_url)
        for row in table.itertuples(index=False):
            raw_event_name = event_name(getattr(row, "Event"))
            if raw_event_name not in EVENT_CONFIG:
                raise RuntimeError(f"Unsupported Sailing Worlds event name for {year}: {raw_event_name!r}")

            class_name, discipline_name, gender = EVENT_CONFIG[raw_event_name]
            for rank, medal_column in ((1, "Gold"), (2, "Silver"), (3, "Bronze")):
                participant_name, resolved_country_name, country_code = parse_entry(getattr(row, medal_column))
                rows.append(
                    {
                        "competition_id": COMPETITION_ID,
                        "competition_name": COMPETITION_NAME,
                        "year": year,
                        "event_date": f"{year}-12-31",
                        "discipline_key": discipline_id(class_name),
                        "discipline_name": discipline_name,
                        "class_key": slug_token(class_name),
                        "event_name": raw_event_name,
                        "gender": gender,
                        "rank": rank,
                        "medal": RANK_TO_MEDAL[rank],
                        "participant_type": "team",
                        "participant_name": participant_name,
                        "country_name": resolved_country_name,
                        "country_code": country_code,
                        "source_url": source_url,
                    }
                )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Sailing World Championships class podium seed.")
    parser.add_argument("--max-year", type=int, default=2026)
    parser.add_argument("--out", type=Path, default=SEED_PATH)
    args = parser.parse_args()

    rows = build_rows(max(START_YEAR, int(args.max_year)))
    if not rows:
        raise RuntimeError("No Sailing World Championships rows extracted.")

    frame = pd.DataFrame(rows)
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype(int)
    frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce").astype(int)
    frame = frame.loc[frame["year"] > 2000].copy()
    frame = frame.sort_values(["year", "class_key", "gender", "rank", "participant_name"]).reset_index(drop=True)

    profiles = (
        frame.groupby(["year", "class_key", "gender"])["rank"]
        .apply(lambda values: tuple(sorted(int(value) for value in values.tolist())))
        .to_dict()
    )
    invalid_profiles = {key: value for key, value in profiles.items() if value != (1, 2, 3)}
    if invalid_profiles:
        sample = dict(list(invalid_profiles.items())[:30])
        raise RuntimeError(f"Unexpected Sailing Worlds rank profile(s): {sample}")

    duplicates = frame.loc[
        frame.duplicated(subset=["year", "class_key", "gender", "participant_name", "country_code"], keep=False)
    ]
    if not duplicates.empty:
        sample = duplicates[["year", "class_key", "gender", "participant_name", "country_code"]].head(20).to_dict(
            "records"
        )
        raise RuntimeError(f"Duplicate entry in same Sailing Worlds event: {sample}")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(args.out, index=False)

    years = sorted(int(year) for year in frame["year"].unique().tolist())
    event_count = frame[["year", "class_key", "gender"]].drop_duplicates().shape[0]
    print(
        f"[seed] world_sailing_championships rows={len(frame)} "
        f"years={years[0]}-{years[-1]} events={event_count} out={args.out}"
    )


if __name__ == "__main__":
    main()
