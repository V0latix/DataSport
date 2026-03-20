from __future__ import annotations

import argparse
import re
import unicodedata
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag


INDEX_URL = "https://en.wikipedia.org/wiki/World_Judo_Championships"
BASE_URL = "https://en.wikipedia.org"
SEED_PATH = Path(__file__).resolve().parent / "world_judo_championships_top3_seed.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder)"}

COMPETITION_ID = "world_judo_championships"
COMPETITION_NAME = "World Judo Championships"
START_YEAR = 2001  # post-2000 scope required by playbook
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}

COUNTRY_OVERRIDES = {
    "great britain": "GBR",
    "south korea": "KOR",
    "north korea": "PRK",
    "russian federation": "RUS",
    "russia": "RUS",
    "ivory coast": "CIV",
    "turkey": "TUR",
    "turkiye": "TUR",
    "chinese taipei": "TPE",
    "kosovo": "KOS",
    "individual neutral athletes": "AIN",
    "international judo federation": "IJF",
    "independent participants a": "KOS",
}
COUNTRY_CODE_NORMALIZATION = {
    "GER": "DEU",
    "GRE": "GRC",
    "NED": "NLD",
    "SUI": "CHE",
    "LAT": "LVA",
    "CRO": "HRV",
    "MGL": "MNG",
}
COUNTRY_NAME_OVERRIDES = {
    "TUR": "Türkiye",
    "TPE": "Chinese Taipei",
    "KOS": "Kosovo",
    "AIN": "Individual Neutral Athletes",
    "IJF": "International Judo Federation",
}


def clean_text(value: str) -> str:
    text = str(value or "")
    text = text.replace("\xa0", " ")
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_text(value: str) -> str:
    text = unicodedata.normalize("NFKD", clean_text(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("’", "'").replace("−", "-")
    return text.lower()


def canonical_country_name(code: str, fallback_name: str) -> str:
    code = str(code).upper().strip()
    if code in COUNTRY_NAME_OVERRIDES:
        return COUNTRY_NAME_OVERRIDES[code]
    try:
        import pycountry

        country = pycountry.countries.get(alpha_3=code)
        if country is not None:
            return str(getattr(country, "name"))
    except Exception:
        pass
    return clean_text(fallback_name) or code


def try_resolve_country(value: str) -> tuple[str, str] | None:
    text = clean_text(value)
    if not text:
        return None

    match = re.search(r"\(([A-Z]{3})\)", text)
    if match:
        code = COUNTRY_CODE_NORMALIZATION.get(match.group(1), match.group(1))
        name = re.sub(r"\([A-Z]{3}\)", "", text).strip(" ,")
        return code, canonical_country_name(code, name)

    norm = normalize_text(text)
    alias = COUNTRY_OVERRIDES.get(norm)
    if alias:
        code = COUNTRY_CODE_NORMALIZATION.get(alias, alias)
        return code, canonical_country_name(code, text)

    if re.fullmatch(r"[A-Z]{3}", text):
        code = COUNTRY_CODE_NORMALIZATION.get(text, text)
        return code, canonical_country_name(code, text)

    try:
        import pycountry

        country = pycountry.countries.lookup(text)
        code = str(getattr(country, "alpha_3"))
        code = COUNTRY_CODE_NORMALIZATION.get(code, code)
        return code, canonical_country_name(code, text)
    except Exception:
        return None


def extract_edition_links(max_year: int) -> dict[int, str]:
    response = requests.get(INDEX_URL, headers=HEADERS, timeout=45)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    mixed_section = soup.find(id="Mixed_competitions")
    if mixed_section is None:
        raise RuntimeError("Unable to find `Mixed_competitions` section on World Judo Championships page.")

    table = mixed_section.find_next("table", class_="wikitable")
    if table is None:
        raise RuntimeError("Unable to find mixed competitions table on World Judo Championships page.")

    links: dict[int, str] = {}
    for row in table.find_all("tr")[1:]:
        cells = row.find_all(["th", "td"])
        if len(cells) < 2:
            continue
        year_text = clean_text(cells[1].get_text(" ", strip=True))
        if not year_text.isdigit():
            continue
        year = int(year_text)
        if year < START_YEAR or year > max_year:
            continue

        link = cells[1].find("a")
        if link is None:
            continue
        href = str(link.get("href") or "").strip()
        if not href or "redlink=1" in href or not href.startswith("/wiki/"):
            continue
        links[year] = urljoin(BASE_URL, href)

    if not links:
        raise RuntimeError(f"No edition links found for years >= {START_YEAR}.")
    return dict(sorted(links.items()))


def detect_gender_for_table(table: Tag) -> str | None:
    for heading in table.find_all_previous(["h4", "h3", "h2"], limit=8):
        text = normalize_text(heading.get_text(" ", strip=True))
        if "women" in text:
            return "women"
        if re.search(r"\bmen\b", text):
            return "men"
        if "mixed" in text or "team" in text:
            return "mixed"
    return None


def is_event_medal_table(table: Tag) -> bool:
    header_row = table.find("tr")
    if header_row is None:
        return False
    headers = [normalize_text(th.get_text(" ", strip=True)) for th in header_row.find_all(["th", "td"])[:6]]
    line = " | ".join(headers)
    return "event" in line and "gold" in line and "silver" in line and "bronze" in line


def canonical_event_label(label: str) -> str:
    text = clean_text(label).replace("−", "-")
    text = re.sub(r"\s*details$", "", text, flags=re.IGNORECASE).strip()
    text = re.sub(r"\((\d+)\s*kg\)", r"(-\1 kg)", text)
    text = re.sub(r"\(\+\s*(\d+)\s*kg\)", r"(+\1 kg)", text)
    text = re.sub(r"\(\-\s*(\d+)\s*kg\)", r"(-\1 kg)", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_weight_token(label: str) -> str | None:
    normalized = canonical_event_label(label)
    match = re.search(r"([+-]?\d+)\s*kg", normalized, flags=re.IGNORECASE)
    if not match:
        return None
    try:
        numeric = int(match.group(1).replace("+", "").replace("-", ""))
        raw = match.group(1).strip()
        if raw.startswith("+"):
            sign = "+"
        elif raw.startswith("-"):
            sign = "-"
        else:
            label_norm = normalize_text(label)
            sign = "+" if "heavyweight" in label_norm and "half-heavyweight" not in label_norm else "-"
        return f"{sign}{numeric}"
    except Exception:
        return None


def parse_athlete_cell(cell: Tag) -> tuple[str, str, str] | None:
    links = cell.find_all("a")

    country_code: str | None = None
    country_name: str | None = None
    country_label_in_cell: str | None = None
    for link in reversed(links):
        for candidate in (link.get("title", ""), link.get_text(" ", strip=True)):
            resolved = try_resolve_country(candidate)
            if resolved is not None:
                country_code, country_name = resolved
                country_label_in_cell = clean_text(candidate)
                break
        if country_code:
            break

    raw_text = clean_text(cell.get_text(" ", strip=True))
    if not raw_text:
        return None

    if country_code is None:
        for size in range(1, 6):
            parts = raw_text.split()
            if len(parts) < size + 1:
                break
            suffix = " ".join(parts[-size:])
            resolved = try_resolve_country(suffix)
            if resolved is not None:
                country_code, country_name = resolved
                country_label_in_cell = clean_text(suffix)
                raw_text = clean_text(" ".join(parts[:-size]))
                break

    if country_code is None:
        match = re.match(r"(.+?)\s*\(\s*([A-Z]{3})\s*\)$", raw_text)
        if match:
            athlete = clean_text(match.group(1))
            code = COUNTRY_CODE_NORMALIZATION.get(match.group(2), match.group(2))
            return athlete, canonical_country_name(code, code), code
        return None

    athlete_name = raw_text
    if country_name:
        athlete_name = re.sub(rf"\b{re.escape(country_name)}\b", "", athlete_name, flags=re.IGNORECASE)
    if country_label_in_cell:
        athlete_name = re.sub(rf"\b{re.escape(country_label_in_cell)}\b", "", athlete_name, flags=re.IGNORECASE)
    athlete_name = re.sub(r"\(\s*[A-Z]{3}\s*\)", "", athlete_name)
    athlete_name = re.sub(r"\s*[,/|-]\s*$", "", athlete_name)
    athlete_name = clean_text(athlete_name)
    if not athlete_name:
        return None
    return athlete_name, str(country_name), str(country_code)


def parse_event_tables_for_edition(year: int, source_url: str) -> list[dict[str, Any]]:
    response = requests.get(source_url, headers=HEADERS, timeout=45)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    rows: list[dict[str, Any]] = []

    for table in soup.find_all("table", class_="wikitable"):
        if not is_event_medal_table(table):
            continue

        table_gender = detect_gender_for_table(table)
        if table_gender not in {"men", "women"}:
            continue

        current_event_label: str | None = None
        current_discipline_key: str | None = None
        current_discipline_name: str | None = None
        previous_silver_not_awarded = False

        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all(["th", "td"], recursive=False)
            if not cells:
                continue

            # Full row: Event + Gold + Silver + Bronze
            if len(cells) >= 4:
                event_label = canonical_event_label(cells[0].get_text(" ", strip=True))
                if not event_label or "team" in normalize_text(event_label):
                    current_event_label = None
                    current_discipline_key = None
                    current_discipline_name = None
                    continue

                weight_token = extract_weight_token(event_label)
                if weight_token is None:
                    current_event_label = None
                    current_discipline_key = None
                    current_discipline_name = None
                    continue

                current_event_label = event_label
                weight_abs = re.sub(r"^[+-]", "", weight_token)
                weight_prefix = "plus" if weight_token.startswith("+") else "minus"
                discipline_weight_slug = f"{weight_prefix}-{weight_abs}-kg"
                current_discipline_key = f"judo-{table_gender}-{discipline_weight_slug}"
                current_discipline_name = f"Judo {table_gender.title()} {weight_token} kg"

                medal_cells = {1: cells[1], 2: cells[2], 3: cells[3]}
                silver_text = normalize_text(cells[2].get_text(" ", strip=True))
                previous_silver_not_awarded = silver_text == "not awarded"
                for rank, medal_cell in medal_cells.items():
                    parsed = parse_athlete_cell(medal_cell)
                    if parsed is None:
                        continue
                    athlete_name, country_name, country_code = parsed
                    rows.append(
                        {
                            "competition_id": COMPETITION_ID,
                            "competition_name": COMPETITION_NAME,
                            "year": int(year),
                            "event_date": f"{year}-12-31",
                            "discipline_key": current_discipline_key,
                            "discipline_name": current_discipline_name,
                            "event_name": current_event_label,
                            "gender": table_gender,
                            "rank": rank,
                            "medal": RANK_TO_MEDAL[rank],
                            "participant_type": "athlete",
                            "participant_name": athlete_name,
                            "country_name": country_name,
                            "country_code": country_code,
                            "source_url": source_url,
                        }
                    )

            # Continuation row for second bronze medalist: only one cell
            elif len(cells) == 1 and current_event_label and current_discipline_key and current_discipline_name:
                parsed = parse_athlete_cell(cells[0])
                if parsed is None:
                    continue
                athlete_name, country_name, country_code = parsed
                rows.append(
                    {
                        "competition_id": COMPETITION_ID,
                        "competition_name": COMPETITION_NAME,
                        "year": int(year),
                        "event_date": f"{year}-12-31",
                        "discipline_key": current_discipline_key,
                        "discipline_name": current_discipline_name,
                        "event_name": current_event_label,
                        "gender": table_gender,
                        "rank": 3,
                        "medal": "bronze",
                        "participant_type": "athlete",
                        "participant_name": athlete_name,
                        "country_name": country_name,
                        "country_code": country_code,
                        "source_url": source_url,
                    }
                )

            # Rare continuation row with two medals (e.g. additional gold + bronze when silver not awarded).
            elif len(cells) == 2 and current_event_label and current_discipline_key and current_discipline_name:
                rank_mapping = [1, 3] if previous_silver_not_awarded else [3, 3]
                for rank, medal_cell in zip(rank_mapping, cells):
                    parsed = parse_athlete_cell(medal_cell)
                    if parsed is None:
                        continue
                    athlete_name, country_name, country_code = parsed
                    rows.append(
                        {
                            "competition_id": COMPETITION_ID,
                            "competition_name": COMPETITION_NAME,
                            "year": int(year),
                            "event_date": f"{year}-12-31",
                            "discipline_key": current_discipline_key,
                            "discipline_name": current_discipline_name,
                            "event_name": current_event_label,
                            "gender": table_gender,
                            "rank": rank,
                            "medal": RANK_TO_MEDAL[rank],
                            "participant_type": "athlete",
                            "participant_name": athlete_name,
                            "country_name": country_name,
                            "country_code": country_code,
                            "source_url": source_url,
                        }
                    )

    return rows


def build_rows(max_year: int) -> tuple[list[dict[str, Any]], list[int]]:
    links = extract_edition_links(max_year=max_year)
    all_rows: list[dict[str, Any]] = []
    missing_years: list[int] = []

    for year, edition_url in links.items():
        try:
            parsed_rows = parse_event_tables_for_edition(year, edition_url)
            if not parsed_rows:
                raise RuntimeError("no event rows parsed")
            all_rows.extend(parsed_rows)
        except Exception:
            missing_years.append(year)

    all_rows = sorted(
        all_rows,
        key=lambda row: (
            int(row["year"]),
            str(row["discipline_key"]),
            int(row["rank"]),
            str(row["country_code"]),
            str(row["participant_name"]),
        ),
    )
    return all_rows, missing_years


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build World Judo Championships podium seed by weight category (post-2000)."
    )
    parser.add_argument("--max-year", type=int, default=2026)
    parser.add_argument("--out", type=Path, default=SEED_PATH)
    args = parser.parse_args()

    max_year = max(START_YEAR, int(args.max_year))
    rows, missing_years = build_rows(max_year=max_year)
    if not rows:
        raise RuntimeError("No rows extracted for World Judo Championships seed.")

    frame = pd.DataFrame(rows)
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype(int)
    frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce").astype(int)
    frame = frame.loc[frame["year"] > 2000].copy()
    frame = frame.dropna(subset=["discipline_key", "participant_name", "country_code"])
    frame = frame.drop_duplicates(
        subset=["year", "discipline_key", "gender", "rank", "participant_name", "country_code"],
        keep="first",
    )
    frame = frame.sort_values(["year", "gender", "discipline_key", "rank", "participant_name"]).reset_index(drop=True)

    profiles = (
        frame.groupby(["year", "discipline_key", "gender"])["rank"]
        .apply(lambda s: tuple(sorted(int(v) for v in s.tolist())))
        .to_dict()
    )
    allowed_profiles = {(1, 1, 3, 3), (1, 2, 3), (1, 2, 3, 3)}
    invalid_profiles = {k: v for k, v in profiles.items() if v not in allowed_profiles}
    if invalid_profiles:
        sample = dict(list(invalid_profiles.items())[:30])
        raise RuntimeError(f"Unexpected rank profile(s), expected podium by weight category: {sample}")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(args.out, index=False)

    event_count = frame[["year", "discipline_key", "gender"]].drop_duplicates().shape[0]
    years = sorted(frame["year"].unique().tolist())
    print(
        f"[seed] world_judo_championships rows={len(frame)} years={years[0]}-{years[-1]} "
        f"events={event_count} out={args.out}"
    )
    if missing_years:
        print(f"[seed] warning missing_years={sorted(set(missing_years))}")


if __name__ == "__main__":
    main()
