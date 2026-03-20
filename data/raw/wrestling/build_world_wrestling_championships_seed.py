from __future__ import annotations

import argparse
import re
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup


SEED_PATH = Path(__file__).resolve().parent / "world_wrestling_championships_top3_seed.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder)"}

SOURCES = [
    {
        "url": "https://en.wikipedia.org/wiki/List_of_World_Championships_medalists_in_wrestling_(freestyle)",
        "competition_id": "world_wrestling_championships_freestyle",
        "competition_name": "World Wrestling Championships (Freestyle)",
        "discipline_key": "wrestling-freestyle",
        "discipline_name": "Wrestling Freestyle",
        "gender": "men",
    },
    {
        "url": "https://en.wikipedia.org/wiki/List_of_World_Championships_medalists_in_wrestling_(Greco-Roman)",
        "competition_id": "world_wrestling_championships_greco_roman",
        "competition_name": "World Wrestling Championships (Greco-Roman)",
        "discipline_key": "wrestling-greco-roman",
        "discipline_name": "Wrestling Greco-Roman",
        "gender": "men",
    },
    {
        "url": "https://en.wikipedia.org/wiki/List_of_World_Championships_medalists_in_wrestling_(women)",
        "competition_id": "world_wrestling_championships_freestyle",
        "competition_name": "World Wrestling Championships (Freestyle)",
        "discipline_key": "wrestling-freestyle",
        "discipline_name": "Wrestling Freestyle",
        "gender": "women",
    },
]

RANK_COLUMNS = [(1, "Gold", "gold"), (2, "Silver", "silver"), (3, "Bronze", "bronze")]
COUNTRY_NAME_OVERRIDES = {
    "AIN": "Individual Neutral Athletes",
    "EUN": "Unified Team",
    "IRI": "Iran",
    "ROC": "Russian Olympic Committee",
    "SCG": "Serbia and Montenegro",
}


def clean_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    if text.lower() in {"nan", "none"}:
        return ""
    return text


def country_name_from_code(code: str) -> str:
    code = str(code).upper().strip()
    if not code:
        return ""
    if code in COUNTRY_NAME_OVERRIDES:
        return COUNTRY_NAME_OVERRIDES[code]
    try:
        import pycountry

        country = pycountry.countries.get(alpha_3=code)
        if country is not None:
            return str(getattr(country, "name", code))
    except Exception:
        pass
    return code


def parse_athlete_and_country(cell_value: Any) -> tuple[str, str] | None:
    text = clean_text(cell_value)
    if not text:
        return None
    match = re.match(r"^(?P<name>.+?)\s*\((?P<code>[A-Za-z0-9]{2,5})\)$", text)
    if match:
        return clean_text(match.group("name")), str(match.group("code")).upper()
    return None


def extract_rows(max_year: int, start_year: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for source in SOURCES:
        response = requests.get(source["url"], headers=HEADERS, timeout=60)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        current_heading: str | None = None
        for node in soup.select("div.mw-heading2, table.wikitable"):
            if node.name == "div" and "mw-heading2" in (node.get("class") or []):
                current_heading = clean_text(node.get_text(" ", strip=True)).replace("[ edit ]", "").strip()
                continue
            if current_heading is None or current_heading.lower() == "medal table":
                continue

            table = pd.read_html(StringIO(str(node)))[0]
            if not {"Games", "Gold", "Silver", "Bronze"}.issubset(set(table.columns)):
                continue

            for record in table.to_dict(orient="records"):
                games = clean_text(record.get("Games"))
                year_match = re.search(r"(19|20)\d{2}", games)
                if year_match is None:
                    continue
                year = int(year_match.group(0))
                if year < start_year or year > max_year:
                    continue

                for rank, column, medal in RANK_COLUMNS:
                    parsed = parse_athlete_and_country(record.get(column))
                    if parsed is None:
                        continue
                    participant_name, country_code = parsed
                    if not participant_name or not country_code:
                        continue

                    rows.append(
                        {
                            "competition_id": source["competition_id"],
                            "competition_name": source["competition_name"],
                            "year": year,
                            "event_date": f"{year}-12-31",
                            "discipline_key": source["discipline_key"],
                            "discipline_name": source["discipline_name"],
                            "weight_class": current_heading,
                            "event_name": current_heading,
                            "gender": source["gender"],
                            "rank": rank,
                            "medal": medal,
                            "participant_type": "athlete",
                            "participant_name": participant_name,
                            "country_name": country_name_from_code(country_code),
                            "country_code": country_code,
                            "source_url": source["url"],
                        }
                    )
    return rows


def build_seed(max_year: int, start_year: int, output: Path) -> pd.DataFrame:
    rows = extract_rows(max_year=max_year, start_year=start_year)
    frame = pd.DataFrame(rows)
    if frame.empty:
        raise RuntimeError("World wrestling seed extraction produced no rows.")

    frame = frame.drop_duplicates(
        subset=[
            "competition_id",
            "year",
            "gender",
            "weight_class",
            "rank",
            "participant_name",
            "country_code",
        ]
    )
    frame = frame.sort_values(
        [
            "year",
            "competition_id",
            "gender",
            "weight_class",
            "rank",
            "participant_name",
            "country_code",
        ]
    ).reset_index(drop=True)

    profiles = (
        frame.groupby(["competition_id", "year", "gender", "weight_class"])["rank"]
        .apply(lambda s: tuple(sorted(int(v) for v in s.tolist())))
        .to_dict()
    )
    allowed_profiles = {(1, 2, 3), (1, 2, 3, 3), (1, 1, 3, 3)}
    bad = {key: value for key, value in profiles.items() if value not in allowed_profiles}
    if bad:
        sample = dict(list(bad.items())[:20])
        raise RuntimeError(f"Unexpected rank profiles in wrestling seed: {sample}")

    output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output, index=False)
    return frame


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build world wrestling championships seed from Wikipedia.")
    parser.add_argument("--max-year", type=int, default=2026)
    parser.add_argument("--start-year", type=int, default=2001)
    parser.add_argument("--output", type=Path, default=SEED_PATH)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    seed = build_seed(max_year=int(args.max_year), start_year=int(args.start_year), output=Path(args.output))
    print(f"[wrestling-seed] wrote {args.output} rows={len(seed)} years={seed.year.min()}-{seed.year.max()}")
    by_competition = seed.groupby(["competition_id", "gender"]).size()
    print(f"[wrestling-seed] rows by competition/gender:\n{by_competition}")


if __name__ == "__main__":
    main()
