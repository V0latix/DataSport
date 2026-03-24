from __future__ import annotations

import argparse
import re
import unicodedata
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
from lxml import html


BASE_DIR = Path(__file__).resolve().parent
SEED_PATH = BASE_DIR / "world_rowing_championships_top3_seed.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder)"}

COMPETITION_ID = "world_rowing_championships"
COMPETITION_NAME = "World Rowing Championships"
INDEX_URL = "https://en.wikipedia.org/wiki/World_Rowing_Championships"
START_YEAR = 2001
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}

BOAT_LABELS = {
    "1x": ("single-sculls", "Single sculls"),
    "2x": ("double-sculls", "Double sculls"),
    "4x": ("quadruple-sculls", "Quadruple sculls"),
    "2-": ("coxless-pair", "Coxless pair"),
    "4-": ("coxless-four", "Coxless four"),
    "2+": ("coxed-pair", "Coxed pair"),
    "4+": ("coxed-four", "Coxed four"),
    "8+": ("eight", "Eight"),
}

CODE_CONFIG: dict[str, dict[str, str]] = {
    "AM1x": {
        "discipline_id": "rowing-adaptive-single-sculls",
        "discipline_name": "Adaptive single sculls",
        "gender": "men",
    },
    "ASM1x": {
        "discipline_id": "rowing-as-single-sculls",
        "discipline_name": "AS single sculls",
        "gender": "men",
    },
    "ASW1x": {
        "discipline_id": "rowing-as-single-sculls",
        "discipline_name": "AS single sculls",
        "gender": "women",
    },
    "AW1x": {
        "discipline_id": "rowing-adaptive-single-sculls",
        "discipline_name": "Adaptive single sculls",
        "gender": "women",
    },
    "IDMx4+": {
        "discipline_id": "rowing-id-mixed-coxed-four",
        "discipline_name": "ID mixed coxed four",
        "gender": "mixed",
    },
    "LTAM4+": {
        "discipline_id": "rowing-lta-coxed-four",
        "discipline_name": "LTA coxed four",
        "gender": "men",
    },
    "LTAM4x+": {
        "discipline_id": "rowing-lta-coxed-four",
        "discipline_name": "LTA coxed four",
        "gender": "men",
    },
    "LTAMix2x": {
        "discipline_id": "rowing-lta-mixed-double-sculls",
        "discipline_name": "LTA mixed double sculls",
        "gender": "mixed",
    },
    "LTAMix4+": {
        "discipline_id": "rowing-lta-mixed-coxed-four",
        "discipline_name": "LTA mixed coxed four",
        "gender": "mixed",
    },
    "LTAMix4x+": {
        "discipline_id": "rowing-lta-mixed-coxed-four",
        "discipline_name": "LTA mixed coxed four",
        "gender": "mixed",
    },
    "LTAMx4+": {
        "discipline_id": "rowing-lta-mixed-coxed-four",
        "discipline_name": "LTA mixed coxed four",
        "gender": "mixed",
    },
    "PR1M1x": {
        "discipline_id": "rowing-pr1-single-sculls",
        "discipline_name": "PR1 single sculls",
        "gender": "men",
    },
    "PR1W1x": {
        "discipline_id": "rowing-pr1-single-sculls",
        "discipline_name": "PR1 single sculls",
        "gender": "women",
    },
    "PR2M1x": {
        "discipline_id": "rowing-pr2-single-sculls",
        "discipline_name": "PR2 single sculls",
        "gender": "men",
    },
    "PR2W1x": {
        "discipline_id": "rowing-pr2-single-sculls",
        "discipline_name": "PR2 single sculls",
        "gender": "women",
    },
    "PR2Mix2x": {
        "discipline_id": "rowing-pr2-mixed-double-sculls",
        "discipline_name": "PR2 mixed double sculls",
        "gender": "mixed",
    },
    "PR3M2-": {
        "discipline_id": "rowing-pr3-coxless-pair",
        "discipline_name": "PR3 coxless pair",
        "gender": "men",
    },
    "PR3W2-": {
        "discipline_id": "rowing-pr3-coxless-pair",
        "discipline_name": "PR3 coxless pair",
        "gender": "women",
    },
    "PR3Mix2x": {
        "discipline_id": "rowing-pr3-mixed-double-sculls",
        "discipline_name": "PR3 mixed double sculls",
        "gender": "mixed",
    },
    "PR3Mix4+": {
        "discipline_id": "rowing-pr3-mixed-coxed-four",
        "discipline_name": "PR3 mixed coxed four",
        "gender": "mixed",
    },
    "TA2x": {
        "discipline_id": "rowing-ta-mixed-double-sculls",
        "discipline_name": "TA mixed double sculls",
        "gender": "mixed",
    },
    "TAMix1x": {
        "discipline_id": "rowing-ta-single-sculls",
        "discipline_name": "TA single sculls",
        "gender": "mixed",
    },
    "TAMix2x": {
        "discipline_id": "rowing-ta-mixed-double-sculls",
        "discipline_name": "TA mixed double sculls",
        "gender": "mixed",
    },
    "TAMx2x": {
        "discipline_id": "rowing-ta-mixed-double-sculls",
        "discipline_name": "TA mixed double sculls",
        "gender": "mixed",
    },
}

COUNTRY_OVERRIDES = {
    "czech republic": "CZE",
    "great britain": "GBR",
    "independent neutral athlete": "AIN",
    "independent neutral athletes": "AIN",
    "individual neutral athlete": "AIN",
    "individual neutral athletes": "AIN",
    "russia": "RUS",
    "serbia and montenegro": "SCG",
    "south korea": "KOR",
    "turkey": "TUR",
    "turkiye": "TUR",
    "yugoslavia": "YUG",
}
COUNTRY_NAME_OVERRIDES = {
    "AIN": "Individual Neutral Athletes",
    "SCG": "Serbia and Montenegro",
}
EVENT_CODE_RE = re.compile(
    r"^(?P<code>("
    r"PR[123](?:Mix|Mx|M|W)?(?:1x|2x|4x\+|4x|2-|4-|2\+|4\+|8\+)"
    r"|LTA(?:Mix|Mx|M|W)?(?:1x|2x|4x\+|4x|2-|4-|2\+|4\+|8\+)"
    r"|TA(?:Mix|Mx|M|W)?(?:1x|2x|4x\+|4x|2-|4-|2\+|4\+|8\+)"
    r"|AS(?:M|W)?1x"
    r"|A(?:M|W)?1x"
    r"|IDMx4\+"
    r"|L(?:Mix|Mx|M|W)?(?:1x|2x|4x\+|4x|2-|4-|2\+|4\+|8\+)"
    r"|(?:Mix|Mx|M|W)(?:1x|2x|4x\+|4x|2-|4-|2\+|4\+|8\+)"
    r"))"
)


def clean_text(value: str) -> str:
    text = str(value or "")
    text = text.replace("\xa0", " ")
    text = text.replace("−", "-")
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_text(value: str) -> str:
    text = unicodedata.normalize("NFKD", clean_text(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.lower()


def build_country_prefix_labels() -> list[str]:
    labels = set(COUNTRY_OVERRIDES.keys())
    try:
        import pycountry

        for country in pycountry.countries:
            for attr in ("name", "official_name", "common_name"):
                value = getattr(country, attr, None)
                if value:
                    labels.add(clean_text(str(value)))
    except Exception:
        pass
    return sorted((clean_text(label) for label in labels if clean_text(label)), key=len, reverse=True)


COUNTRY_PREFIX_LABELS = build_country_prefix_labels()


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

    norm = normalize_text(text)
    alias = COUNTRY_OVERRIDES.get(norm)
    if alias:
        return alias, canonical_country_name(alias, text)

    try:
        import pycountry

        country = pycountry.countries.lookup(text)
        code = str(getattr(country, "alpha_3"))
        return code, canonical_country_name(code, text)
    except Exception:
        return None


def infer_country_from_cell(cell: html.HtmlElement) -> tuple[str, str, list[str]] | None:
    labels: list[str] = []
    for link in cell.xpath(".//a"):
        for candidate in (link.get("title", ""), link.text_content()):
            resolved = try_resolve_country(candidate)
            if resolved is None:
                continue
            code, name = resolved
            label = clean_text(candidate)
            if label:
                labels.append(label)
            return code, name, labels

    raw_text = clean_text(cell.text_content())
    for size in range(5, 0, -1):
        parts = raw_text.split()
        if len(parts) < size:
            continue
        suffix = " ".join(parts[-size:])
        resolved = try_resolve_country(suffix)
        if resolved is not None:
            code, name = resolved
            return code, name, [suffix]
    for size in range(5, 0, -1):
        parts = raw_text.split()
        if len(parts) < size:
            continue
        prefix = " ".join(parts[:size])
        resolved = try_resolve_country(prefix)
        if resolved is not None:
            code, name = resolved
            return code, name, [prefix]
    raw_norm = normalize_text(raw_text)
    raw_compact = raw_norm.replace(" ", "")
    for label in COUNTRY_PREFIX_LABELS:
        label_norm = normalize_text(label)
        label_compact = label_norm.replace(" ", "")
        if raw_norm.startswith(label_norm) or raw_compact.startswith(label_compact):
            resolved = try_resolve_country(label)
            if resolved is not None:
                code, name = resolved
                return code, name, [label]
    return None


def parse_athlete_name(cell: html.HtmlElement, country_labels: list[str], country_name: str) -> str:
    athlete_links: list[str] = []
    for link in cell.xpath(".//a"):
        text = clean_text(link.text_content())
        if not text:
            continue
        if try_resolve_country(link.get("title", "") or text):
            continue
        athlete_links.append(text)
    if athlete_links:
        return athlete_links[0]

    raw_text = clean_text(cell.text_content())
    for label in country_labels + [country_name]:
        if label:
            raw_text = re.sub(rf"\b{re.escape(label)}\b", "", raw_text, flags=re.IGNORECASE)
    raw_text = re.sub(r"\((b|s|c|m|\d+)\)", "", raw_text, flags=re.IGNORECASE)
    raw_text = re.sub(r"\s+", " ", raw_text).strip(" ,;-")
    return raw_text


def parse_event_code(raw_event: str) -> tuple[str | None, str]:
    raw = clean_text(raw_event)
    if not raw:
        return None, ""
    raw = raw.replace("details", "")
    raw = re.sub(r"\[\d+\]", "", raw)
    raw = clean_text(raw)
    match = EVENT_CODE_RE.match(raw)
    if match is None:
        return None, ""
    code = match.group("code")
    description = clean_text(raw[len(code) :])
    if code.lower() in {"men", "women", "pararowing", "para-rowing"}:
        return None, description
    return code, description


def infer_code_config(code: str, description: str) -> dict[str, str]:
    if code in CODE_CONFIG:
        participant_type = "athlete" if code.endswith("1x") else "team"
        return {**CODE_CONFIG[code], "participant_type": participant_type}

    working = code
    gender = ""
    lightweight = False

    if working.startswith("L") and not working.startswith(("LTA", "LTAM")):
        lightweight = True
        working = working[1:]

    if working.startswith("Mix"):
        gender = "mixed"
        working = working[3:]
    elif working.startswith("Mx"):
        gender = "mixed"
        working = working[2:]
    elif working.startswith("M"):
        gender = "men"
        working = working[1:]
    elif working.startswith("W"):
        gender = "women"
        working = working[1:]

    boat_slug, boat_name = BOAT_LABELS.get(working, ("", ""))
    if not boat_slug or not gender:
        raise RuntimeError(f"Unsupported rowing event code `{code}` (description=`{description}`).")

    parts = ["rowing"]
    if gender == "mixed":
        parts.append("mixed")
    if lightweight:
        parts.append("lightweight")
    parts.append(boat_slug)

    discipline_id = "-".join(parts)
    discipline_name_parts = []
    if gender == "mixed":
        discipline_name_parts.append("Mixed")
    if lightweight:
        discipline_name_parts.append("Lightweight")
    discipline_name_parts.append(boat_name)

    participant_type = "athlete" if working == "1x" else "team"
    return {
        "discipline_id": discipline_id,
        "discipline_name": " ".join(discipline_name_parts),
        "gender": gender,
        "participant_type": participant_type,
    }


def extract_event_date(page_html: str, year: int) -> str:
    tables = pd.read_html(StringIO(page_html))
    for table in tables[:3]:
        if len(table.columns) < 2:
            continue
        left_col = table.columns[0]
        right_col = table.columns[1]
        for _, row in table.iterrows():
            left = clean_text(str(row.get(left_col, "")))
            if left.lower() != "dates":
                continue
            value = clean_text(str(row.get(right_col, "")))
            return parse_end_date(value, year)
    return f"{year}-12-31"


def parse_end_date(raw_dates: str, year: int) -> str:
    text = clean_text(raw_dates)
    text = re.sub(r"\(.*?\)", "", text).strip()
    if not text:
        return f"{year}-12-31"

    parts = re.split(r"\s*[–—-]\s*", text)
    target = clean_text(parts[-1])
    first = clean_text(parts[0])

    months = (
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    )
    if not any(month.lower() in target.lower() for month in months):
        for month in months:
            if month.lower() in first.lower():
                target = f"{target} {month}"
                break
    if str(year) not in target:
        target = f"{target} {year}"

    parsed = pd.to_datetime(target, errors="coerce", dayfirst=True)
    if pd.isna(parsed):
        return f"{year}-12-31"
    return parsed.strftime("%Y-%m-%d")


def looks_like_time(value: str) -> bool:
    text = clean_text(value)
    return bool(re.match(r"^\d+:\d{2}[:\.,]\d{2}", text))


def medal_cells(cells: list[html.HtmlElement]) -> list[tuple[html.HtmlElement, int]]:
    ranked_cells: list[tuple[html.HtmlElement, int]] = []
    for cell in cells[1:]:
        cell_text = clean_text(cell.text_content())
        if not cell_text or looks_like_time(cell_text):
            continue
        ranked_cells.append((cell, len(ranked_cells) + 1))
        if len(ranked_cells) == 3:
            break
    return ranked_cells


def should_skip_table(table: html.HtmlElement) -> bool:
    section_h2 = normalize_text(table.xpath("string(preceding::h2[1])"))
    if "under 23" in section_h2 or "under 19" in section_h2 or "junior" in section_h2:
        return True
    return False


def extract_rows_for_year(year: int) -> list[dict[str, str | int]]:
    url = f"https://en.wikipedia.org/wiki/{year}_World_Rowing_Championships"
    response = requests.get(url, headers=HEADERS, timeout=45)
    if response.status_code != 200:
        return []

    page_html = response.text
    root = html.fromstring(page_html)
    event_date = extract_event_date(page_html, year)
    rows: list[dict[str, str | int]] = []

    for table in root.xpath('//table[contains(@class,"wikitable")]'):
        if should_skip_table(table):
            continue

        header_cells = [clean_text(cell.text_content()) for cell in table.xpath(".//tr[1]/*")[:6]]
        header_joined = " ".join(header_cells)
        if "Event" not in header_joined or "Gold" not in header_joined or "Silver" not in header_joined:
            continue

        for tr in table.xpath(".//tr[position()>1]"):
            cells = tr.xpath("./th|./td")
            if len(cells) < 4:
                continue

            raw_event = clean_text(cells[0].text_content())
            code, description = parse_event_code(raw_event)
            if code is None:
                continue

            config = infer_code_config(code, description)
            for medal_cell, rank in medal_cells(cells):
                medal_text = clean_text(medal_cell.text_content())
                if not medal_text:
                    continue
                medal_lower = medal_text.lower()
                if "not awarded" in medal_lower:
                    continue
                if "awarded" in medal_lower:
                    continue
                if medal_lower.startswith("only ") and "compet" in medal_lower:
                    continue
                if re.sub(r"[^\w]+", "", medal_text) == "":
                    continue

                country = infer_country_from_cell(medal_cell)
                if country is None:
                    raise RuntimeError(f"Unable to resolve country for `{code}` `{medal_text}` on {url}.")
                country_code, country_name, country_labels = country

                participant_name = country_name
                if config["participant_type"] == "athlete":
                    participant_name = parse_athlete_name(medal_cell, country_labels, country_name)
                    if not participant_name:
                        raise RuntimeError(f"Unable to resolve athlete name for `{code}` `{medal_text}` on {url}.")

                rows.append(
                    {
                        "competition_id": COMPETITION_ID,
                        "competition_name": COMPETITION_NAME,
                        "year": year,
                        "event_date": event_date,
                        "discipline_key": code,
                        "discipline_id": config["discipline_id"],
                        "discipline_name": config["discipline_name"],
                        "event_name": raw_event,
                        "gender": config["gender"],
                        "rank": rank,
                        "medal": RANK_TO_MEDAL[rank],
                        "participant_type": config["participant_type"],
                        "participant_name": participant_name,
                        "country_name": country_name,
                        "country_code": country_code,
                        "source_url": url,
                    }
                )

    return rows


def build_seed(max_year: int) -> pd.DataFrame:
    all_rows: list[dict[str, str | int]] = []
    missing_years: list[int] = []
    for year in range(START_YEAR, max_year + 1):
        rows = extract_rows_for_year(year)
        if not rows:
            missing_years.append(year)
            continue
        all_rows.extend(rows)

    if not all_rows:
        raise RuntimeError("No World Rowing Championships rows extracted.")

    frame = pd.DataFrame(all_rows)
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype(int)
    frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce").astype(int)
    frame["event_date"] = pd.to_datetime(frame["event_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    frame = frame.loc[(frame["year"] > 2000) & (frame["year"] <= max_year)].copy()
    frame = frame.loc[frame["rank"].isin([1, 2, 3])].copy()
    frame = frame.drop_duplicates(
        subset=[
            "year",
            "discipline_key",
            "rank",
            "participant_name",
            "country_code",
        ]
    ).sort_values(["year", "discipline_key", "rank", "country_code", "participant_name"]).reset_index(drop=True)

    if frame.empty:
        raise RuntimeError(f"No post-2000 World Rowing Championships rows extracted for <= {max_year}.")

    profiles = (
        frame.groupby(["year", "discipline_key"])["rank"]
        .apply(lambda s: tuple(sorted(set(int(v) for v in s.tolist()))))
        .to_dict()
    )
    invalid = {
        key: profile
        for key, profile in profiles.items()
        if not profile or profile[0] != 1 or any(rank not in {1, 2, 3} for rank in profile)
    }
    if invalid:
        sample = dict(list(invalid.items())[:20])
        raise RuntimeError(f"Unexpected rank profiles in World Rowing seed: {sample}")

    years = sorted(frame["year"].unique().tolist())
    expected = set(range(START_YEAR, max_year + 1))
    observed = set(years)
    missing_post_2000 = sorted(expected - observed)
    print(f"Extracted {len(frame)} rows across {len(years)} years.")
    print(f"Years covered: {years[0]}-{years[-1]}")
    print(f"Missing years within scope: {missing_post_2000}")
    return frame


def main() -> None:
    parser = argparse.ArgumentParser(description="Build World Rowing Championships podium seed from Wikipedia.")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--out", type=Path, default=SEED_PATH)
    args = parser.parse_args()

    frame = build_seed(args.year)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(args.out, index=False)
    print(f"Wrote {len(frame)} rows to {args.out}")


if __name__ == "__main__":
    main()
