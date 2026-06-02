from __future__ import annotations

import argparse
import re
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
from lxml import html


BASE_DIR = Path(__file__).resolve().parent
SEED_PATH = BASE_DIR / "icf_canoe_world_championships_top3_seed.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder)"}
WIKI_BASE = "https://en.wikipedia.org"
START_YEAR = 2001
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}

COUNTRY_OVERRIDES = {
    "ain": "AIN",
    "authorised neutral athlete": "AIN",
    "authorised neutral athletes": "AIN",
    "authorized neutral athlete": "AIN",
    "authorized neutral athletes": "AIN",
    "australasia": "ANZ",
    "belarus": "BLR",
    "china": "CHN",
    "chinese taipei": "TPE",
    "czech republic": "CZE",
    "east germany": "GDR",
    "germany": "DEU",
    "great britain": "GBR",
    "hong kong": "HKG",
    "individual neutral athlete": "AIN",
    "individual neutral athletes": "AIN",
    "iran": "IRN",
    "moldova": "MDA",
    "north korea": "PRK",
    "people's republic of china": "CHN",
    "russia": "RUS",
    "russian federation": "RUS",
    "serbia and montenegro": "SCG",
    "slovakia": "SVK",
    "slovenia": "SVN",
    "south korea": "KOR",
    "soviet union": "URS",
    "turkey": "TUR",
    "turkiye": "TUR",
    "ukraine": "UKR",
    "united kingdom": "GBR",
    "united states": "USA",
    "united states of america": "USA",
    "west germany": "FRG",
    "yugoslavia": "YUG",
}
IOC_TO_ISO3 = {
    "AUS": "AUS",
    "AUT": "AUT",
    "AZE": "AZE",
    "BLR": "BLR",
    "BRA": "BRA",
    "BUL": "BGR",
    "CAN": "CAN",
    "CHN": "CHN",
    "CRO": "HRV",
    "CUB": "CUB",
    "CZE": "CZE",
    "DEN": "DNK",
    "ESP": "ESP",
    "EST": "EST",
    "FIN": "FIN",
    "FRA": "FRA",
    "GBR": "GBR",
    "GEO": "GEO",
    "GER": "DEU",
    "GRE": "GRC",
    "HUN": "HUN",
    "ITA": "ITA",
    "JPN": "JPN",
    "KAZ": "KAZ",
    "LAT": "LVA",
    "LTU": "LTU",
    "MDA": "MDA",
    "MEX": "MEX",
    "NED": "NLD",
    "NZL": "NZL",
    "POL": "POL",
    "POR": "PRT",
    "ROU": "ROU",
    "RUS": "RUS",
    "SCG": "SCG",
    "SLO": "SVN",
    "SVK": "SVK",
    "SWE": "SWE",
    "SUI": "CHE",
    "TPE": "TPE",
    "TUR": "TUR",
    "UKR": "UKR",
    "URS": "URS",
    "USA": "USA",
    "UZB": "UZB",
    "YUG": "YUG",
}
COUNTRY_NAME_OVERRIDES = {
    "AIN": "Individual Neutral Athletes",
    "ANZ": "Australasia",
    "FRG": "West Germany",
    "GDR": "East Germany",
    "SCG": "Serbia and Montenegro",
    "TPE": "Chinese Taipei",
    "URS": "Soviet Union",
    "YUG": "Yugoslavia",
}


@dataclass(frozen=True)
class CompetitionConfig:
    competition_id: str
    competition_name: str
    discipline_id: str
    discipline_name: str
    index_url: str
    page_re: re.Pattern[str]


CONFIGS = [
    CompetitionConfig(
        competition_id="icf_canoe_sprint_world_championships",
        competition_name="ICF Canoe Sprint World Championships",
        discipline_id="canoe-sprint",
        discipline_name="Canoe Sprint",
        index_url=f"{WIKI_BASE}/wiki/ICF_Canoe_Sprint_World_Championships",
        page_re=re.compile(r"/wiki/(?P<year>20\d{2})_ICF_Canoe_Sprint_World_Championships$"),
    ),
    CompetitionConfig(
        competition_id="icf_canoe_slalom_world_championships",
        competition_name="ICF Canoe Slalom World Championships",
        discipline_id="canoe-slalom",
        discipline_name="Canoe Slalom",
        index_url=f"{WIKI_BASE}/wiki/ICF_Canoe_Slalom_World_Championships",
        page_re=re.compile(r"/wiki/(?P<year>20\d{2})_ICF_Canoe_Slalom_World_Championships$"),
    ),
]


def clean_text(value: str) -> str:
    text = str(value or "")
    text = text.replace("\xa0", " ")
    text = text.replace("−", "-").replace("–", "-")
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_text(value: str) -> str:
    text = unicodedata.normalize("NFKD", clean_text(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.lower()


def slugify(value: str) -> str:
    text = normalize_text(value)
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return re.sub(r"-+", "-", text).strip("-")


def build_country_labels() -> list[tuple[str, str]]:
    labels: dict[str, str] = {}
    for label, code in COUNTRY_OVERRIDES.items():
        labels[normalize_text(label)] = code
    try:
        import pycountry

        for country in pycountry.countries:
            code = str(getattr(country, "alpha_3"))
            for attr in ("name", "official_name", "common_name"):
                value = getattr(country, attr, None)
                if value:
                    labels.setdefault(normalize_text(str(value)), code)
    except Exception:
        pass
    return sorted(labels.items(), key=lambda item: len(item[0]), reverse=True)


COUNTRY_LABELS = build_country_labels()


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


def resolve_country(value: str) -> tuple[str, str] | None:
    text = clean_text(value)
    if not text:
        return None

    code_match = re.fullmatch(r"[A-Z]{3}", text)
    if code_match:
        code = IOC_TO_ISO3.get(text, text)
        return code, canonical_country_name(code, text)

    alias = COUNTRY_OVERRIDES.get(normalize_text(text))
    if alias:
        code = IOC_TO_ISO3.get(alias, alias)
        return code, canonical_country_name(code, text)

    try:
        import pycountry

        country = pycountry.countries.lookup(text)
        code = str(getattr(country, "alpha_3"))
        return code, canonical_country_name(code, text)
    except Exception:
        return None


def request_doc(url: str) -> html.HtmlElement:
    response = requests.get(url, headers=HEADERS, timeout=45)
    response.raise_for_status()
    return html.fromstring(response.content)


def discover_pages(config: CompetitionConfig, max_year: int) -> list[tuple[int, str]]:
    doc = request_doc(config.index_url)
    pages: dict[int, str] = {}
    for link in doc.xpath("//a[@href]"):
        href = str(link.get("href") or "")
        title = str(link.get("title") or "")
        if "Paracanoe" in title or "redlink=1" in href:
            continue
        match = config.page_re.fullmatch(href)
        if not match:
            continue
        year = int(match.group("year"))
        if START_YEAR <= year <= max_year:
            pages.setdefault(year, urljoin(WIKI_BASE, href))
    return sorted(pages.items())


def heading_context(doc: html.HtmlElement) -> list[tuple[html.HtmlElement, list[str]]]:
    context: list[tuple[int, str]] = []
    output: list[tuple[html.HtmlElement, list[str]]] = []
    nodes = doc.xpath(
        "//h2|//h3|//h4|//h5|//table[contains(concat(' ', normalize-space(@class), ' '), ' wikitable ')]"
    )
    for node in nodes:
        if node.tag in {"h2", "h3", "h4", "h5"}:
            level = int(node.tag[1])
            text = clean_text(node.text_content())
            context = [item for item in context if item[0] < level]
            context.append((level, text))
        elif node.tag == "table":
            output.append((node, [item[1] for item in context]))
    return output


def is_medal_table(table: html.HtmlElement) -> bool:
    header = clean_text(" ".join(table.xpath(".//tr[1]//th//text() | .//tr[1]//td//text()"))).lower()
    return "event" in header and "gold" in header and "silver" in header and "bronze" in header


def infer_gender(headings: list[str], row_event: str, page_title: str) -> str:
    haystack = " ".join([*headings, row_event, page_title]).lower()
    if "mixed" in haystack or row_event.upper().startswith("X"):
        return "mixed"
    if "women" in haystack or "women's" in haystack or re.search(r"\bW(?:1|2|4|K|C)", row_event):
        return "women"
    return "men"


def clean_event_name(value: str) -> str:
    text = clean_text(value)
    text = re.sub(r"\bdetails\b", "", text, flags=re.I)
    text = re.sub(r"details$", "", text, flags=re.I)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def event_key(competition_id: str, gender: str, event_name: str) -> str:
    return f"{competition_id.replace('icf_canoe_', '')}_{gender}_{slugify(event_name)}"


def medal_columns(table: html.HtmlElement, cells: list[html.HtmlElement]) -> list[tuple[int, html.HtmlElement, str]]:
    header_cells = [clean_text(cell.text_content()).lower() for cell in table.xpath(".//tr[1]/*")]
    if len(header_cells) == len(cells):
        output: list[tuple[int, html.HtmlElement, str]] = []
        for rank, label in ((1, "gold"), (2, "silver"), (3, "bronze")):
            try:
                index = next(i for i, header in enumerate(header_cells) if label in header)
            except StopIteration:
                continue
            score = clean_text(cells[index + 1].text_content()) if index + 1 < len(cells) else ""
            output.append((rank, cells[index], score))
        if len(output) == 3:
            return output

    if len(cells) >= 7:
        return [
            (1, cells[1], clean_text(cells[2].text_content())),
            (2, cells[3], clean_text(cells[4].text_content())),
            (3, cells[5], clean_text(cells[6].text_content())),
        ]
    if len(cells) >= 4:
        return [
            (1, cells[1], ""),
            (2, cells[2], ""),
            (3, cells[3], ""),
        ]
    return []


def country_from_links(cell: html.HtmlElement) -> tuple[str, str] | None:
    for link in cell.xpath(".//a"):
        if (link.get("href") or "").startswith("#cite_note"):
            continue
        for candidate in (link.text_content(), link.get("title") or ""):
            resolved = resolve_country(candidate)
            if resolved is not None:
                return resolved
    return None


def country_from_text(cell_text: str) -> tuple[str, str] | None:
    paren_codes = re.findall(r"\(([A-Z]{3})\)", cell_text)
    if paren_codes:
        return resolve_country(paren_codes[-1])

    text = clean_text(re.sub(r"\([A-Z]{3}\)", "", cell_text))
    norm_text = normalize_text(text)
    for label, alias_code in COUNTRY_LABELS:
        if norm_text.startswith(label):
            code = IOC_TO_ISO3.get(alias_code, alias_code)
            return code, canonical_country_name(code, label)
        if norm_text.endswith(label):
            code = IOC_TO_ISO3.get(alias_code, alias_code)
            return code, canonical_country_name(code, label)

    tokens = text.split()
    for size in range(min(5, len(tokens)), 0, -1):
        for candidate in (" ".join(tokens[:size]), " ".join(tokens[-size:])):
            resolved = resolve_country(candidate)
            if resolved is not None:
                return resolved
    return None


def extract_medalists(cell: html.HtmlElement) -> list[tuple[str, str, str]]:
    raw_text = clean_text(cell.text_content())
    coded_segments = re.findall(r"(.+?)\s*\(([A-Z]{3})\)", raw_text)
    if len(coded_segments) > 1:
        medalists = []
        for name, raw_code in coded_segments:
            resolved = resolve_country(raw_code)
            if resolved is None:
                continue
            country_code, country_name = resolved
            medalists.append((clean_text(name), country_name, country_code))
        if medalists:
            return medalists

    country = country_from_links(cell) or country_from_text(raw_text)
    if country is None:
        raise RuntimeError(f"Could not resolve country from medal cell: {raw_text!r}")
    country_code, country_name = country

    athlete_links: list[str] = []
    for link in cell.xpath(".//a"):
        text = clean_text(link.text_content())
        if not text or text.lower() == "details" or text.startswith("["):
            continue
        if resolve_country(text) is not None:
            continue
        athlete_links.append(text)

    if athlete_links:
        participant_name = " / ".join(dict.fromkeys(athlete_links))
    else:
        stripped = re.sub(r"\([A-Z]{3}\)", "", raw_text)
        for country_label in {country_name, country_code}:
            stripped = re.sub(rf"\b{re.escape(country_label)}\b", "", stripped)
        participant_name = clean_text(stripped) or country_name
    return [(participant_name, country_name, country_code)]


def parse_page(config: CompetitionConfig, year: int, url: str) -> list[dict[str, str | int]]:
    doc = request_doc(url)
    page_title = clean_text(" ".join(doc.xpath("//h1//text()")))
    event_date = f"{year}-12-31"
    rows: list[dict[str, str | int]] = []

    for table, headings in heading_context(doc):
        if not is_medal_table(table):
            continue
        headings_norm = " ".join(headings).lower()
        if config.discipline_id == "canoe-sprint" and "paracanoe" in headings_norm:
            continue
        if "medal table" in headings_norm:
            continue

        for tr in table.xpath(".//tr[position() > 1]"):
            cells = tr.xpath("./th|./td")
            if len(cells) < 4:
                continue
            event_name = clean_event_name(cells[0].text_content())
            if not event_name or event_name.lower() in {"event"}:
                continue
            gender = infer_gender(headings, event_name, page_title)
            key = event_key(config.competition_id, gender, event_name)
            for rank, medal_cell, score in medal_columns(table, cells):
                medal_text = clean_text(medal_cell.text_content()).lower()
                if medal_text in {"", "-", "na", "n/a"}:
                    continue
                for participant_name, country_name, country_code in extract_medalists(medal_cell):
                    participant_type = "team" if "/" in participant_name or " team" in event_name.lower() else "athlete"
                    rows.append(
                        {
                            "competition_id": config.competition_id,
                            "competition_name": config.competition_name,
                            "year": year,
                            "event_date": event_date,
                            "discipline_id": config.discipline_id,
                            "discipline_name": config.discipline_name,
                            "event_key": key,
                            "event_name": event_name,
                            "gender": gender,
                            "rank": rank,
                            "medal": RANK_TO_MEDAL[rank],
                            "participant_type": participant_type,
                            "participant_name": participant_name,
                            "country_name": country_name,
                            "country_code": country_code,
                            "score_raw": score,
                            "source_url": url,
                        }
                    )
    return rows


def build_seed(max_year: int, sleep_seconds: float) -> pd.DataFrame:
    rows: list[dict[str, str | int]] = []
    for config in CONFIGS:
        pages = discover_pages(config, max_year)
        for year, url in pages:
            if year <= 2000:
                continue
            print(f"[seed] {config.competition_id} {year} {url}")
            rows.extend(parse_page(config, year, url))
            time.sleep(sleep_seconds)

    frame = pd.DataFrame(rows)
    if frame.empty:
        raise RuntimeError("No ICF Canoe World Championships rows parsed.")
    frame["year"] = pd.to_numeric(frame["year"], errors="raise").astype(int)
    frame = frame.loc[frame["year"] > 2000].copy()
    frame = frame.drop_duplicates(
        subset=["competition_id", "year", "event_key", "rank", "participant_name", "country_code"]
    )
    return frame.sort_values(["competition_id", "year", "event_key", "rank", "country_code"]).reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build ICF canoe sprint/slalom world championships seed.")
    parser.add_argument("--max-year", type=int, default=2026)
    parser.add_argument("--sleep", type=float, default=0.2)
    parser.add_argument("--out", type=Path, default=SEED_PATH)
    args = parser.parse_args()

    frame = build_seed(max_year=args.max_year, sleep_seconds=args.sleep)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(args.out, index=False)
    print(f"[seed] wrote {len(frame)} rows to {args.out}")


if __name__ == "__main__":
    main()
