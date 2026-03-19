from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag


BASE_DIR = Path(__file__).resolve().parent
PAGES_DIR = BASE_DIR / "wikipedia_pages"
SEED_PATH = BASE_DIR / "uci_road_cycling_major_competitions_top3_seed.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport seed builder)"}
RANK_TO_MEDAL = {1: "gold", 2: "silver", 3: "bronze"}

# French/English country aliases observed in cycling pages.
COUNTRY_CODE_OVERRIDES = {
    "(wikidata:q145)": "GBR",
    "albania": "ALB",
    "allemagne": "GER",
    "empire allemand": "GER",
    "allemagne de l est": "GDR",
    "allemagne de l'ouest": "FRG",
    "allemagne de louest": "FRG",
    "allemagne de lest": "GDR",
    "argentine": "ARG",
    "australie": "AUS",
    "austria": "AUT",
    "autriche": "AUT",
    "belgique": "BEL",
    "belgium": "BEL",
    "belarus": "BLR",
    "bielorussie": "BLR",
    "canada": "CAN",
    "colombie": "COL",
    "colombia": "COL",
    "croatie": "HRV",
    "croatia": "HRV",
    "czechoslovakia": "TCH",
    "tchecoslovaquie": "TCH",
    "republique tcheque": "CZE",
    "tchequie": "CZE",
    "czech republic": "CZE",
    "czechia": "CZE",
    "danemark": "DNK",
    "denmark": "DNK",
    "espagne": "ESP",
    "spain": "ESP",
    "estonie": "EST",
    "estonia": "EST",
    "etats unis": "USA",
    "etats-unis": "USA",
    "united states": "USA",
    "finlande": "FIN",
    "finland": "FIN",
    "france": "FRA",
    "federal republic of germany": "FRG",
    "republique federale d allemagne": "FRG",
    "germany": "GER",
    "great britain": "GBR",
    "royaume uni": "GBR",
    "royaume-uni": "GBR",
    "hollande": "NLD",
    "hungary": "HUN",
    "hongrie": "HUN",
    "ireland": "IRL",
    "irlande": "IRL",
    "italie": "ITA",
    "italy": "ITA",
    "kazakhstan": "KAZ",
    "lettonie": "LVA",
    "latvia": "LVA",
    "lituanie": "LTU",
    "lithuania": "LTU",
    "luxembourg": "LUX",
    "mexique": "MEX",
    "moldavie": "MDA",
    "new zealand": "NZL",
    "nouvelle-zelande": "NZL",
    "nouvelle zelande": "NZL",
    "norvege": "NOR",
    "norway": "NOR",
    "pays bas": "NLD",
    "pays-bas": "NLD",
    "netherlands": "NLD",
    "pologne": "POL",
    "poland": "POL",
    "portugal": "PRT",
    "roumanie": "ROU",
    "romania": "ROU",
    "russie": "RUS",
    "russia": "RUS",
    "republique de weimar": "GER",
    "seconde republique espagnole": "ESP",
    "slovaquie": "SVK",
    "slovakia": "SVK",
    "slovenie": "SVN",
    "slovenia": "SVN",
    "suede": "SWE",
    "sweden": "SWE",
    "suisse": "CHE",
    "switzerland": "CHE",
    "soviet union": "URS",
    "union sovietique": "URS",
    "equateur": "ECU",
    "ukraine": "UKR",
    "west germany": "FRG",
    "east germany": "GDR",
    "yougoslavie": "YUG",
    "yugoslavia": "YUG",
    "weimar republic": "GER",
}

CODE_TO_COUNTRY = {
    "ALB": "Albania",
    "ARG": "Argentina",
    "AUS": "Australia",
    "AUT": "Austria",
    "BEL": "Belgium",
    "BLR": "Belarus",
    "CAN": "Canada",
    "CHE": "Switzerland",
    "COL": "Colombia",
    "CZE": "Czech Republic",
    "DNK": "Denmark",
    "ESP": "Spain",
    "EST": "Estonia",
    "ECU": "Ecuador",
    "FIN": "Finland",
    "FRA": "France",
    "FRG": "West Germany",
    "GBR": "Great Britain",
    "GDR": "East Germany",
    "GER": "Germany",
    "HRV": "Croatia",
    "HUN": "Hungary",
    "IRL": "Ireland",
    "ITA": "Italy",
    "KAZ": "Kazakhstan",
    "LTU": "Lithuania",
    "LUX": "Luxembourg",
    "MDA": "Moldova",
    "MEX": "Mexico",
    "LVA": "Latvia",
    "NLD": "Netherlands",
    "NOR": "Norway",
    "NZL": "New Zealand",
    "POL": "Poland",
    "PRT": "Portugal",
    "ROU": "Romania",
    "RUS": "Russia",
    "SVK": "Slovakia",
    "SVN": "Slovenia",
    "SWE": "Sweden",
    "TCH": "Czechoslovakia",
    "UKR": "Ukraine",
    "URS": "Soviet Union",
    "USA": "United States",
    "YUG": "Yugoslavia",
}


@dataclass(frozen=True)
class PageSpec:
    competition_key: str
    competition_id: str
    competition_name: str
    discipline_key: str
    discipline_name: str
    gender: str
    participant_type: str
    source_url: str
    parser: str


PAGE_SPECS: list[PageSpec] = [
    PageSpec(
        competition_key="uci_road_world_championships_men_road_race",
        competition_id="uci_road_world_championships",
        competition_name="UCI Road World Championships",
        discipline_key="road-race",
        discipline_name="Road Race",
        gender="men",
        participant_type="athlete",
        source_url="https://en.wikipedia.org/wiki/UCI_Road_World_Championships_%E2%80%93_Men%27s_road_race",
        parser="en_world_medals",
    ),
    PageSpec(
        competition_key="uci_road_world_championships_women_road_race",
        competition_id="uci_road_world_championships",
        competition_name="UCI Road World Championships",
        discipline_key="road-race",
        discipline_name="Road Race",
        gender="women",
        participant_type="athlete",
        source_url="https://en.wikipedia.org/wiki/UCI_Road_World_Championships_%E2%80%93_Women%27s_road_race",
        parser="en_world_medals",
    ),
    PageSpec(
        competition_key="uci_road_world_championships_men_time_trial",
        competition_id="uci_road_world_championships",
        competition_name="UCI Road World Championships",
        discipline_key="time-trial",
        discipline_name="Time Trial",
        gender="men",
        participant_type="athlete",
        source_url="https://en.wikipedia.org/wiki/UCI_Road_World_Championships_%E2%80%93_Men%27s_time_trial",
        parser="en_world_medals",
    ),
    PageSpec(
        competition_key="uci_road_world_championships_women_time_trial",
        competition_id="uci_road_world_championships",
        competition_name="UCI Road World Championships",
        discipline_key="time-trial",
        discipline_name="Time Trial",
        gender="women",
        participant_type="athlete",
        source_url="https://en.wikipedia.org/wiki/UCI_Road_World_Championships_%E2%80%93_Women%27s_Time_Trial",
        parser="en_world_medals",
    ),
    PageSpec(
        competition_key="tour_de_france_men",
        competition_id="tour_de_france",
        competition_name="Tour de France",
        discipline_key="road-race",
        discipline_name="Road Race",
        gender="men",
        participant_type="athlete",
        source_url="https://fr.wikipedia.org/wiki/Palmar%C3%A8s_du_Tour_de_France",
        parser="fr_podium",
    ),
    PageSpec(
        competition_key="giro_d_italia_men",
        competition_id="giro_d_italia",
        competition_name="Giro d'Italia",
        discipline_key="road-race",
        discipline_name="Road Race",
        gender="men",
        participant_type="athlete",
        source_url="https://fr.wikipedia.org/wiki/Tour_d%27Italie",
        parser="fr_podium",
    ),
    PageSpec(
        competition_key="vuelta_a_espana_men",
        competition_id="vuelta_a_espana",
        competition_name="Vuelta a Espana",
        discipline_key="road-race",
        discipline_name="Road Race",
        gender="men",
        participant_type="athlete",
        source_url="https://fr.wikipedia.org/wiki/Tour_d%27Espagne",
        parser="fr_podium",
    ),
    PageSpec(
        competition_key="milan_san_remo_men",
        competition_id="milan_san_remo",
        competition_name="Milan-San Remo",
        discipline_key="road-race",
        discipline_name="Road Race",
        gender="men",
        participant_type="athlete",
        source_url="https://fr.wikipedia.org/wiki/Milan-San_Remo",
        parser="fr_podium",
    ),
    PageSpec(
        competition_key="tour_of_flanders_men",
        competition_id="tour_of_flanders",
        competition_name="Tour of Flanders",
        discipline_key="road-race",
        discipline_name="Road Race",
        gender="men",
        participant_type="athlete",
        source_url="https://fr.wikipedia.org/wiki/Tour_des_Flandres",
        parser="fr_podium",
    ),
    PageSpec(
        competition_key="paris_roubaix_men",
        competition_id="paris_roubaix",
        competition_name="Paris-Roubaix",
        discipline_key="road-race",
        discipline_name="Road Race",
        gender="men",
        participant_type="athlete",
        source_url="https://fr.wikipedia.org/wiki/Paris-Roubaix",
        parser="fr_podium",
    ),
    PageSpec(
        competition_key="liege_bastogne_liege_men",
        competition_id="liege_bastogne_liege",
        competition_name="Liege-Bastogne-Liege",
        discipline_key="road-race",
        discipline_name="Road Race",
        gender="men",
        participant_type="athlete",
        source_url="https://fr.wikipedia.org/wiki/Li%C3%A8ge-Bastogne-Li%C3%A8ge",
        parser="fr_podium",
    ),
    PageSpec(
        competition_key="il_lombardia_men",
        competition_id="il_lombardia",
        competition_name="Il Lombardia",
        discipline_key="road-race",
        discipline_name="Road Race",
        gender="men",
        participant_type="athlete",
        source_url="https://fr.wikipedia.org/wiki/Tour_de_Lombardie",
        parser="fr_podium",
    ),
    PageSpec(
        competition_key="tour_de_france_women",
        competition_id="tour_de_france_femmes",
        competition_name="Tour de France Femmes",
        discipline_key="road-race",
        discipline_name="Road Race",
        gender="women",
        participant_type="athlete",
        source_url="https://fr.wikipedia.org/wiki/Tour_de_France_Femmes",
        parser="fr_podium",
    ),
    PageSpec(
        competition_key="giro_d_italia_women",
        competition_id="giro_d_italia_women",
        competition_name="Giro d'Italia Women",
        discipline_key="road-race",
        discipline_name="Road Race",
        gender="women",
        participant_type="athlete",
        source_url="https://fr.wikipedia.org/wiki/Tour_d%27Italie_f%C3%A9minin",
        parser="fr_podium",
    ),
    PageSpec(
        competition_key="vuelta_a_espana_women",
        competition_id="vuelta_a_espana_femenina",
        competition_name="Vuelta a Espana Femenina",
        discipline_key="road-race",
        discipline_name="Road Race",
        gender="women",
        participant_type="athlete",
        source_url="https://fr.wikipedia.org/wiki/Tour_d%27Espagne_f%C3%A9minin",
        parser="fr_podium",
    ),
    PageSpec(
        competition_key="milan_san_remo_women",
        competition_id="milan_san_remo_women",
        competition_name="Milan-San Remo Women",
        discipline_key="road-race",
        discipline_name="Road Race",
        gender="women",
        participant_type="athlete",
        source_url="https://fr.wikipedia.org/wiki/Milan-San_Remo_f%C3%A9minin",
        parser="fr_podium",
    ),
    PageSpec(
        competition_key="tour_of_flanders_women",
        competition_id="tour_of_flanders_women",
        competition_name="Tour of Flanders Women",
        discipline_key="road-race",
        discipline_name="Road Race",
        gender="women",
        participant_type="athlete",
        source_url="https://fr.wikipedia.org/wiki/Tour_des_Flandres_f%C3%A9minin",
        parser="fr_podium",
    ),
    PageSpec(
        competition_key="paris_roubaix_women",
        competition_id="paris_roubaix_femmes",
        competition_name="Paris-Roubaix Femmes",
        discipline_key="road-race",
        discipline_name="Road Race",
        gender="women",
        participant_type="athlete",
        source_url="https://fr.wikipedia.org/wiki/Paris-Roubaix_Femmes",
        parser="fr_podium",
    ),
    PageSpec(
        competition_key="liege_bastogne_liege_women",
        competition_id="liege_bastogne_liege_women",
        competition_name="Liege-Bastogne-Liege Women",
        discipline_key="road-race",
        discipline_name="Road Race",
        gender="women",
        participant_type="athlete",
        source_url="https://fr.wikipedia.org/wiki/Li%C3%A8ge-Bastogne-Li%C3%A8ge_f%C3%A9minin",
        parser="fr_podium",
    ),
]


def normalize(text: str) -> str:
    value = unicodedata.normalize("NFKD", str(text))
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = value.replace("’", "'")
    value = re.sub(r"\s+", " ", value).strip().lower()
    return value


def extract_year(text: str) -> int | None:
    match = re.search(r"\b(18\d{2}|19\d{2}|20\d{2})\b", text)
    if not match:
        return None
    return int(match.group(1))


def extract_country_from_flag_title(title: str) -> str | None:
    cleaned = re.sub(r"\s+", " ", title).strip()
    patterns = [
        r"^Drapeau\s*:\s*(.+)$",
        r"^Drapeau\s+de\s+l['’](.+)$",
        r"^Drapeau\s+de\s+la\s+(.+)$",
        r"^Drapeau\s+du\s+(.+)$",
        r"^Drapeau\s+des\s+(.+)$",
    ]
    for pattern in patterns:
        match = re.match(pattern, cleaned, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return None


def extract_country_code(country_name: str, text: str) -> str | None:
    text_code = re.search(r"\(\s*([A-Z]{3})\s*\)", text)
    if text_code:
        return text_code.group(1)

    key = normalize(country_name)
    if key in COUNTRY_CODE_OVERRIDES:
        return COUNTRY_CODE_OVERRIDES[key]

    try:
        import pycountry

        country = pycountry.countries.lookup(country_name)
        code = getattr(country, "alpha_3", None)
        if code:
            return code
    except Exception:
        pass

    return None


def clean_text(text: str) -> str:
    cleaned = re.sub(r"\[[^\]]+\]", "", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def parse_athlete_cell_fr(cell: Tag) -> tuple[str | None, str | None, str | None]:
    raw_text = clean_text(cell.get_text(" ", strip=True))
    country_name: str | None = None
    country_code: str | None = None

    for link in cell.select("a[title]"):
        title = clean_text(link.get("title", ""))
        if normalize(title).startswith("drapeau"):
            country_name = extract_country_from_flag_title(title)
            if country_name:
                break
        if re.fullmatch(r"[A-Z]{3}", title):
            country_code = title
            country_name = CODE_TO_COUNTRY.get(title, title)
            break
        parent_classes = set(link.parent.get("class") or [])
        if not country_name and parent_classes.intersection({"mw-image-border", "flagicon"}):
            country_name = title or country_name

    athlete_name: str | None = None
    for link in cell.find_all("a"):
        if any(cls in {"mw-image-border", "noviewer", "flagicon"} for cls in (link.parent.get("class") or [])):
            continue
        text = clean_text(link.get_text(" ", strip=True))
        if not text:
            continue
        if normalize(text).startswith("drapeau"):
            continue
        athlete_name = text
        break

    if not athlete_name:
        match = re.match(r"^(.*?)\s*(?:\(.*\))?$", raw_text)
        athlete_name = clean_text(match.group(1)) if match else raw_text

    if not athlete_name or athlete_name in {"-", "—"}:
        return None, None, None

    if not country_code:
        country_code = extract_country_code(country_name or "", raw_text)
    if not country_name and country_code:
        country_name = CODE_TO_COUNTRY.get(country_code, country_code)

    return athlete_name, country_name, country_code


def parse_athlete_cell_en(cell: Tag) -> tuple[str | None, str | None, str | None]:
    raw_text = clean_text(cell.get_text(" ", strip=True))
    athlete_name: str | None = None

    for link in cell.find_all("a"):
        text = clean_text(link.get_text(" ", strip=True))
        if not text or text.lower() == "details":
            continue
        athlete_name = text
        break

    if not athlete_name:
        match = re.match(r"^(.*?)\s*\(\s*[A-Z]{3}\s*\)", raw_text)
        athlete_name = clean_text(match.group(1)) if match else raw_text

    code_match = re.search(r"\(\s*([A-Z]{3})\s*\)", raw_text)
    country_code = code_match.group(1) if code_match else None
    country_name = CODE_TO_COUNTRY.get(country_code or "", country_code)

    if not athlete_name or athlete_name in {"-", "—"}:
        return None, None, None

    return athlete_name, country_name, country_code


def table_headers(table: Tag) -> list[str]:
    first_rows = table.find_all("tr")[:3]
    headers: list[str] = []
    for row in first_rows:
        ths = row.find_all("th", recursive=False)
        if not ths:
            continue
        row_headers = [clean_text(th.get_text(" ", strip=True)) for th in ths]
        if row_headers and len(row_headers) >= len(headers):
            headers = row_headers
    return headers


def locate_podium_columns(headers: Iterable[str]) -> tuple[int, int, int, int] | None:
    idx_year = idx_first = idx_second = idx_third = None
    for idx, header in enumerate(headers):
        value = normalize(header)
        if idx_year is None and ("annee" in value or value == "year" or "championship" in value):
            idx_year = idx
        if idx_first is None and (
            "vainqueur" in value
            or value == "gold"
            or value.startswith("1 er")
            or value.startswith("1e")
            or value.startswith("1re")
        ):
            idx_first = idx
        if idx_second is None and (
            "deux" in value or value == "silver" or value.startswith("2 e") or value.startswith("2e")
        ):
            idx_second = idx
        if idx_third is None and (
            "trois" in value or value == "bronze" or value.startswith("3 e") or value.startswith("3e")
        ):
            idx_third = idx

    if None in {idx_year, idx_first, idx_second, idx_third}:
        return None
    return int(idx_year), int(idx_first), int(idx_second), int(idx_third)


def parse_fr_podium_table(soup: BeautifulSoup, spec: PageSpec) -> list[dict[str, str | int | None]]:
    records: list[dict[str, str | int | None]] = []
    for table in soup.find_all("table"):
        headers = table_headers(table)
        indices = locate_podium_columns(headers)
        if indices is None:
            continue
        idx_year, idx_first, idx_second, idx_third = indices
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"], recursive=False)
            if len(cells) <= max(idx_year, idx_first, idx_second, idx_third):
                continue
            year = extract_year(clean_text(cells[idx_year].get_text(" ", strip=True)))
            if year is None:
                continue
            podium_cells = {1: cells[idx_first], 2: cells[idx_second], 3: cells[idx_third]}
            for rank, podium_cell in podium_cells.items():
                athlete_name, country_name, country_code = parse_athlete_cell_fr(podium_cell)
                if not athlete_name or not country_name:
                    continue
                records.append(
                    {
                        "competition_key": spec.competition_key,
                        "competition_id": spec.competition_id,
                        "competition_name": spec.competition_name,
                        "year": year,
                        "event_date": f"{year}-12-31",
                        "discipline_key": spec.discipline_key,
                        "discipline_name": spec.discipline_name,
                        "gender": spec.gender,
                        "rank": rank,
                        "medal": RANK_TO_MEDAL[rank],
                        "participant_type": spec.participant_type,
                        "participant_name": athlete_name,
                        "country_name": country_name,
                        "country_code": country_code,
                        "source_url": spec.source_url,
                    }
                )
    return records


def parse_en_world_medal_table(soup: BeautifulSoup, spec: PageSpec) -> list[dict[str, str | int | None]]:
    records: list[dict[str, str | int | None]] = []
    for table in soup.find_all("table"):
        headers = table_headers(table)
        indices = locate_podium_columns(headers)
        if indices is None:
            continue
        idx_year, idx_gold, idx_silver, idx_bronze = indices
        normalized_headers = [normalize(h) for h in headers]
        if "gold" not in normalized_headers or "silver" not in normalized_headers or "bronze" not in normalized_headers:
            continue

        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"], recursive=False)
            if len(cells) <= max(idx_year, idx_gold, idx_silver, idx_bronze):
                continue
            year = extract_year(clean_text(cells[idx_year].get_text(" ", strip=True)))
            if year is None:
                continue

            podium_cells = {1: cells[idx_gold], 2: cells[idx_silver], 3: cells[idx_bronze]}
            for rank, podium_cell in podium_cells.items():
                athlete_name, country_name, country_code = parse_athlete_cell_en(podium_cell)
                if not athlete_name or not country_name:
                    continue
                records.append(
                    {
                        "competition_key": spec.competition_key,
                        "competition_id": spec.competition_id,
                        "competition_name": spec.competition_name,
                        "year": year,
                        "event_date": f"{year}-12-31",
                        "discipline_key": spec.discipline_key,
                        "discipline_name": spec.discipline_name,
                        "gender": spec.gender,
                        "rank": rank,
                        "medal": RANK_TO_MEDAL[rank],
                        "participant_type": spec.participant_type,
                        "participant_name": athlete_name,
                        "country_name": country_name,
                        "country_code": country_code,
                        "source_url": spec.source_url,
                    }
                )
        if records:
            break
    return records


def fetch_html(spec: PageSpec) -> str:
    response = requests.get(spec.source_url, timeout=40, headers=HEADERS)
    response.raise_for_status()
    return response.text


def parse_spec(spec: PageSpec, html: str) -> list[dict[str, str | int | None]]:
    soup = BeautifulSoup(html, "html.parser")
    if spec.parser == "fr_podium":
        return parse_fr_podium_table(soup, spec)
    if spec.parser == "en_world_medals":
        return parse_en_world_medal_table(soup, spec)
    raise RuntimeError(f"Unsupported parser: {spec.parser}")


def main() -> None:
    PAGES_DIR.mkdir(parents=True, exist_ok=True)

    all_rows: list[dict[str, str | int | None]] = []
    for spec in PAGE_SPECS:
        html = fetch_html(spec)
        page_slug = spec.source_url.split("/wiki/")[-1]
        page_path = PAGES_DIR / f"{page_slug}.html"
        page_path.write_text(html, encoding="utf-8")
        rows = parse_spec(spec, html)
        if not rows:
            raise RuntimeError(f"No rows parsed for {spec.competition_key} from {spec.source_url}")
        all_rows.extend(rows)

    frame = pd.DataFrame(all_rows)
    frame = frame.drop_duplicates(
        subset=[
            "competition_id",
            "year",
            "discipline_key",
            "gender",
            "rank",
            "participant_name",
            "country_name",
        ]
    )

    frame["year"] = pd.to_numeric(frame["year"], errors="coerce")
    frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce")
    frame = frame.dropna(subset=["year", "rank", "participant_name", "country_name"]).copy()
    frame["year"] = frame["year"].astype(int)
    frame["rank"] = frame["rank"].astype(int)

    # Keep strict Top 3 profile by event.
    profiles = (
        frame.groupby(["competition_id", "year", "discipline_key", "gender"])["rank"]
        .apply(lambda s: tuple(sorted(s.tolist())))
        .to_dict()
    )
    invalid = {k: v for k, v in profiles.items() if v != (1, 2, 3)}
    if invalid:
        print(f"[seed] dropping {len(invalid)} incomplete event(s) without strict top-3 profile")
        for key, profile in list(sorted(invalid.items()))[:30]:
            print(f" - {key}: {profile}")
        invalid_keys = set(invalid.keys())
        frame = frame.loc[
            ~frame.apply(
                lambda row: (
                    row["competition_id"],
                    int(row["year"]),
                    row["discipline_key"],
                    row["gender"],
                )
                in invalid_keys,
                axis=1,
            )
        ].copy()
        profiles = (
            frame.groupby(["competition_id", "year", "discipline_key", "gender"])["rank"]
            .apply(lambda s: tuple(sorted(s.tolist())))
            .to_dict()
        )
        still_invalid = {k: v for k, v in profiles.items() if v != (1, 2, 3)}
        if still_invalid:
            sample = dict(list(still_invalid.items())[:20])
            raise RuntimeError(f"Unexpected rank profile(s) after filtering: {sample}")

    unresolved_codes = frame.loc[frame["country_code"].isna() | (frame["country_code"] == ""), "country_name"]
    if not unresolved_codes.empty:
        missing = sorted(set(unresolved_codes.astype(str)))
        print("[seed] unresolved country_code count:", len(missing))
        for name in missing:
            print(" -", name)

    frame = frame.sort_values(
        ["competition_id", "year", "discipline_key", "gender", "rank", "participant_name"]
    ).reset_index(drop=True)

    frame.to_csv(SEED_PATH, index=False)

    events = frame[["competition_id", "year", "discipline_key", "gender"]].drop_duplicates().shape[0]
    print(f"[seed] rows={len(frame)} events={events}")
    for competition_id, count in frame.groupby("competition_id").size().sort_values(ascending=False).items():
        print(f"[seed] {competition_id}: {count} rows")
    print(f"[seed] wrote {SEED_PATH}")


if __name__ == "__main__":
    main()
