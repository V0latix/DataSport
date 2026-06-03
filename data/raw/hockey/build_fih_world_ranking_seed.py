from __future__ import annotations

import argparse
import re
import zipfile
import zlib
from collections import defaultdict
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import requests
from lxml import html


SEED_PATH = Path(__file__).resolve().parent / "fih_world_rankings_top10_seed.csv"
ZIP_URL = "https://www.fih.hockey/static-assets/pdf/rankings-archive-2003-2024.zip"
SOURCE_PAGE = "https://www.fih.hockey/outdoor-hockey-rankings/archive"
HEADERS = {"User-Agent": "Mozilla/5.0 (DataSport FIH rankings seed builder)"}
CURRENT_RANKINGS_URL = "https://www.fih.hockey/outdoor-hockey-rankings"
NETHERLANDS_WOMEN_URL = "https://www.fih.hockey/outdoor-rankings/netherlands-women-hockey-rankings-48"

COMPETITIONS = {
    "men": {
        "competition_id": "fih_men_world_ranking",
        "competition_name": "FIH Men's World Ranking",
        "pdf_marker": "Men's World Rankings",
    },
    "women": {
        "competition_id": "fih_women_world_ranking",
        "competition_name": "FIH Women's World Ranking",
        "pdf_marker": "Women's World Rankings",
    },
}

COUNTRY_OVERRIDES = {
    "China": "CHN",
    "Chinese Taipei": "TPE",
    "Czech Republic": "CZE",
    "Czechia": "CZE",
    "England": "ENG",
    "Hong Kong China": "HKG",
    "Korea": "KOR",
    "South Africa": "ZAF",
    "United States": "USA",
    "USA": "USA",
}

COUNTRY_NAME_NORMALIZATION = {
    "New": "New Zealand",
    "Usa": "USA",
    "United States Of America": "United States",
}
MONTHS = {month: index for index, month in enumerate(["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}


def _decode_pdf_string(raw: bytes) -> str:
    out: list[str] = []
    idx = 0
    while idx < len(raw):
        char = raw[idx]
        if char == 92:
            idx += 1
            if idx >= len(raw):
                break
            escaped = raw[idx]
            mapping = {
                ord("n"): "\n",
                ord("r"): "\r",
                ord("t"): "\t",
                ord("b"): "\b",
                ord("f"): "\f",
                ord("("): "(",
                ord(")"): ")",
                ord("\\"): "\\",
            }
            if escaped in mapping:
                out.append(mapping[escaped])
            elif 48 <= escaped <= 55:
                octal = bytes([escaped])
                cursor = idx + 1
                while cursor < len(raw) and len(octal) < 3 and 48 <= raw[cursor] <= 55:
                    octal += bytes([raw[cursor]])
                    cursor += 1
                out.append(chr(int(octal, 8)))
                idx = cursor - 1
            else:
                out.append(chr(escaped))
        else:
            out.append(chr(char))
        idx += 1
    return "".join(out)


def _extract_pdf_items(pdf_bytes: bytes) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    streams = re.findall(rb"stream\r?\n(.*?)\r?\nendstream", pdf_bytes, flags=re.S)
    for stream_index, stream in enumerate(streams):
        try:
            decoded = zlib.decompress(stream)
        except zlib.error:
            continue

        for text_block in re.findall(rb"BT\r?\n(.*?)\r?\nET", decoded, flags=re.S):
            matrix = re.search(
                rb"1\s+0\s+0\s+1\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s+Tm",
                text_block,
            )
            if not matrix:
                continue

            text_parts: list[str] = []
            for array_text in re.findall(rb"\[(.*?)\]\s*TJ", text_block, flags=re.S):
                strings = re.finditer(rb"\((?:\\.|[^\\)])*\)", array_text, flags=re.S)
                text_parts.extend(_decode_pdf_string(match.group(0)[1:-1]) for match in strings)

            text = re.sub(r"\s+", " ", "".join(text_parts).replace("\xa0", " ")).strip()
            if text:
                items.append(
                    {
                        "page": stream_index,
                        "x": float(matrix.group(1)),
                        "y": float(matrix.group(2)),
                        "text": text,
                    }
                )
    return items


def _clean_team_name(value: str) -> str:
    text = value.replace("*", "").strip()
    text = re.sub(r"\s+", " ", text)
    text = text.title().replace(" And ", " and ")
    return COUNTRY_NAME_NORMALIZATION.get(text, text)


def _team_name_from_detail(title: str, url: str, gender: str) -> str:
    match = re.match(rf"(.+?)\s+{gender.title()}\s+World Hockey Ranking", title)
    if match:
        candidate = _clean_team_name(match.group(1))
        if candidate and candidate not in {"New", "South", "United"}:
            return candidate

    slug = url.rstrip("/").split("/")[-1]
    slug = re.sub(rf"-{gender}-hockey-rankings-\d+$", "", slug)
    slug = slug.replace("---", "-")
    return _clean_team_name(" ".join(part for part in slug.split("-") if part))


def _country_code(country_name: str) -> str:
    alias = COUNTRY_OVERRIDES.get(country_name)
    if alias:
        return alias

    try:
        import pycountry

        country = pycountry.countries.lookup(country_name)
        code = getattr(country, "alpha_3", None)
        if code:
            return str(code).upper()
    except Exception:
        pass

    return re.sub(r"[^A-Za-z0-9]", "", country_name.upper())[:3]


def _parse_pdf_rows(pdf_bytes: bytes, gender: str, max_rank: int) -> list[dict[str, Any]]:
    items = _extract_pdf_items(pdf_bytes)
    rows: list[dict[str, Any]] = []
    meta = COMPETITIONS[gender]

    for page in sorted({item["page"] for item in items}):
        page_items = [item for item in items if item["page"] == page]
        year_headers = sorted(
            [
                item
                for item in page_items
                if re.fullmatch(r"20\d{2}", item["text"]) and float(item["y"]) > 450
            ],
            key=lambda item: float(item["x"]),
        )
        if not year_headers:
            continue

        years = [int(item["text"]) for item in year_headers]
        min_year_x = min(float(item["x"]) for item in year_headers)
        header_y = min(float(item["y"]) for item in year_headers)
        row_items = [
            item
            for item in page_items
            if 100 <= float(item["y"]) < header_y - 3
            and not re.fullmatch(r"20\d{2}", item["text"])
            and item["text"] not in {"World", "Ranking"}
            and "Rankings" not in item["text"]
        ]

        grouped_by_y: dict[float, list[dict[str, Any]]] = defaultdict(list)
        for item in row_items:
            grouped_by_y[round(float(item["y"]), 2)].append(item)

        inferred_rank = 0
        for y_value in sorted(grouped_by_y.keys(), reverse=True):
            cells = sorted(grouped_by_y[y_value], key=lambda item: float(item["x"]))
            rank_cells = [
                cell
                for cell in cells
                if re.fullmatch(r"\d{1,3}", cell["text"]) and float(cell["x"]) < min_year_x
            ]
            if rank_cells:
                rank = int(rank_cells[0]["text"])
            else:
                inferred_rank += 1
                rank = inferred_rank

            if rank > max_rank:
                continue

            team_cells = [cell for cell in cells if not re.fullmatch(r"\d{1,3}", cell["text"])]
            if len(team_cells) != len(years):
                raise RuntimeError(
                    f"Unexpected FIH PDF row shape for {gender} page={page} rank={rank}: "
                    f"{len(team_cells)} teams for {len(years)} years"
                )

            for year, cell in zip(years, team_cells):
                country_name = _clean_team_name(str(cell["text"]))
                rows.append(
                    {
                        "competition_id": meta["competition_id"],
                        "competition_name": meta["competition_name"],
                        "year": year,
                        "event_date": f"{year}-12-31",
                        "discipline_key": "hockey",
                        "discipline_name": "Hockey",
                        "gender": gender,
                        "rank": rank,
                        "participant_type": "team",
                        "participant_name": country_name,
                        "country_name": country_name,
                        "country_code": _country_code(country_name),
                        "source_url": ZIP_URL,
                    }
                )
    return rows


def _field_text(node: Any, class_name: str) -> str:
    return re.sub(r"\s+", " ", node.xpath(f'string(.//*[contains(@class,"{class_name}")])')).strip()


def _extract_team_links(page_html: str, gender: str) -> list[str]:
    document = html.fromstring(page_html)
    links = {
        str(anchor.get("href"))
        for anchor in document.xpath("//a[@href]")
        if f"{gender}-hockey-rankings" in str(anchor.get("href") or "")
    }
    return sorted(links)


def _snapshot_points_from_detail(page_html: str, url: str, gender: str) -> dict[str, Any] | None:
    document = html.fromstring(page_html)
    title = document.xpath("string(//title)").strip()
    team_name = _team_name_from_detail(title, url, gender)
    rank_text = document.xpath(
        'string(//*[contains(@class,"rank-details")]//*[contains(@class,"team-rank")]//*[contains(@class,"rank-number")][1])'
    ).strip()
    points_text = document.xpath(
        'string(//*[contains(@class,"rank-details")]//*[contains(@class,"best-rank")]//*[contains(@class,"rank-number")][1])'
    ).strip()

    try:
        current_rank = int(float(rank_text))
    except Exception:
        current_rank = None
    try:
        current_points = float(points_text)
    except Exception:
        current_points = None

    first_2026_rows: list[tuple[pd.Timestamp, float, int]] = []
    for row in document.xpath('//*[contains(@class,"table-row-top")]'):
        date_text = _field_text(row, "date")
        parts = date_text.split()
        if len(parts) != 2 or parts[1] not in MONTHS:
            continue

        day = int(parts[0])
        month = MONTHS[parts[1]]
        detail_text = re.sub(r"\s+", " ", row.getparent().text_content())
        if "2025-26" in detail_text and month <= 6:
            year = 2026
        elif "2025-26" in detail_text and month >= 7:
            year = 2025
        else:
            year = 2026 if month <= 6 else 2025

        if year <= 2025:
            continue

        try:
            points_before = float(_field_text(row, "points-before"))
            rank_before = int(float(_field_text(row, "rank-before")))
        except Exception:
            continue
        first_2026_rows.append((pd.Timestamp(year=year, month=month, day=day), points_before, rank_before))

    if first_2026_rows:
        first_row = min(first_2026_rows, key=lambda item: item[0])
        return {
            "team_name": team_name,
            "country_name": team_name,
            "country_code": _country_code(team_name),
            "points": first_row[1],
            "rank_before": first_row[2],
            "source_url": url,
        }

    if current_points is None:
        return None
    return {
        "team_name": team_name,
        "country_name": team_name,
        "country_code": _country_code(team_name),
        "points": current_points,
        "rank_before": current_rank,
        "source_url": url,
    }


def _build_2025_rows(max_rank: int) -> list[dict[str, Any]]:
    session = requests.Session()
    session.headers.update(HEADERS)
    current_response = session.get(CURRENT_RANKINGS_URL, timeout=90)
    current_response.raise_for_status()
    women_response = session.get(NETHERLANDS_WOMEN_URL, timeout=90)
    women_response.raise_for_status()

    links_by_gender = {
        "men": _extract_team_links(current_response.text, "men"),
        "women": _extract_team_links(women_response.text, "women"),
    }

    rows: list[dict[str, Any]] = []
    for gender, links in links_by_gender.items():
        snapshot_rows: list[dict[str, Any]] = []
        for href in links:
            url = urljoin(CURRENT_RANKINGS_URL, href)
            response = session.get(url, timeout=60)
            if response.status_code == 404:
                continue
            response.raise_for_status()
            item = _snapshot_points_from_detail(response.text, url, gender)
            if item:
                snapshot_rows.append(item)

        ranked = sorted(snapshot_rows, key=lambda item: (-float(item["points"]), str(item["team_name"])))
        meta = COMPETITIONS[gender]
        for rank, item in enumerate(ranked[:max_rank], start=1):
            rows.append(
                {
                    "competition_id": meta["competition_id"],
                    "competition_name": meta["competition_name"],
                    "year": 2025,
                    "event_date": "2025-12-31",
                    "discipline_key": "hockey",
                    "discipline_name": "Hockey",
                    "gender": gender,
                    "rank": rank,
                    "participant_type": "team",
                    "participant_name": item["team_name"],
                    "country_name": item["country_name"],
                    "country_code": item["country_code"],
                    "source_url": item["source_url"],
                }
            )
    return rows


def build_seed(max_rank: int, include_2025_snapshot: bool) -> pd.DataFrame:
    response = requests.get(ZIP_URL, headers=HEADERS, timeout=90)
    response.raise_for_status()

    rows: list[dict[str, Any]] = []
    with zipfile.ZipFile(BytesIO(response.content)) as archive:
        names = archive.namelist()
        for gender, meta in COMPETITIONS.items():
            pdf_name = next((name for name in names if meta["pdf_marker"] in name and name.endswith(".pdf")), None)
            if pdf_name is None:
                raise RuntimeError(f"Missing {meta['pdf_marker']} PDF in FIH rankings archive.")
            rows.extend(_parse_pdf_rows(archive.read(pdf_name), gender=gender, max_rank=max_rank))

    if include_2025_snapshot:
        rows.extend(_build_2025_rows(max_rank=max_rank))

    frame = pd.DataFrame(rows)
    if frame.empty:
        raise RuntimeError("FIH rankings seed extraction produced no rows.")

    frame = frame.sort_values(["competition_id", "year", "rank", "country_code"]).reset_index(drop=True)
    profiles = frame.groupby(["competition_id", "year"])["rank"].apply(lambda values: tuple(sorted(values))).to_dict()
    expected = tuple(range(1, max_rank + 1))
    bad_profiles = {key: value for key, value in profiles.items() if value != expected}
    if bad_profiles:
        sample = dict(list(bad_profiles.items())[:10])
        raise RuntimeError(f"Unexpected FIH ranking profiles: {sample}")
    return frame


def main() -> None:
    parser = argparse.ArgumentParser(description="Build FIH World Ranking top10 seed from official archive PDFs.")
    parser.add_argument("--max-rank", type=int, default=10)
    parser.add_argument(
        "--skip-2025-snapshot",
        action="store_true",
        help="Only parse the official 2003-2024 archive ZIP; do not derive the 2025 snapshot from team pages.",
    )
    parser.add_argument("--output", type=Path, default=SEED_PATH)
    args = parser.parse_args()

    frame = build_seed(max_rank=args.max_rank, include_2025_snapshot=not args.skip_2025_snapshot)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(args.output, index=False)
    print(
        f"Wrote {len(frame)} rows to {args.output} "
        f"({frame['year'].min()}-{frame['year'].max()}, source={SOURCE_PAGE})"
    )


if __name__ == "__main__":
    main()
