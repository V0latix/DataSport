from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.core.db import SQLiteDB
from src.core.utils import slugify, utc_now_iso

from .base import Connector


SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
SPARQL_QUERY = """
SELECT ?sport ?sportLabel ?federation ?federationLabel WHERE {
  ?sport wdt:P31/wdt:P279* wd:Q31629.
  OPTIONAL { ?sport wdt:P2416 ?federation. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
LIMIT 500
"""


SAMPLE_BINDINGS = {
    "results": {
        "bindings": [
            {
                "sport": {"type": "uri", "value": "http://www.wikidata.org/entity/Q349"},
                "sportLabel": {"xml:lang": "en", "type": "literal", "value": "Sport"},
                "federation": {"type": "uri", "value": "http://www.wikidata.org/entity/Q5296"},
                "federationLabel": {"xml:lang": "en", "type": "literal", "value": "International Olympic Committee"},
            },
            {
                "sport": {"type": "uri", "value": "http://www.wikidata.org/entity/Q2736"},
                "sportLabel": {"xml:lang": "en", "type": "literal", "value": "Association football"},
                "federation": {"type": "uri", "value": "http://www.wikidata.org/entity/Q186854"},
                "federationLabel": {"xml:lang": "en", "type": "literal", "value": "FIFA"},
            },
            {
                "sport": {"type": "uri", "value": "http://www.wikidata.org/entity/Q5372"},
                "sportLabel": {"xml:lang": "en", "type": "literal", "value": "Basketball"},
                "federation": {"type": "uri", "value": "http://www.wikidata.org/entity/Q1140110"},
                "federationLabel": {"xml:lang": "en", "type": "literal", "value": "FIBA"},
            },
        ]
    }
}


class WikidataConnector(Connector):
    id = "wikidata"
    name = "Wikidata SPARQL"
    source_type = "sparql"
    license_notes = "CC0 (Wikidata), verify downstream license compatibility."
    base_url = SPARQL_ENDPOINT

    def fetch(self, season_year: int, out_dir: Path) -> list[Path]:
        del season_year
        out_path = out_dir / "wikidata_sport_federations.json"
        metadata_path = out_dir / "fetch_meta.json"
        headers = {"Accept": "application/sparql-results+json"}
        params = {"query": SPARQL_QUERY, "format": "json"}
        mode = "live"

        try:
            payload = self._request_json(SPARQL_ENDPOINT, headers=headers, params=params, retries=3, sleep_seconds=1.5)
        except Exception as exc:
            payload = SAMPLE_BINDINGS
            mode = f"fallback_sample ({exc})"

        self._write_json(out_path, payload)
        self._write_json(metadata_path, {"mode": mode, "fetched_at_utc": utc_now_iso()})
        return [out_path, metadata_path]

    def parse(self, raw_paths: list[Path], season_year: int) -> dict[str, pd.DataFrame]:
        del season_year
        data_path = next(path for path in raw_paths if path.name.endswith("wikidata_sport_federations.json"))
        payload = json.loads(data_path.read_text(encoding="utf-8"))
        bindings = payload.get("results", {}).get("bindings", [])
        timestamp = utc_now_iso()

        sports_rows: list[dict[str, object]] = []
        federation_rows: list[dict[str, object]] = []
        for row in bindings:
            sport_name = row.get("sportLabel", {}).get("value")
            sport_uri = row.get("sport", {}).get("value", "")
            if not sport_name:
                continue
            sport_id = slugify(sport_name)
            sports_rows.append(
                {
                    "sport_id": sport_id,
                    "sport_name": sport_name,
                    "sport_slug": sport_id,
                    "created_at_utc": timestamp,
                }
            )
            federation_uri = row.get("federation", {}).get("value")
            federation_name = row.get("federationLabel", {}).get("value")
            if federation_uri:
                federation_rows.append(
                    {
                        "sport_id": sport_id,
                        "federation_qid": federation_uri.rsplit("/", 1)[-1],
                        "federation_name": federation_name,
                    }
                )

            if sport_uri and sport_uri.startswith("http://www.wikidata.org/entity/"):
                pass

        sports_df = pd.DataFrame(sports_rows).drop_duplicates(subset=["sport_id"])
        federation_df = pd.DataFrame(federation_rows).drop_duplicates(subset=["sport_id", "federation_qid"])
        empty = pd.DataFrame()
        return {
            "sports": sports_df,
            "sport_federations": federation_df,
            "competitions": empty,
            "events": empty,
            "participants": empty,
            "results": empty,
        }

    def upsert(self, db: SQLiteDB, payload: dict[str, pd.DataFrame]) -> None:
        db.upsert_dataframe("sports", payload.get("sports", pd.DataFrame()), ["sport_id"])
        db.upsert_dataframe(
            "sport_federations",
            payload.get("sport_federations", pd.DataFrame()),
            ["sport_id", "federation_qid"],
        )

