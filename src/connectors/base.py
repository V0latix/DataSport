from __future__ import annotations

import json
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from src.core.db import SQLiteDB
from src.core.utils import safe_mkdir


DEFAULT_USER_AGENT = "DataSportPipeline/0.1 (open-source nations ranking builder)"


class MissingCredentialError(RuntimeError):
    """Raised when connector cannot run due to missing API credentials."""


class Connector(ABC):
    id: str = ""
    name: str = ""
    source_type: str = "api"
    license_notes: str = ""
    base_url: str = ""

    @abstractmethod
    def fetch(self, season_year: int, out_dir: Path) -> list[Path]:
        raise NotImplementedError

    @abstractmethod
    def parse(self, raw_paths: list[Path], season_year: int) -> dict[str, pd.DataFrame]:
        raise NotImplementedError

    @abstractmethod
    def upsert(self, db: SQLiteDB, payload: dict[str, pd.DataFrame]) -> None:
        raise NotImplementedError

    def export(self, payload: dict[str, pd.DataFrame], exports_dir: Path) -> None:
        safe_mkdir(exports_dir)
        for name, frame in payload.items():
            if frame is None or frame.empty:
                continue
            csv_path = exports_dir / f"{name}.csv"
            frame.to_csv(csv_path, index=False)

    def source_row(self) -> dict[str, str]:
        return {
            "source_id": self.id,
            "source_name": self.name,
            "source_type": self.source_type,
            "license_notes": self.license_notes,
            "base_url": self.base_url,
        }

    def _request_json(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        timeout: int = 45,
        retries: int = 3,
        sleep_seconds: float = 1.0,
    ) -> dict[str, Any]:
        combined_headers = {"User-Agent": DEFAULT_USER_AGENT}
        if headers:
            combined_headers.update(headers)

        last_error: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                response = requests.get(url, headers=combined_headers, params=params, timeout=timeout)
                if response.status_code in (429, 500, 502, 503, 504):
                    response.raise_for_status()
                if response.status_code >= 400:
                    response.raise_for_status()
                return response.json()
            except Exception as exc:
                last_error = exc
                if attempt == retries:
                    break
                time.sleep(sleep_seconds * attempt)
        if last_error:
            raise last_error
        raise RuntimeError(f"Request failed with unknown error for {url}")

    @staticmethod
    def _write_json(path: Path, payload: dict[str, Any]) -> None:
        safe_mkdir(path.parent)
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
