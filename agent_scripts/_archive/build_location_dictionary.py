"""Build locationDictionary.json keyed by city and seeded with top cities by population."""

from __future__ import annotations

import io
import json
import sys
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
DICTIONARY_PATH = ROOT / "job_board_application" / "convex" / "locationDictionary.json"
CITIES_URL = "https://download.geonames.org/export/dump/cities15000.zip"
COUNTRY_INFO_URL = "https://download.geonames.org/export/dump/countryInfo.txt"
TOP_N = 1000

STATE_NAME_BY_ABBR = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DC": "District of Columbia",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "IA": "Iowa",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "MA": "Massachusetts",
    "MD": "Maryland",
    "ME": "Maine",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MO": "Missouri",
    "MS": "Mississippi",
    "MT": "Montana",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "NE": "Nebraska",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NV": "Nevada",
    "NY": "New York",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VA": "Virginia",
    "VT": "Vermont",
    "WA": "Washington",
    "WI": "Wisconsin",
    "WV": "West Virginia",
    "WY": "Wyoming",
}


@dataclass(frozen=True)
class CityRow:
    name: str
    country_code: str
    admin1: str
    population: int


def _fetch_bytes(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=60) as response:
        return response.read()


def _to_ascii(value: str) -> str:
    try:
        import unicodedata

        normalized = unicodedata.normalize("NFKD", value)
        encoded = normalized.encode("ascii", "ignore").decode("ascii")
        return encoded
    except Exception:
        return value


def _parse_country_info(text: str) -> dict[str, str]:
    countries: dict[str, str] = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) < 5:
            continue
        code = parts[0].strip()
        name = _to_ascii(parts[4].strip())
        if code and name:
            countries[code] = name
    return countries


def _parse_city_rows(zip_bytes: bytes) -> list[CityRow]:
    rows: list[CityRow] = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zip_file:
        names = zip_file.namelist()
        if not names:
            return rows
        with zip_file.open(names[0]) as handle:
            for raw in handle:
                line = raw.decode("utf-8", errors="ignore").strip()
                if not line:
                    continue
                parts = line.split("\t")
                if len(parts) < 15:
                    continue
                asciiname = _to_ascii(parts[2].strip())
                if not asciiname:
                    continue
                country_code = parts[8].strip()
                admin1 = parts[10].strip()
                try:
                    population = int(parts[14]) if parts[14] else 0
                except ValueError:
                    population = 0
                rows.append(
                    CityRow(
                        name=asciiname,
                        country_code=country_code,
                        admin1=admin1,
                        population=population,
                    )
                )
    return rows


def _load_existing_entries() -> list[dict[str, Any]]:
    raw = json.loads(DICTIONARY_PATH.read_text("utf-8"))
    if isinstance(raw, list):
        return raw
    entries: list[dict[str, Any]] = []
    if isinstance(raw, dict):
        for value in raw.values():
            if isinstance(value, list):
                entries.extend(value)
            else:
                entries.append(value)
    return entries


def _normalize_entry(entry: dict[str, Any], fallback_city: str | None = None) -> dict[str, Any]:
    city = (entry.get("city") or fallback_city or "").strip()
    state = (entry.get("state") or "Unknown").strip()
    country = (entry.get("country") or "Unknown").strip()
    aliases = [alias.strip() for alias in entry.get("aliases", []) if isinstance(alias, str) and alias.strip()]
    normalized: dict[str, Any] = {
        "city": city,
        "state": state,
        "country": country,
    }
    if aliases:
        normalized["aliases"] = aliases
    if entry.get("remoteOnly"):
        normalized["remoteOnly"] = True
    population = entry.get("population")
    if isinstance(population, int) and population > 0:
        normalized["population"] = population
    return normalized


def _merge_entries(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = dict(existing)
    if incoming.get("population"):
        existing_pop = merged.get("population")
        incoming_pop = incoming["population"]
        if not isinstance(existing_pop, int) or incoming_pop > existing_pop:
            merged["population"] = incoming_pop
    aliases = set(merged.get("aliases") or [])
    aliases.update(incoming.get("aliases") or [])
    if aliases:
        merged["aliases"] = sorted(aliases, key=str.lower)
    if incoming.get("remoteOnly"):
        merged["remoteOnly"] = True
    return merged


def _add_entry(container: dict[str, list[dict[str, Any]]], entry: dict[str, Any]) -> None:
    city = entry["city"]
    bucket = container.setdefault(city, [])
    for idx, existing in enumerate(bucket):
        if (
            existing.get("city") == entry.get("city")
            and existing.get("state") == entry.get("state")
            and existing.get("country") == entry.get("country")
        ):
            bucket[idx] = _merge_entries(existing, entry)
            return
    bucket.append(entry)


def _flatten_entries(values: Iterable[list[dict[str, Any]]]) -> list[dict[str, Any]]:
    return [item for group in values for item in group]


def build_dictionary() -> dict[str, list[dict[str, Any]]]:
    existing_entries = [_normalize_entry(entry) for entry in _load_existing_entries()]

    country_info = _parse_country_info(_fetch_bytes(COUNTRY_INFO_URL).decode("utf-8", errors="ignore"))
    city_rows = _parse_city_rows(_fetch_bytes(CITIES_URL))
    city_rows.sort(key=lambda row: row.population, reverse=True)
    top_rows = city_rows[:TOP_N]

    dictionary: dict[str, list[dict[str, Any]]] = {}
    for entry in existing_entries:
        _add_entry(dictionary, entry)

    for row in top_rows:
        country = country_info.get(row.country_code)
        if not country:
            continue
        if row.country_code == "US":
            state = STATE_NAME_BY_ABBR.get(row.admin1) or country
        else:
            state = country
        normalized = _normalize_entry(
            {
                "city": row.name,
                "state": state,
                "country": country,
                "population": row.population,
            }
        )
        _add_entry(dictionary, normalized)

    ordered = {key: dictionary[key] for key in sorted(dictionary.keys(), key=str.lower)}
    return ordered


def main() -> int:
    dictionary = build_dictionary()
    total_entries = len(_flatten_entries(dictionary.values()))
    if total_entries < TOP_N:
        print(f"warning: only {total_entries} entries found", file=sys.stderr)
    payload = json.dumps(dictionary, indent=2, ensure_ascii=True, sort_keys=False)
    DICTIONARY_PATH.write_text(f"{payload}\n", "utf-8")
    print(f"wrote {total_entries} entries across {len(dictionary)} cities to {DICTIONARY_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
