#!/usr/bin/env python3
"""
Alma Lasers - Find Clinics harvester (UK-only) using embedded JSON dataset.

We extract the big JSON array (already saved as data/_debug/locations_array.txt),
parse it, and filter to UK by postcode + UK lat/lon bbox fallback.

Outputs:
- data/alma/locations.csv  (with postcodes)
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Optional

# Inputs/outputs
DEBUG_DIR = Path("data/_debug")
LOCATIONS_JSON = DEBUG_DIR / "locations_array.txt"

OUT_DIR = Path("data/alma")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# UK postcode regex (standard-ish)
UK_POSTCODE_RE = re.compile(r"\b([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})\b", re.I)

# UK rough bounding box (fallback)
UK_LAT_MIN, UK_LAT_MAX = 49.8, 60.9
UK_LON_MIN, UK_LON_MAX = -8.5, 2.5


def norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def to_float(s) -> Optional[float]:
    try:
        if s is None:
            return None
        ss = str(s).strip()
        if not ss:
            return None
        return float(ss)
    except Exception:
        return None


def in_uk_bbox(lat: Optional[float], lon: Optional[float]) -> bool:
    if lat is None or lon is None:
        return False
    return (UK_LAT_MIN <= lat <= UK_LAT_MAX) and (UK_LON_MIN <= lon <= UK_LON_MAX)


def extract_postcode(addr: str) -> str:
    m = UK_POSTCODE_RE.search(addr or "")
    return m.group(1).upper() if m else ""


def stable_id(name: str, postcode: str, lat: str, lon: str) -> str:
    key = f"{name.lower()}|{postcode.upper()}|{lat}|{lon}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]


@dataclass
class Location:
    source: str
    source_ids: str
    name: str
    postcode: str
    streetaddress: str
    loc_lat: str
    loc_long: str
    website: str
    phone: str


def pick_first(obj: dict, keys: List[str]) -> str:
    for k in keys:
        v = obj.get(k)
        if v is None:
            continue
        v = str(v).strip()
        if v:
            return v
    return ""


def parse_dataset() -> List[Location]:
    if not LOCATIONS_JSON.exists():
        raise SystemExit(f"Missing {LOCATIONS_JSON}. Recreate it from data/_debug/find_clinics.html first.")

    data = json.loads(LOCATIONS_JSON.read_text(encoding="utf-8"))

    out: List[Location] = []
    for item in data:
        if not isinstance(item, dict):
            continue

        title = norm_ws(str(item.get("title") or ""))
        m = item.get("map") or {}
        if not isinstance(m, dict):
            m = {}

        address = norm_ws(str(m.get("address") or ""))
        lat_s = str(m.get("lat") or "").strip()
        lon_s = str(m.get("lng") or "").strip()
        lat = to_float(lat_s)
        lon = to_float(lon_s)

        postcode = extract_postcode(address)

        # UK filter:
        # - primary: postcode present
        # - fallback: UK bbox (covers cases where address is missing postcode)
        if not postcode and not in_uk_bbox(lat, lon):
            continue

        # Extra guard: if bbox says UK but address clearly says another country, drop it
        if not postcode:
            addr_low = address.lower()
            # crude but effective: if they literally state another country name, reject
            for bad in ["united arab emirates", "dubai", "australia", "south africa", "ecuador", "united states", "usa", "canada"]:
                if bad in addr_low:
                    break
            else:
                pass  # no break
            # if we broke, skip
            if any(bad in addr_low for bad in ["united arab emirates", "dubai", "australia", "south africa", "ecuador", "united states", "usa", "canada"]):
                continue

        website = pick_first(item, ["website", "url", "link"])
        phone = pick_first(item, ["phone", "tel", "telephone"])

        sid = stable_id(title, postcode, lat_s, lon_s)
        out.append(Location(
            source="alma",
            source_ids=sid,
            name=title,
            postcode=postcode,
            streetaddress=address,
            loc_lat=lat_s,
            loc_long=lon_s,
            website=website,
            phone=phone,
        ))

    # de-dupe
    uniq = {}
    for r in out:
        uniq[r.source_ids] = r
    return list(uniq.values())


def write_csv(path: Path, rows: List[Location]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(Location.__dataclass_fields__.keys()))
        w.writeheader()
        for r in sorted(rows, key=lambda x: ((x.postcode or ""), (x.name or ""))):
            w.writerow(asdict(r))


def main():
    rows = parse_dataset()
    out = OUT_DIR / "locations.csv"
    write_csv(out, rows)

    with_pc = sum(1 for r in rows if r.postcode)
    print(f"WROTE: {out} rows: {len(rows)} (with_postcode={with_pc})")


if __name__ == "__main__":
    main()
