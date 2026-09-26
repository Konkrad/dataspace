"""Turn OData products (plus the source CSV) into an Arrow table.

The schema is built explicitly so that every file types its columns the same
way, which keeps the per-day files easy to merge later.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import pyarrow as pa
import shapely
from shapely.geometry import shape

log = logging.getLogger(__name__)

TS = pa.timestamp("us", tz="UTC")

# Fixed columns, in output order. Attribute columns follow, sorted by name.
FIXED_SCHEMA = pa.schema([
    ("id", pa.string()),
    ("name", pa.string()),
    ("odata_found", pa.bool_()),
    ("content_start", TS),
    ("content_end", TS),
    ("content_type", pa.string()),
    ("content_length", pa.int64()),
    ("ingestion_date", TS),
    ("origin_date", TS),
    ("publication_date", TS),
    ("modification_date", TS),
    ("eviction_date", TS),
    ("online", pa.bool_()),
    ("s3_path", pa.string()),
    ("checksum_md5", pa.string()),
    ("checksum_blake3", pa.string()),
    ("quicklook_url", pa.string()),
    ("checksum_json", pa.string()),
    ("attributes_json", pa.string()),
    ("assets_json", pa.string()),
    ("locations_json", pa.string()),
])

ATTR_TYPES = {
    "String": pa.string(),
    "Integer": pa.int64(),
    "Double": pa.float64(),
    "DateTimeOffset": TS,
    "Boolean": pa.bool_(),
}


def parse_ts(value: Any) -> datetime | None:
    """Parse ISO-8601 timestamps; values without an offset are taken as UTC."""
    if value is None or value == "" or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    # fromisoformat accepts at most 6 fractional digits
    if "." in s:
        head, _, rest = s.partition(".")
        digits = len(rest) - len(rest.lstrip("0123456789"))
        if digits > 6:
            s = head + "." + rest[:6] + rest[digits:]
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_footprint(product: dict) -> shapely.Geometry | None:
    gj = product.get("GeoFootprint")
    if gj:
        try:
            return shape(gj)
        except Exception:  # noqa: BLE001 - fall through to WKT
            log.debug("bad GeoFootprint for %s", product.get("Id"))
    fp = product.get("Footprint")
    if fp:
        wkt = fp
        if wkt.startswith("geography'"):
            wkt = wkt[len("geography'"):].rstrip("'")
        if ";" in wkt:
            wkt = wkt.split(";", 1)[1]
        try:
            return shapely.from_wkt(wkt)
        except Exception:  # noqa: BLE001
            log.warning("unparseable footprint for %s", product.get("Id"))
    return None


def _wkt_or_none(wkt: Any) -> shapely.Geometry | None:
    if not isinstance(wkt, str) or not wkt.strip():
        return None
    try:
        return shapely.from_wkt(wkt)
    except Exception:  # noqa: BLE001
        return None


def _int_or_none(v: Any) -> int | None:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _csv_get(row: dict, key: str) -> Any:
    v = row.get(key)
    return None if v is None or (isinstance(v, float) and pd.isna(v)) else v


def _checksum(checksums: list[dict], algorithm: str) -> str | None:
    for c in checksums or []:
        if str(c.get("Algorithm", "")).upper() == algorithm:
            return c.get("Value")
    return None


def _dumps(v: Any) -> str | None:
    return None if v is None else json.dumps(v, separators=(",", ":"), ensure_ascii=False)


def s3_uri(path: Any) -> str | None:
    """Normalize an S3Path to a full ``s3://`` URI.

    OData's ``S3Path`` omits the scheme (``/eodata/...``); the CSV's
    ``S3Path`` already includes it (``s3://eodata/...``). Both end up in the
    same column, so callers should not have to special-case either source.
    """
    if not path:
        return None
    if path.startswith("s3://"):
        return path
    return "s3:/" + path if path.startswith("/") else "s3://" + path


def _attr_value(a: dict, typ: pa.DataType) -> Any:
    v = a.get("Value")
    if v is None:
        return None
    if typ == TS:
        return parse_ts(v)
    if typ == pa.int64():
        return int(v)
    if typ == pa.float64():
        return float(v)
    if typ == pa.bool_():
        return v if isinstance(v, bool) else str(v).lower() == "true"
    return str(v)


def from_odata(product: dict, csv_row: dict) -> tuple[dict, dict, shapely.Geometry | None]:
    checksums = product.get("Checksum") or []
    content = product.get("ContentDate") or {}
    quicklook = next((a.get("DownloadLink") for a in product.get("Assets") or [] if a.get("Type") == "QUICKLOOK"), None)
    rec = {
        "id": product["Id"],
        "name": product.get("Name"),
        "odata_found": True,
        "content_start": parse_ts(content.get("Start")),
        "content_end": parse_ts(content.get("End")),
        "content_type": product.get("ContentType"),
        "content_length": _int_or_none(product.get("ContentLength")),
        "ingestion_date": parse_ts(_csv_get(csv_row, "IngestionDate")),
        "origin_date": parse_ts(product.get("OriginDate")),
        "publication_date": parse_ts(product.get("PublicationDate")),
        "modification_date": parse_ts(product.get("ModificationDate")),
        "eviction_date": parse_ts(product.get("EvictionDate")),
        "online": product.get("Online"),
        "s3_path": s3_uri(product.get("S3Path")),
        "checksum_md5": _checksum(checksums, "MD5"),
        "checksum_blake3": _checksum(checksums, "BLAKE3"),
        "quicklook_url": quicklook,
        "checksum_json": _dumps(checksums),
        "attributes_json": _dumps(product.get("Attributes")),
        "assets_json": _dumps(product.get("Assets")),
        "locations_json": _dumps(product.get("Locations")),
    }
    attrs = {a["Name"]: a for a in product.get("Attributes") or [] if a.get("Name")}
    geom = parse_footprint(product)
    if geom is None:
        geom = _wkt_or_none(_csv_get(csv_row, "Bbox"))
    return rec, attrs, geom


def from_csv_only(csv_row: dict) -> tuple[dict, dict, shapely.Geometry | None]:
    """Row for a product that the CSV lists but OData does not return."""
    algo = str(_csv_get(csv_row, "Checksum:Algorithm") or "").upper()
    value = _csv_get(csv_row, "Checksum:Value")
    rec = {
        "id": csv_row["Id"],
        "name": _csv_get(csv_row, "Name"),
        "odata_found": False,
        "content_start": parse_ts(_csv_get(csv_row, "ContentDate:Start")),
        "content_end": parse_ts(_csv_get(csv_row, "ContentDate:End")),
        "content_length": _int_or_none(_csv_get(csv_row, "ContentLength")),
        "ingestion_date": parse_ts(_csv_get(csv_row, "IngestionDate")),
        "modification_date": parse_ts(_csv_get(csv_row, "ModificationDate")),
        "s3_path": s3_uri(_csv_get(csv_row, "S3Path")),
        "checksum_md5": value if algo == "MD5" else None,
        "checksum_blake3": value if algo == "BLAKE3" else None,
    }
    return rec, {}, _wkt_or_none(_csv_get(csv_row, "Bbox"))


def build_table(csv_df: pd.DataFrame, products: dict[str, dict]) -> tuple[pa.Table, list, int]:
    """Return (attribute table without geometry, geometries, number of missing ids).

    Rows are sorted by ``content_start`` (then ``name``).
    """
    rows: list[tuple[dict, dict, Any]] = []
    missing = 0
    for csv_row in csv_df.to_dict("records"):
        p = products.get(csv_row["Id"])
        if p is None:
            missing += 1
            rows.append(from_csv_only(csv_row))
        else:
            rows.append(from_odata(p, csv_row))

    far_future = datetime.max.replace(tzinfo=timezone.utc)
    rows.sort(key=lambda r: (r[0].get("content_start") or far_future, r[0].get("name") or ""))

    # Work out one type per attribute name.
    attr_types: dict[str, pa.DataType] = {}
    for _, attrs, _ in rows:
        for name, a in attrs.items():
            t = ATTR_TYPES.get(a.get("ValueType"), pa.string())
            prev = attr_types.get(name)
            if prev is None:
                attr_types[name] = t
            elif prev != t:
                attr_types[name] = pa.string()

    fixed_names = set(FIXED_SCHEMA.names) | {"geometry", "bbox"}
    attr_cols = {name: (f"attr_{name}" if name in fixed_names else name) for name in sorted(attr_types)}

    columns: dict[str, pa.Array] = {}
    for f in FIXED_SCHEMA:
        columns[f.name] = pa.array([r[0].get(f.name) for r in rows], type=f.type)
    for name, col in attr_cols.items():
        t = attr_types[name]
        vals = []
        for _, attrs, _ in rows:
            a = attrs.get(name)
            try:
                vals.append(None if a is None else _attr_value(a, t))
            except (TypeError, ValueError):
                vals.append(None)
        columns[col] = pa.array(vals, type=t)

    table = pa.table(columns)
    return table, [r[2] for r in rows], missing
