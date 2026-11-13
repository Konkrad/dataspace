"""``collections.json`` for the STAC Browser on the landing page.

The browser answers a STAC API itself (DuckDB-WASM over the monthly files,
see the stac-browser fork's ``src/local-api``). This writes what it needs to
know up front: CDSE's STAC collections that occur in the files, each with its
STAC collection metadata (title, description, ... from CDSE's own STAC API,
kept in ``cdse_collections.json``), its time extent and item count, and
``cdse:files``: which folders and months hold its items.

It reads only the ``collection`` and ``datetime`` columns of every file, from
R2 itself when the R2 credentials are set (Cloudflare's cache can still serve
an older version of a file), else from the public URLs.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request
from pathlib import Path

import duckdb

from . import stac

log = logging.getLogger("cdsemeta.stac_api")

STAC_VERSION = "1.1.0"
CDSE_STAC = "https://stac.dataspace.copernicus.eu/v1"
CDSE_COLLECTIONS = Path(__file__).with_name("cdse_collections.json")
# Fields of CDSE's collections copied into ours.
KEEP = ("title", "description", "keywords", "license", "providers", "summaries", "sci:citation")


# Requests with this header skip Cloudflare's cache (a cache rule on the zone,
# see CONTRIBUTING.md): a month rewritten minutes ago is still the old version
# there for up to a day, and reading it mixes versions (HTTP 416, bad bytes).
FRESH_HEADER = "x-dataspace-fresh"


def read_fresh(con: duckdb.DuckDBPyConnection, base: str) -> None:
    """Send ``FRESH_HEADER`` on this connection's requests under ``base`` (a
    temporary secret: it isn't saved in a database file)."""
    if not base.startswith(("http://", "https://")):
        return
    con.execute("INSTALL httpfs; LOAD httpfs")
    scope = "'" + base.rstrip("/").replace("'", "''") + "'"
    con.execute(f"CREATE TEMPORARY SECRET fresh (TYPE http, SCOPE {scope}, "
                f"EXTRA_HTTP_HEADERS MAP {{'{FRESH_HEADER}': '1'}})")


def connect_reader() -> tuple[duckdb.DuckDBPyConnection, str | None]:
    """A DuckDB connection and the s3:// prefix of the bucket, if R2 credentials are set."""
    con = duckdb.connect()
    con.execute("SET threads = 8")
    con.execute("SET enable_progress_bar = false")
    con.execute("SET TimeZone = 'UTC'")
    need = ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET")
    if not all(os.environ.get(k) for k in need):
        return con, None
    endpoint = os.environ.get("R2_ENDPOINT_URL") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    con.execute("INSTALL httpfs; LOAD httpfs")
    q = lambda s: "'" + s.replace("'", "''") + "'"  # noqa: E731
    con.execute(f"""CREATE SECRET r2 (TYPE s3, KEY_ID {q(os.environ['R2_ACCESS_KEY_ID'])},
        SECRET {q(os.environ['R2_SECRET_ACCESS_KEY'])}, REGION 'auto', URL_STYLE 'path',
        ENDPOINT {q(endpoint.removeprefix('https://').rstrip('/'))})""")
    return con, f"s3://{os.environ['R2_BUCKET']}"


def scan(keys: list[str], public_base: str) -> list[tuple]:
    """(folder, month, collection, items, first datetime, last datetime) for every file key."""
    con, s3 = connect_reader()
    base = s3 or public_base.rstrip("/")
    read_fresh(con, base)
    out = []
    def query(batch: list[str]) -> list[tuple]:
        files = ", ".join("'" + f"{base}/{k}".replace("'", "''") + "'" for k in batch)
        return con.execute(f"""
            SELECT filename, collection, count(*),
                   strftime(CAST(min(datetime) AS TIMESTAMP), '%Y-%m-%dT%H:%M:%S.%fZ'),
                   strftime(CAST(max(datetime) AS TIMESTAMP), '%Y-%m-%dT%H:%M:%S.%fZ')
            FROM read_parquet([{files}], filename = true, union_by_name = true)
            GROUP BY ALL""").fetchall()

    for i in range(0, len(keys), 50):  # in batches: keeps each query's file list short
        batch = keys[i:i + 50]
        try:
            rows = query(batch)
        except duckdb.BinderException:  # none of them STAC yet (no collection column): file by file
            rows = []
            for key in batch:
                try:
                    rows += query([key])
                except duckdb.BinderException:
                    log.warning("%s is not STAC GeoParquet (no collection column), left out", key)
        for filename, coll, n, tmin, tmax in rows:
            key = filename[len(base) + 1:]
            folder, month = key.split("/")
            out.append((folder.removesuffix(".parquet"), month.removesuffix(".parquet"), coll, n, tmin, tmax))
        log.info("scanned %d/%d files", min(i + 50, len(keys)), len(keys))
    return out


def collections_json(rows: list[tuple], data_base: str, cdse: dict) -> dict:
    by_coll: dict[str, dict] = {}
    for folder, month, coll, n, tmin, tmax in rows:
        c = by_coll.setdefault(coll, {"files": {}, "n": 0, "min": tmin, "max": tmax})
        c["files"].setdefault(folder, []).append(month)
        c["n"] += n
        c["min"] = min(c["min"], tmin) if tmin and c["min"] else (c["min"] or tmin)
        c["max"] = max(c["max"], tmax) if tmax and c["max"] else (c["max"] or tmax)
    collections = []
    for cid in sorted(by_coll):
        c = by_coll[cid]
        out = {"type": "Collection", "stac_version": STAC_VERSION, "id": cid}
        if cid in stac.CDSE_COLLECTIONS:
            meta = cdse.get(cid) or {"title": cid, "description": f"CDSE's STAC collection {cid}."}
            out.update({k: meta[k] for k in KEEP if k in meta})
            out["links"] = [{"rel": "via", "href": f"{CDSE_STAC}/collections/{cid}", "type": "application/json",
                             "title": "This collection in CDSE's STAC API"}]
        else:  # product types CDSE has no STAC collection for: our own folder id
            mission, kind = cid.rsplit("-", 1)
            out["title"] = f"{mission.replace('sentinel', 'Sentinel')} {kind.upper()}"
            out["description"] = (f"{out['title']} products from the CDSE catalogue. CDSE's STAC API has no "
                                  f"collection for these product types, so this one is named after the folder.")
            out["license"] = "other"
        out["extent"] = {"spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
                         "temporal": {"interval": [[c["min"], c["max"]]]}}
        out["cdse:items"] = c["n"]
        out["cdse:files"] = {f: sorted(set(m)) for f, m in sorted(c["files"].items())}
        collections.append(out)
    return {
        "id": "cdse-catalogue",
        "title": "Copernicus Data Space catalogue",
        "description": (
            "The product metadata of every Sentinel-1, -2, -3 and -5P product in the "
            "[Copernicus Data Space Ecosystem](https://dataspace.copernicus.eu/), with the item ids, collections "
            "and properties of CDSE's own STAC API, as STAC GeoParquet. Updated daily.\n\n"
            f"Search runs in your browser, on the files themselves. Download them or query them with DuckDB: "
            f"[files and how to use them]({data_base.rstrip('/')}/about/), "
            "[documentation](https://github.com/Konkrad/dataspace#readme)."),
        "data_base": data_base,
        "collections": collections,
    }


def fetch_cdse(ids: list[str], out: Path = CDSE_COLLECTIONS) -> dict:
    """Refresh ``cdse_collections.json`` from CDSE's STAC API (rate-limited, so slowly)."""
    have = json.loads(out.read_text()) if out.exists() else {}
    for cid in ids:
        if cid in have:
            continue
        for attempt in range(6):
            try:
                with urllib.request.urlopen(f"{CDSE_STAC}/collections/{cid}", timeout=60) as r:
                    meta = json.load(r)
                have[cid] = {k: meta[k] for k in KEEP if k in meta}
                log.info("fetched %s", cid)
                break
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    log.info("%s: not in CDSE's STAC API", cid)
                    break
                if e.code != 429:
                    raise
                time.sleep(int(e.headers.get("Retry-After") or 10) + 2)
        time.sleep(3)
    out.write_text(json.dumps(dict(sorted(have.items())), indent=1, ensure_ascii=False) + "\n")
    return have
