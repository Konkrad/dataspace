"""Write the public index of the R2 bucket: browsable HTML pages, a plain file
list for download tools, and a DuckDB catalogue with one view per collection.

R2 has no directory listing, so this lists the bucket over the S3 API and
uploads, next to the monthly files (``<mission>-<collection>.parquet/<YYYY-MM>.parquet``):

- ``index.html``: every collection, with links to its folder, and how to use
  the data. A Cloudflare rewrite rule serves it for ``/``.
- ``<mission>-<collection>.parquet/index.html``: the months of one collection.
  Served for the folder URL by the same rule, so ``rclone`` (http backend) and
  ``wget -r`` can mirror the bucket without a key.
- ``files.txt``: one URL per line, for ``wget -i`` / ``aria2c -i``.
- ``files.tsv``: URL, size, ETag and last-modified per file, for incremental
  mirroring.
- ``dataspace.duckdb``: ``ATTACH '<base>/dataspace.duckdb' AS ds (READ_ONLY)``
  and query ``ds.sentinel_1_grd`` etc. Views over the HTTPS URLs, plus the
  tables ``datasets`` and ``files``.

Run after every merge (all missions), e.g. as the last job of the workflow.
"""

from __future__ import annotations

import argparse
import html
import json
import logging
import os
import mimetypes
import re
import shutil
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from . import cfcache, r2, stac_api

log = logging.getLogger("cdsemeta.index")

DEFAULT_BASE_URL = "https://dataspace.konrad.online"
DB_NAME = "dataspace.duckdb"
KEY_RE = re.compile(r"^(?P<ds>[^/]+)\.parquet/(?P<month>\d{4}-\d{2})\.parquet$")
# Short cache for the generated files, so a new month shows up soon after the run.
INDEX_CACHE = "public, max-age=300"
# The viewer's files have content hashes in their names.
ASSET_CACHE = "public, max-age=31536000, immutable"
VIEWER_PREFIX = "_viewer/"
ABOUT = "about/index.html"
COLLECTIONS_JSON = "collections.json"
# Files of the STAC Browser build that aren't needed on the bucket.
BROWSER_SKIP = (".map", ".htaccess", "mitm.html", "mockServiceWorker.js")
VIEWER_ENTRY = "src/main.js"


@dataclass
class File:
    key: str
    month: str
    size: int
    etag: str
    modified: datetime


@dataclass
class Dataset:
    id: str  # "sentinel-1-grd", the folder is "<id>.parquet/"
    files: list[File] = field(default_factory=list)

    @property
    def mission(self) -> str:
        return self.id.rsplit("-", 1)[0]

    @property
    def collection(self) -> str:
        return self.id.rsplit("-", 1)[1]

    @property
    def folder(self) -> str:
        return f"{self.id}.parquet/"

    @property
    def view(self) -> str:
        return self.id.replace("-", "_")

    @property
    def size(self) -> int:
        return sum(f.size for f in self.files)


def datasets_from_objects(objects: list[dict]) -> list[Dataset]:
    """Group the monthly files of a bucket listing by collection; ignore everything else."""
    by_id: dict[str, Dataset] = {}
    for o in objects:
        m = KEY_RE.match(o["Key"])
        if not m:
            continue
        mod = o["LastModified"]
        if mod.tzinfo is None:
            mod = mod.replace(tzinfo=timezone.utc)
        by_id.setdefault(m["ds"], Dataset(m["ds"])).files.append(
            File(o["Key"], m["month"], int(o["Size"]), str(o.get("ETag", "")).strip('"'), mod))
    for d in by_id.values():
        d.files.sort(key=lambda f: f.month)
    return [by_id[k] for k in sorted(by_id)]


def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def url(base: str, key: str) -> str:
    return f"{base.rstrip('/')}/{key}"


def files_txt(datasets: list[Dataset], base: str) -> str:
    return "".join(f"{url(base, f.key)}\n" for d in datasets for f in d.files)


def files_tsv(datasets: list[Dataset], base: str) -> str:
    rows = ["url\tsize\tetag\tlast_modified"]
    rows += [f"{url(base, f.key)}\t{f.size}\t{f.etag}\t{f.modified.isoformat()}"
             for d in datasets for f in d.files]
    return "\n".join(rows) + "\n"


def build_duckdb(datasets: list[Dataset], base: str, out: Path) -> None:
    """A small DuckDB file: a view per collection over the HTTPS URLs, plus listings.

    Creating a view binds it, which reads the files' schemas, so ``base`` must be
    reachable from here (it is the public URL in production, a folder in tests).
    """
    out.unlink(missing_ok=True)
    con = duckdb.connect(str(out))
    try:
        stac_api.read_fresh(con, base)
        # Few parallel requests and patient retries: the public domain may rate-limit (429).
        con.execute("SET threads = 4; SET http_retries = 8; SET http_retry_wait_ms = 1000; SET http_retry_backoff = 2")
        con.execute("""CREATE TABLE datasets (name VARCHAR, mission VARCHAR, collection VARCHAR,
                        first_month VARCHAR, last_month VARCHAR, files INTEGER, bytes BIGINT, folder VARCHAR)""")
        con.execute("""CREATE TABLE files (dataset VARCHAR, month VARCHAR, url VARCHAR, bytes BIGINT,
                        etag VARCHAR, last_modified TIMESTAMPTZ)""")
        for d in datasets:
            con.execute("INSERT INTO datasets VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        [d.view, d.mission, d.collection, d.files[0].month, d.files[-1].month,
                         len(d.files), d.size, url(base, d.folder)])
            con.executemany("INSERT INTO files VALUES (?, ?, ?, ?, ?, ?)",
                            [[d.view, f.month, url(base, f.key), f.size, f.etag, f.modified] for f in d.files])
            urls = ", ".join("'" + url(base, f.key).replace("'", "''") + "'" for f in d.files)
            log.info("view %s over %d files", d.view, len(d.files))
            # union_by_name: newer months can carry OData attributes older ones don't.
            con.execute(f"CREATE VIEW {d.view} AS SELECT * FROM read_parquet([{urls}], union_by_name = true)")
        con.execute("CHECKPOINT")
    finally:
        con.close()


_STYLE = """
:root { --bg: #fff; --fg: #1a1a1a; --muted: #5f6672; --line: #dde0e5; --code: #f3f4f6; --accent: #0b5fff; }
@media (prefers-color-scheme: dark) {
  :root { --bg: #14161a; --fg: #e8e9ec; --muted: #9aa1ad; --line: #2e323a; --code: #1d2026; --accent: #6b9bff; }
}
body { margin: 0 auto; max-width: 960px; padding: 24px 16px 48px; background: var(--bg); color: var(--fg);
       font: 15px/1.5 system-ui, sans-serif; }
h1 { font-size: 1.5rem; margin: 0 0 4px; } h2 { font-size: 1.1rem; margin: 32px 0 8px; }
p { margin: 8px 0; } .muted { color: var(--muted); }
a { color: var(--accent); }
table { width: 100%; border-collapse: collapse; font-variant-numeric: tabular-nums; }
th, td { text-align: left; padding: 6px 8px; border-bottom: 1px solid var(--line); white-space: nowrap; }
td.num, th.num { text-align: right; }
.scroll { overflow-x: auto; }
pre { background: var(--code); padding: 12px; border-radius: 6px; overflow-x: auto; font-size: 13px; }
code { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
"""


def _page(title: str, body: str) -> str:
    return (f'<!doctype html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n'
            f'<meta name="viewport" content="width=device-width, initial-scale=1">\n'
            f"<title>{html.escape(title)}</title>\n<style>{_STYLE}</style>\n</head>\n<body>\n{body}\n</body>\n</html>\n")


def viewer_html(datasets: list[Dataset], base: str, entry: dict) -> str:
    """The map: controls, the map element, the collection list and the bundle (viewer/)."""
    e = html.escape
    data = json.dumps({"base": base.rstrip("/"),
                       "datasets": [{"id": d.id, "months": [f.month for f in d.files]} for d in datasets]})
    data = data.replace("</", "<\\/")  # can't end the <script> early
    css = "".join(f'<link rel="stylesheet" href="/{VIEWER_PREFIX}{e(c)}">' for c in entry.get("css", []))
    return f"""{css}
<section class="viewer">
<div class="controls">
  <label>Collection <select id="collection"></select></label>
  <label>Month
    <button id="prev" title="Previous month" aria-label="Previous month">‹</button>
    <input id="month" type="month">
    <button id="next" title="Next month" aria-label="Next month">›</button>
  </label>
  <a id="file">parquet file</a>
  <span id="status"></span>
</div>
<div id="map"></div>
<p class="muted">Footprints of one collection and sensing month; click one for its products. Zoomed out, only a
thinned-out overview of the file is loaded.</p>
</section>
<script id="datasets" type="application/json">{data}</script>
<script type="module" src="/{VIEWER_PREFIX}{e(entry["file"])}"></script>"""


def root_html(datasets: list[Dataset], base: str, generated: datetime, viewer_entry: dict | None = None) -> str:
    e = html.escape
    base = base.rstrip("/")
    rows = "\n".join(
        f'<tr><td><a href="/{e(d.folder)}">{e(d.folder)}</a></td><td><code>{e(d.view)}</code></td>'
        f'<td>{e(d.files[0].month)} – {e(d.files[-1].month)}</td>'
        f'<td class="num">{len(d.files)}</td><td class="num">{human(d.size)}</td></tr>'
        for d in datasets)
    total = sum(d.size for d in datasets)
    nfiles = sum(len(d.files) for d in datasets)
    first = datasets[0] if datasets else None
    ex_view = first.view if first else "sentinel_1_grd"
    ex_key = first.files[-1].key if first else "sentinel-1-grd.parquet/2024-05.parquet"
    ex_folder = first.folder if first else "sentinel-1-grd.parquet/"
    body = f"""<h1>Copernicus Data Space catalogue</h1>
<p class="muted">Product metadata of the Sentinel missions from the
<a href="https://dataspace.copernicus.eu/">Copernicus Data Space Ecosystem</a>,
as GeoParquet, one file per collection and sensing month. Updated daily.
{nfiles} files, {human(total)}. Generated {generated:%Y-%m-%d %H:%M} UTC.
Columns, collections and more examples: <a href="https://github.com/Konkrad/dataspace#readme">documentation</a>.</p>
<p><a href="/">Search and browse the catalogue</a> (STAC Browser, in your browser).</p>
{viewer_html(datasets, base, viewer_entry) if viewer_entry else ""}

<h2>Collections</h2>
<div class="scroll"><table>
<thead><tr><th>Folder</th><th>DuckDB view</th><th>Months</th><th class="num">Files</th><th class="num">Size</th></tr></thead>
<tbody>
{rows}
</tbody>
</table></div>
<p>Each file is <code>{e(base)}/&lt;mission&gt;-&lt;collection&gt;.parquet/&lt;YYYY-MM&gt;.parquet</code>,
by the sensing month (<code>datetime</code>). Downloads: <a href="/files.txt">files.txt</a> (one URL per line),
<a href="/files.tsv">files.tsv</a> (with size, ETag and last-modified), <a href="/{DB_NAME}">{DB_NAME}</a>,
<a href="/{COLLECTIONS_JSON}">{COLLECTIONS_JSON}</a> (the STAC collections and their files).</p>

<h2>Query with DuckDB</h2>
<p>Attach the catalogue and every collection is a view; only the parts of the files a query needs are fetched.</p>
<pre><code>ATTACH '{e(base)}/{DB_NAME}' AS ds (READ_ONLY);
SHOW ALL TABLES;
FROM ds.datasets;

SELECT id, collection, datetime, assets.product.href
FROM ds.{e(ex_view)}
WHERE datetime &gt;= '2024-05-01' AND datetime &lt; '2024-06-01'
  AND bbox.xmin &lt; 13.8 AND bbox.xmax &gt; 13.0 AND bbox.ymin &lt; 52.7 AND bbox.ymax &gt; 52.3;</code></pre>
<p>Or read single months directly:</p>
<pre><code>SELECT count(*) FROM '{e(base)}/{e(ex_key)}';</code></pre>

<h2>Python</h2>
<pre><code>import duckdb
con = duckdb.connect()
con.sql("ATTACH '{e(base)}/{DB_NAME}' AS ds (READ_ONLY)")
df = con.sql("FROM ds.{e(ex_view)} WHERE datetime &gt;= '2024-05-01' LIMIT 1000").df()

import geopandas as gpd
gdf = gpd.read_parquet("{e(base)}/{e(ex_key)}")</code></pre>

<h2>QGIS / GDAL</h2>
<pre><code>ogrinfo -so /vsicurl/{e(base)}/{e(ex_key)}</code></pre>
<p>In QGIS: Layer → Add Vector Layer → Protocol: HTTP(S), with the file URL.</p>

<h2>Download</h2>
<p>Everything, or one collection, with <a href="https://rclone.org/">rclone</a>. Repeat runs fetch only what changed:</p>
<pre><code>rclone sync --http-url {e(base)} :http: ./dataspace
rclone sync --http-url {e(base)}/{e(ex_folder)} :http: ./dataspace/{e(ex_folder)}</code></pre>
<p>Or from the file list:</p>
<pre><code>wget -x -nH -N -i {e(base)}/files.txt
aria2c -x 4 -j 4 --auto-file-renaming=false -i {e(base)}/files.txt</code></pre>
"""
    return _page("Copernicus Data Space catalogue", body)


def folder_html(d: Dataset, base: str, generated: datetime) -> str:
    e = html.escape
    rows = "\n".join(
        f'<tr><td><a href="{e(f.month)}.parquet">{e(f.month)}.parquet</a></td>'
        f'<td class="num">{human(f.size)}</td><td>{f.modified:%Y-%m-%d %H:%M}</td></tr>'
        for f in reversed(d.files))
    body = f"""<p><a href="/about/">← all collections</a></p>
<h1>{e(d.folder)}</h1>
<p class="muted">{len(d.files)} files, {human(d.size)}. DuckDB view <code>ds.{e(d.view)}</code>.
Generated {generated:%Y-%m-%d %H:%M} UTC.</p>
<div class="scroll"><table>
<thead><tr><th>File</th><th class="num">Size</th><th>Last modified (UTC)</th></tr></thead>
<tbody>
{rows}
</tbody>
</table></div>
"""
    return _page(d.folder, body)


def content_type(path: Path) -> str:
    ctype = {".js": "text/javascript", ".mjs": "text/javascript", ".css": "text/css", ".wasm": "application/wasm",
             ".json": "application/json", ".map": "application/json"}.get(path.suffix)
    ctype = ctype or mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    return f"{ctype}; charset=utf-8" if ctype.startswith("text/") or ctype == "application/json" else ctype


def copy_viewer(viewer_dist: Path, out_dir: Path) -> tuple[dict, list[tuple[Path, str, str, str]]]:
    """Copy the built viewer (viewer/dist) to out_dir/_viewer/; return its entry and the files."""
    manifest = json.loads((viewer_dist / ".vite" / "manifest.json").read_text())
    entry = manifest[VIEWER_ENTRY]
    items = []
    for src in sorted(viewer_dist.rglob("*")):
        rel = src.relative_to(viewer_dist)
        if src.is_dir() or rel.parts[0] == ".vite":
            continue
        dst = out_dir / VIEWER_PREFIX / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dst)
        items.append((dst, VIEWER_PREFIX + rel.as_posix(), content_type(src), ASSET_CACHE))
    return entry, items


def browser_index(page: str, datasets: list[Dataset]) -> str:
    """STAC Browser's index.html, plus a hidden list of links in <body> (STAC Browser replaces
    the body when it starts): rclone's http backend and `wget -r` mirror the bucket from it."""
    e = html.escape
    links = [f'<a href="{e(d.folder)}">{e(d.folder)}</a>' for d in datasets]
    links += [f'<a href="{n}">{n}</a>' for n in ("files.txt", "files.tsv", DB_NAME, COLLECTIONS_JSON, "about/")]
    nav = '<nav hidden aria-hidden="true">\n' + "\n".join(links) + "\n</nav>\n"
    return page.replace("</body>", nav + "</body>", 1)


def copy_browser(browser_dist: Path, out_dir: Path, datasets: list[Dataset]) -> list[tuple[Path, str, str, str]]:
    """The STAC Browser build (fork with src/local-api) at the bucket root."""
    items = []
    for src in sorted(browser_dist.rglob("*")):
        rel = src.relative_to(browser_dist).as_posix()
        if src.is_dir() or rel.endswith(BROWSER_SKIP):
            continue
        dst = out_dir / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        if rel == "index.html":
            dst.write_text(browser_index(src.read_text(encoding="utf-8"), datasets), encoding="utf-8")
        else:
            shutil.copyfile(src, dst)
        # assets/ has content hashes in its names; the rest (index.html, sw.js, ...) can change
        items.append((dst, rel, content_type(src), ASSET_CACHE if rel.startswith("assets/") else INDEX_CACHE))
    return items


def write_all(datasets: list[Dataset], base: str, out_dir: Path, *, duckdb_file: bool = True,
              viewer_dist: Path | None = None, browser_dist: Path | None = None,
              collections: dict | None = None) -> list[tuple[Path, str, str, str]]:
    """Write every index file under out_dir; return (path, key, content type, cache control) to upload."""
    now = datetime.now(timezone.utc)
    out_dir.mkdir(parents=True, exist_ok=True)
    items: list[tuple[Path, str, str, str]] = []

    def put(key: str, text: str, ctype: str) -> None:
        p = out_dir / key
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")
        items.append((p, key, ctype, INDEX_CACHE))

    entry = None
    if viewer_dist:
        entry, assets = copy_viewer(viewer_dist, out_dir)
        items.extend(assets)
    if browser_dist:
        # STAC Browser at the root; the files and how to use them under /about/
        items.extend(copy_browser(browser_dist, out_dir, datasets))
        put(ABOUT, root_html(datasets, base, now, None), "text/html; charset=utf-8")
    else:
        put("index.html", root_html(datasets, base, now, entry), "text/html; charset=utf-8")
    if collections is not None:
        put(COLLECTIONS_JSON, json.dumps(collections, ensure_ascii=False), "application/json; charset=utf-8")
    for d in datasets:
        put(f"{d.folder}index.html", folder_html(d, base, now), "text/html; charset=utf-8")
    put("files.txt", files_txt(datasets, base), "text/plain; charset=utf-8")
    put("files.tsv", files_tsv(datasets, base), "text/tab-separated-values; charset=utf-8")
    if duckdb_file:
        db = out_dir / DB_NAME
        build_duckdb(datasets, base, db)
        items.append((db, DB_NAME, "application/octet-stream", INDEX_CACHE))
    return items


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base-url", default=os.environ.get("PUBLIC_BASE_URL", DEFAULT_BASE_URL),
                    help=f"public URL of the bucket (default: $PUBLIC_BASE_URL or {DEFAULT_BASE_URL})")
    ap.add_argument("--out-dir", type=Path, default=Path("index"), help="where the files are written")
    ap.add_argument("--no-duckdb", action="store_true", help=f"skip {DB_NAME} (it reads every file's schema)")
    ap.add_argument("--viewer", type=Path, default=None, metavar="DIST",
                    help="built map viewer (viewer/dist, `npm run build`) to put on the landing page")
    ap.add_argument("--browser", type=Path, default=None, metavar="DIST",
                    help="built STAC Browser (fork with src/local-api) to put at the root; the landing page "
                         "moves to /about/")
    ap.add_argument("--no-collections", action="store_true",
                    help=f"skip {COLLECTIONS_JSON} (it reads the collection column of every file)")
    ap.add_argument("--upload-r2", action="store_true", help="upload the files to R2 ($R2_BUCKET)")
    args = ap.parse_args(argv)
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    datasets = datasets_from_objects(r2.list_objects())
    if not datasets:
        log.error("no <dataset>.parquet/<YYYY-MM>.parquet files in the bucket")
        return 1
    log.info("%d collections, %d files", len(datasets), sum(len(d.files) for d in datasets))
    collections = None
    if not args.no_collections:
        rows = stac_api.scan([f.key for d in datasets for f in d.files], args.base_url)
        cdse = json.loads(stac_api.CDSE_COLLECTIONS.read_text()) if stac_api.CDSE_COLLECTIONS.exists() else {}
        collections = stac_api.collections_json([r for r in rows if r[2]], args.base_url, cdse)
        log.info("%d STAC collections", len(collections["collections"]))
    items = write_all(datasets, args.base_url, args.out_dir, duckdb_file=not args.no_duckdb,
                      viewer_dist=args.viewer, browser_dist=args.browser, collections=collections)
    if args.upload_r2:
        # Assets, folder pages and lists first, the root page last: it refers to all of them.
        for path, key, ctype, cache in sorted(items, key=lambda i: i[1] == "index.html"):
            r2.upload(path, key, content_type=ctype, cache_control=cache)
        # The cache rule keeps everything a day, whatever Cache-Control says.
        # Pages are cached under their folder URL too (/, /about/), via the index rewrite rule.
        keys = [key for _, key, _, _ in items]
        keys += [k.removesuffix("index.html") for k in keys if k == "index.html" or k.endswith("/index.html")]
        cfcache.purge(keys, args.base_url)
    log.info("wrote %d files to %s%s", len(items), args.out_dir, " and uploaded them" if args.upload_r2 else "")
    return 0


if __name__ == "__main__":
    sys.exit(main())
