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
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from . import r2

log = logging.getLogger("cdsemeta.index")

DEFAULT_BASE_URL = "https://dataspace.konrad.online"
DB_NAME = "dataspace.duckdb"
KEY_RE = re.compile(r"^(?P<ds>[^/]+)\.parquet/(?P<month>\d{4}-\d{2})\.parquet$")
# Short cache for the generated files, so a new month shows up soon after the run.
INDEX_CACHE = "public, max-age=300"


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


def root_html(datasets: list[Dataset], base: str, generated: datetime) -> str:
    e = html.escape
    base = base.rstrip("/")
    rows = "\n".join(
        f'<tr><td><a href="{e(d.folder)}">{e(d.folder)}</a></td><td><code>{e(d.view)}</code></td>'
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

<h2>Collections</h2>
<div class="scroll"><table>
<thead><tr><th>Folder</th><th>DuckDB view</th><th>Months</th><th class="num">Files</th><th class="num">Size</th></tr></thead>
<tbody>
{rows}
</tbody>
</table></div>
<p>Each file is <code>{e(base)}/&lt;mission&gt;-&lt;collection&gt;.parquet/&lt;YYYY-MM&gt;.parquet</code>,
by the month of <code>content_start</code>. Downloads: <a href="files.txt">files.txt</a> (one URL per line),
<a href="files.tsv">files.tsv</a> (with size, ETag and last-modified), <a href="{DB_NAME}">{DB_NAME}</a>.</p>

<h2>Query with DuckDB</h2>
<p>Attach the catalogue and every collection is a view; only the parts of the files a query needs are fetched.</p>
<pre><code>ATTACH '{e(base)}/{DB_NAME}' AS ds (READ_ONLY);
SHOW ALL TABLES;
FROM ds.datasets;

SELECT name, content_start, s3_path
FROM ds.{e(ex_view)}
WHERE content_start &gt;= '2024-05-01' AND content_start &lt; '2024-06-01'
  AND bbox.xmin &lt; 13.8 AND bbox.xmax &gt; 13.0 AND bbox.ymin &lt; 52.7 AND bbox.ymax &gt; 52.3;</code></pre>
<p>Or read single months directly:</p>
<pre><code>SELECT count(*) FROM '{e(base)}/{e(ex_key)}';</code></pre>

<h2>Python</h2>
<pre><code>import duckdb
con = duckdb.connect()
con.sql("ATTACH '{e(base)}/{DB_NAME}' AS ds (READ_ONLY)")
df = con.sql("FROM ds.{e(ex_view)} WHERE content_start &gt;= '2024-05-01' LIMIT 1000").df()

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
    body = f"""<p><a href="../">← all collections</a></p>
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


def write_all(datasets: list[Dataset], base: str, out_dir: Path, *, duckdb_file: bool = True) -> list[tuple[Path, str, str]]:
    """Write every index file under out_dir; return (path, key, content type) to upload."""
    now = datetime.now(timezone.utc)
    out_dir.mkdir(parents=True, exist_ok=True)
    items: list[tuple[Path, str, str]] = []

    def put(key: str, text: str, ctype: str) -> None:
        p = out_dir / key
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")
        items.append((p, key, ctype))

    put("index.html", root_html(datasets, base, now), "text/html; charset=utf-8")
    for d in datasets:
        put(f"{d.folder}index.html", folder_html(d, base, now), "text/html; charset=utf-8")
    put("files.txt", files_txt(datasets, base), "text/plain; charset=utf-8")
    put("files.tsv", files_tsv(datasets, base), "text/tab-separated-values; charset=utf-8")
    if duckdb_file:
        db = out_dir / DB_NAME
        build_duckdb(datasets, base, db)
        items.append((db, DB_NAME, "application/octet-stream"))
    return items


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base-url", default=os.environ.get("PUBLIC_BASE_URL", DEFAULT_BASE_URL),
                    help=f"public URL of the bucket (default: $PUBLIC_BASE_URL or {DEFAULT_BASE_URL})")
    ap.add_argument("--out-dir", type=Path, default=Path("index"), help="where the files are written")
    ap.add_argument("--no-duckdb", action="store_true", help=f"skip {DB_NAME} (it reads every file's schema)")
    ap.add_argument("--upload-r2", action="store_true", help="upload the files to R2 ($R2_BUCKET)")
    args = ap.parse_args(argv)
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    datasets = datasets_from_objects(r2.list_objects())
    if not datasets:
        log.error("no <dataset>.parquet/<YYYY-MM>.parquet files in the bucket")
        return 1
    log.info("%d collections, %d files", len(datasets), sum(len(d.files) for d in datasets))
    items = write_all(datasets, args.base_url, args.out_dir, duckdb_file=not args.no_duckdb)
    if args.upload_r2:
        # Folder pages and lists first, the root page last: it links to all of them.
        for path, key, ctype in sorted(items, key=lambda i: i[1] == "index.html"):
            r2.upload(path, key, content_type=ctype, cache_control=INDEX_CACHE)
    log.info("wrote %d files to %s%s", len(items), args.out_dir, " and uploaded them" if args.upload_r2 else "")
    return 0


if __name__ == "__main__":
    sys.exit(main())
