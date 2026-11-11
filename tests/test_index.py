from datetime import datetime, timezone

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from cdsemeta import index


def _obj(key, size=10, etag='"abc"'):
    return {"Key": key, "Size": size, "ETag": etag, "LastModified": datetime(2026, 11, 2, tzinfo=timezone.utc)}


def test_datasets_from_objects_groups_and_ignores_other_keys():
    objs = [_obj("sentinel-1-grd.parquet/2024-06.parquet"), _obj("sentinel-1-grd.parquet/2024-05.parquet"),
            _obj("sentinel-5p-l2.parquet/2019-01.parquet"), _obj("index.html"), _obj("files.txt"),
            _obj("sentinel-1-grd.parquet/index.html"), _obj("dataspace.duckdb"), _obj("other/2024-05.parquet")]
    ds = index.datasets_from_objects(objs)
    assert [d.id for d in ds] == ["sentinel-1-grd", "sentinel-5p-l2"]
    s1, s5p = ds
    assert [f.month for f in s1.files] == ["2024-05", "2024-06"]
    assert (s1.mission, s1.collection, s1.view, s1.folder) == ("sentinel-1", "grd", "sentinel_1_grd", "sentinel-1-grd.parquet/")
    assert (s5p.mission, s5p.collection, s5p.view) == ("sentinel-5p", "l2", "sentinel_5p_l2")
    assert s1.files[0].etag == "abc"


def test_write_all(tmp_path):
    data = tmp_path / "bucket"
    (data / "sentinel-1-grd.parquet").mkdir(parents=True)
    # Months with different columns: the view must union them by name.
    pq.write_table(pa.table({"name": ["a", "b"], "orbitNumber": [1, 2]}), data / "sentinel-1-grd.parquet/2024-05.parquet")
    pq.write_table(pa.table({"name": ["c"], "newAttr": ["x"]}), data / "sentinel-1-grd.parquet/2024-06.parquet")
    objs = [_obj(str(p.relative_to(data)), p.stat().st_size) for p in sorted(data.rglob("*.parquet"))]
    ds = index.datasets_from_objects(objs)

    out = tmp_path / "out"
    items = index.write_all(ds, str(data), out)
    keys = {k: c for _, k, c, _ in items}
    assert keys == {"index.html": "text/html; charset=utf-8",
                    "sentinel-1-grd.parquet/index.html": "text/html; charset=utf-8",
                    "files.txt": "text/plain; charset=utf-8",
                    "files.tsv": "text/tab-separated-values; charset=utf-8",
                    "dataspace.duckdb": "application/octet-stream"}

    assert (out / "files.txt").read_text().splitlines() == [
        f"{data}/sentinel-1-grd.parquet/2024-05.parquet", f"{data}/sentinel-1-grd.parquet/2024-06.parquet"]
    tsv = (out / "files.tsv").read_text().splitlines()
    assert tsv[0] == "url\tsize\tetag\tlast_modified" and len(tsv) == 3

    root = (out / "index.html").read_text()
    assert 'href="sentinel-1-grd.parquet/"' in root and "2024-05 – 2024-06" in root
    folder = (out / "sentinel-1-grd.parquet/index.html").read_text()
    assert 'href="2024-06.parquet"' in folder and 'href="../"' in folder

    con = duckdb.connect()
    con.execute(f"ATTACH '{out / 'dataspace.duckdb'}' AS ds (READ_ONLY)")
    assert con.sql("SELECT count(*), count(newAttr), count(orbitNumber) FROM ds.sentinel_1_grd").fetchone() == (3, 1, 2)
    assert con.sql("SELECT name, first_month, last_month, files FROM ds.datasets").fetchone() == (
        "sentinel_1_grd", "2024-05", "2024-06", 2)
    assert con.sql("SELECT count(*) FROM ds.files").fetchone() == (2,)


def test_write_all_with_viewer(tmp_path):
    dist = tmp_path / "dist"
    (dist / ".vite").mkdir(parents=True)
    (dist / "assets").mkdir()
    (dist / "assets/main-abc.js").write_text("console.log(1)")
    (dist / "assets/main-abc.css").write_text("body{}")
    (dist / ".vite/manifest.json").write_text(
        '{"src/main.js": {"file": "assets/main-abc.js", "isEntry": true, "css": ["assets/main-abc.css"]}}')
    ds = index.datasets_from_objects([_obj("sentinel-1-grd.parquet/2024-05.parquet"),
                                      _obj("sentinel-1-grd.parquet/2024-06.parquet")])
    out = tmp_path / "out"
    items = index.write_all(ds, "https://example.org/", out, duckdb_file=False, viewer_dist=dist)
    by_key = {k: (c, cache) for _, k, c, cache in items}
    assert by_key["_viewer/assets/main-abc.js"] == ("text/javascript; charset=utf-8", index.ASSET_CACHE)
    assert by_key["_viewer/assets/main-abc.css"][0] == "text/css; charset=utf-8"
    assert by_key["index.html"][1] == index.INDEX_CACHE
    assert not any(k.startswith("_viewer/.vite") for k in by_key)
    root = (out / "index.html").read_text()
    assert '<script type="module" src="/_viewer/assets/main-abc.js"></script>' in root
    assert '<link rel="stylesheet" href="/_viewer/assets/main-abc.css">' in root
    data = root.split('<script id="datasets" type="application/json">')[1].split("</script>")[0]
    import json
    assert json.loads(data) == {"base": "https://example.org",
                                "datasets": [{"id": "sentinel-1-grd", "months": ["2024-05", "2024-06"]}]}
    # Viewer files stay out of the download lists.
    assert "_viewer" not in (out / "files.txt").read_text()
