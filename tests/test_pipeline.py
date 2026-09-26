import json
from pathlib import Path

import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from s1meta import csvfile, geoparquet
from s1meta.naming import latest_per_day, parquet_name, parse_tag, tag_for
from s1meta.transform import build_table, parse_ts

DATA = Path(__file__).parent / "data"
CSV = DATA / "S1A_20240101_COPERNICUS_catalogue_20260901.csv"


def products():
    return {p["Id"]: p for p in json.loads((DATA / "odata_response.json").read_text())["value"]}


def test_naming():
    key = "S1A/2024/01/S1A_20240101_COPERNICUS_catalogue_20260901.csv"
    assert tag_for(key) == "S1A_20240101_COPERNICUS_catalogue_20260901"
    assert parquet_name(key) == "S1A_20240101_COPERNICUS_catalogue_20260901.parquet"
    info = parse_tag(tag_for(key))
    assert (info.platform, info.day, info.generated) == ("S1A", "20240101", "20260901")
    assert parse_tag("all-20260925") is None
    with pytest.raises(ValueError):
        tag_for("S1A/bad name.csv")


def test_latest_per_day():
    tags = [
        "S1A_20260920_COPERNICUS_catalogue_20260921",
        "S1A_20260920_COPERNICUS_catalogue_20260925",
        "S1B_20260920_COPERNICUS_catalogue_20260921",
        "all-20260925",
    ]
    assert latest_per_day(tags) == [
        "S1A_20260920_COPERNICUS_catalogue_20260925",
        "S1B_20260920_COPERNICUS_catalogue_20260921",
    ]


def test_parse_ts():
    assert parse_ts("9999-12-31T23:59:59.999999Z").year == 9999
    assert parse_ts("2024-02-16T11:09:59.935").utcoffset().total_seconds() == 0
    assert parse_ts("2024-01-01T00:00:00.1234567Z").microsecond == 123456
    assert parse_ts(None) is None and parse_ts("") is None


def test_read_csv():
    df = csvfile.read_csv(CSV)
    assert len(df) == 3
    assert "Bbox" in df.columns


def test_build_and_write(tmp_path):
    df = csvfile.read_csv(CSV)
    table, geoms, missing = build_table(df, products())
    assert missing == 1
    assert table.num_rows == 3
    assert table.schema.field("orbitNumber").type == pa.int64()
    assert table.schema.field("productType").type == pa.string()
    assert pa.types.is_timestamp(table.schema.field("processingDate").type)
    starts = table.column("content_start").to_pylist()
    assert starts == sorted(starts)

    out = geoparquet.write(table, geoms, tmp_path / "x.parquet")
    geoparquet.check(out, 3)
    gdf = gpd.read_parquet(out)
    assert len(gdf) == 3 and gdf.crs.to_string() == "OGC:CRS84"
    assert gdf.geometry.notna().all()

    missing_row = gdf[~gdf.odata_found].iloc[0]
    assert missing_row["name"].endswith("CARD-COH12")
    assert missing_row["checksum_md5"] == "a1da1d85bc98a1235a89c7c9dd9deb34"
    assert missing_row["s3_path"].startswith("s3://eodata/")

    found = gdf[gdf.odata_found]
    assert found["quicklook_url"].notna().all()
    assert found["checksum_blake3"].notna().all()

    geo = json.loads(pq.read_schema(out).metadata[b"geo"])
    assert geo["columns"]["geometry"]["geometry_types"] == ["Polygon"]
    assert "covering" in geo["columns"]["geometry"]


def test_merge(tmp_path):
    from s1meta.merge import merge

    df = csvfile.read_csv(CSV)
    table, geoms, _ = build_table(df, products())
    a = geoparquet.write(table, geoms, tmp_path / "a.parquet")
    # second file without OData attributes -> different columns
    table2, geoms2, _ = build_table(df, {})
    b = geoparquet.write(table2, geoms2, tmp_path / "b.parquet")
    out = tmp_path / "all.parquet"
    assert merge([a, b], out, tmp_path) == 6
    gdf = gpd.read_parquet(out)
    assert len(gdf) == 6 and "orbitNumber" in gdf.columns


def test_push_links_package_to_repo(tmp_path, monkeypatch):
    from s1meta.registry import SOURCE_ANNOTATION, SOURCE_REPO_URL, Registry

    calls = []
    reg = Registry("ghcr.io/konkrad/dataspace", extra_args=[])
    monkeypatch.setattr(reg, "_run", lambda args, cwd=None: calls.append(args) or "")
    f = tmp_path / "x.parquet"
    f.write_bytes(b"x")
    reg.push("x", f, {"s1meta.rows": 1})
    ann = json.loads((tmp_path / "x.parquet.annotations.json").read_text())["$manifest"]
    assert ann[SOURCE_ANNOTATION] == SOURCE_REPO_URL
    assert ann["s1meta.rows"] == "1"
    assert calls[0][1] == "ghcr.io/konkrad/dataspace:x"
