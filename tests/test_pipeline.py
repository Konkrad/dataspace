import json
from pathlib import Path

import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from cdsemeta import csvfile, geoparquet
from cdsemeta.config import Config
from cdsemeta.naming import latest_per_day, parquet_name, parquet_relpath, parse_tag, relpath_for_tag, tag_for
from cdsemeta.transform import build_table, parse_ts

DATA = Path(__file__).parent / "data"
CSV = DATA / "S1A_20240101_COPERNICUS_catalogue_20260901.csv"


def products():
    return {p["Id"]: p for p in json.loads((DATA / "odata_response.json").read_text())["value"]}


def test_naming():
    key = "S1A/2024/01/S1A_20240101_COPERNICUS_catalogue_20260901.csv"
    assert tag_for(key) == "S1A_20240101_COPERNICUS_catalogue_20260901"
    assert parquet_name(key) == "S1A_20240101_COPERNICUS_catalogue_20260901.parquet"
    assert parquet_relpath(key) == "S1A/2024/01/S1A_20240101_COPERNICUS_catalogue_20260901.parquet"
    assert relpath_for_tag(tag_for(key)) == parquet_relpath(key)
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


def test_latest_per_day_orders_by_day_not_platform():
    # S1B's single day is chronologically between two of S1A's -- a plain
    # sort of the tag strings would group all of S1A before any S1B, since
    # tags start with the platform code. merge() needs (day, platform) order
    # so cross-satellite files interleave correctly.
    tags = [
        "S1A_20260101_COPERNICUS_catalogue_20260102",
        "S1A_20260103_COPERNICUS_catalogue_20260104",
        "S1B_20260102_COPERNICUS_catalogue_20260103",
    ]
    assert latest_per_day(tags) == [
        "S1A_20260101_COPERNICUS_catalogue_20260102",
        "S1B_20260102_COPERNICUS_catalogue_20260103",
        "S1A_20260103_COPERNICUS_catalogue_20260104",
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
    from cdsemeta.merge import merge

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


def test_build_stac_table(tmp_path):
    from cdsemeta.merge import merge
    from cdsemeta.stac import build_stac_table, write

    df = csvfile.read_csv(CSV)
    table, geoms, _ = build_table(df, products())
    a = geoparquet.write(table, geoms, tmp_path / "a.parquet")
    out = tmp_path / "all.parquet"
    merge([a], out, tmp_path)
    src = pq.read_table(out)

    odata_found_rows = src.column("odata_found").to_pylist().count(True)
    stac_table = build_stac_table(src, "sentinel-1")
    assert stac_table.num_rows == odata_found_rows < src.num_rows
    ext = stac_table.column("stac_extensions")[0].as_py()
    assert "https://stac-extensions.github.io/sar/v1.3.0/schema.json" in ext

    write(stac_table, src.schema.metadata[b"geo"], "sentinel-1", tmp_path / "stac.parquet")
    gdf = gpd.read_parquet(tmp_path / "stac.parquet")
    assert not gdf["id"].str.endswith(".SAFE").any()
    # odata_found=False rows (e.g. CARD-COH12, which has no OData attributes)
    # are dropped from the STAC output entirely.
    assert not gdf["id"].str.contains("CARD-COH12").any()

    found = gdf[gdf["product:type"].notna()].iloc[0]
    assert found["platform"] == "sentinel-1a"
    assert list(found["sar:polarizations"]) == ["HH", "HV"]
    assert found["sat:platform_international_designator"] == "2014-016A"
    assert found["product:timeliness_category"] in ("NRT-3h", "Fast-24h")
    assert found["processing:level"] == "L1"
    assert found["assets"]["data"]["href"].startswith("s3://eodata/")


def test_push_links_package_to_repo(tmp_path, monkeypatch):
    from cdsemeta.registry import SOURCE_ANNOTATION, SOURCE_REPO_URL, Registry

    calls = []
    reg = Registry("ghcr.io/konkrad/dataspace/sentinel-1", extra_args=[])
    monkeypatch.setattr(reg, "_run", lambda args, cwd=None: calls.append(args) or "")
    f = tmp_path / "S1A" / "2024" / "01" / "x.parquet"
    f.parent.mkdir(parents=True)
    f.write_bytes(b"x")
    reg.push("x", f, {"cdsemeta.rows": 1}, root=tmp_path)
    ann = json.loads((tmp_path / "x.parquet.annotations.json").read_text())["$manifest"]
    assert ann[SOURCE_ANNOTATION] == SOURCE_REPO_URL
    assert ann["cdsemeta.rows"] == "1"
    assert calls[0][1] == "ghcr.io/konkrad/dataspace/sentinel-1:x"
    assert "S1A/2024/01/x.parquet:application/vnd.apache.parquet" in calls[0]


@pytest.mark.parametrize("key", [
    "S2B/2023/07/S2B_20230715_COPERNICUS_catalogue_20260901.csv",
    "S5P/2024/01/S5P_20240101_COPERNICUS_catalogue_20260901.csv",
])
def test_naming_other_missions(key):
    tag = tag_for(key)
    assert parse_tag(tag).platform == key.split("/")[0]
    assert relpath_for_tag(tag) == parquet_relpath(key)


def test_config_missions(monkeypatch):
    from cdsemeta.config import Config

    for var in ("MISSION", "OCI_BASE", "OCI_REPO", "PLATFORMS"):
        monkeypatch.delenv(var, raising=False)
    cfg = Config.from_env()
    assert cfg.mission == "sentinel-1"
    assert cfg.platforms == ("S1A", "S1B", "S1C", "S1D")
    assert cfg.oci_repo == "ghcr.io/konkrad/dataspace/sentinel-1"

    monkeypatch.setenv("MISSION", "sentinel-2")
    cfg = Config.from_env()
    assert cfg.platforms == ("S2A", "S2B", "S2C")
    assert cfg.oci_repo == "ghcr.io/konkrad/dataspace/sentinel-2"

    monkeypatch.setenv("PLATFORMS", "S2C")
    monkeypatch.setenv("OCI_REPO", "localhost:5000/x")
    cfg = Config.from_env()
    assert cfg.platforms == ("S2C",) and cfg.oci_repo == "localhost:5000/x"

    monkeypatch.setenv("MISSION", "landsat-9")
    with pytest.raises(ValueError, match="sentinel-1"):
        Config.from_env()


def test_tags_of_new_package_is_empty(monkeypatch):
    from cdsemeta.registry import Registry, RegistryError

    reg = Registry("r", extra_args=[])

    def fail(args, cwd=None):
        raise RegistryError("repo tags failed (1): Error response from registry: name unknown: repository name not known")

    monkeypatch.setattr(reg, "_run", fail)
    assert reg.tags() == set()


class _FakeResponse:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)


def test_odata_waf_block_raises_blocked(monkeypatch):
    from cdsemeta.odata import Blocked, ODataClient

    client = ODataClient(requests.Session(), "https://example/odata", rps=0)
    body = {"status": "error", "data": {"message": "rejected due to a violation, reference ID: 123"}}
    monkeypatch.setattr(client.session, "get", lambda *a, **k: _FakeResponse(403, body))
    with pytest.raises(Blocked, match="violation"):
        client._get(None)


def test_odata_ordinary_403_is_not_blocked(monkeypatch):
    from cdsemeta.odata import ODataClient

    client = ODataClient(requests.Session(), "https://example/odata", rps=0)
    monkeypatch.setattr(client.session, "get", lambda *a, **k: _FakeResponse(403, {"error": {"message": "nope"}}))
    with pytest.raises(requests.HTTPError):
        client._get(None)


def test_worker_backs_off_on_blocked(monkeypatch):
    from cdsemeta import worker
    from cdsemeta.listing import CsvEntry
    from cdsemeta.odata import Blocked

    sleeps = []

    def fake_sleep(seconds):
        sleeps.append(seconds)
        raise worker.Stop()

    monkeypatch.setattr(worker.time, "sleep", fake_sleep)
    monkeypatch.setattr(worker, "list_csvs", lambda *a, **k: [CsvEntry("S1A/2024/01/x.csv", "md5", 1, "now")])
    monkeypatch.setattr(worker, "process", lambda *a, **k: (_ for _ in ()).throw(Blocked("rejected")))
    monkeypatch.setattr(worker.Registry, "tags", lambda self: set())

    cfg = Config.from_env()
    try:
        worker.run(cfg)
    except worker.Stop:
        pass
    # Backs off and would retry, rather than idling forever.
    assert sleeps == [worker.BLOCKED_WAIT_MIN]


def test_worker_exits_on_blocked_when_run_once(monkeypatch):
    from cdsemeta import worker
    from cdsemeta.listing import CsvEntry
    from cdsemeta.odata import Blocked

    monkeypatch.setenv("RUN_ONCE", "1")
    monkeypatch.setattr(worker.time, "sleep", lambda s: pytest.fail("should not sleep"))
    monkeypatch.setattr(worker, "list_csvs", lambda *a, **k: [CsvEntry("S1A/2024/01/x.csv", "md5", 1, "now")])
    monkeypatch.setattr(worker, "process", lambda *a, **k: (_ for _ in ()).throw(Blocked("rejected")))
    monkeypatch.setattr(worker.Registry, "tags", lambda self: set())
    assert worker.run(Config.from_env()) == 1


def test_worker_stops_polling_once_caught_up(monkeypatch):
    from cdsemeta import worker

    sleeps = []

    def fake_sleep(seconds):
        sleeps.append(seconds)
        raise worker.Stop()

    monkeypatch.setattr(worker.time, "sleep", fake_sleep)
    monkeypatch.setattr(worker, "list_csvs", lambda *a, **k: [])
    monkeypatch.setattr(worker.Registry, "tags", lambda self: set())

    cfg = Config.from_env()  # RUN_ONCE=0 by default -- continuous backfill mode
    assert not cfg.run_once
    try:
        worker.run(cfg)
    except worker.Stop:
        pass
    # Never reaches the per-pass "sleeping Ns before checking again" sleep
    # (cfg.poll_interval); only the permanent 86400s idle sleep.
    assert sleeps == [86400]

