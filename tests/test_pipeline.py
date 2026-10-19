import json
import shutil
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
import requests

from cdsemeta import csvfile, geoparquet
from cdsemeta.config import Config
from cdsemeta.naming import (day_tag_for, find_day_file, latest_per_day, parquet_name, parquet_relpath, parse_tag,
                             relpath_for_tag, tag_for)
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
    kept = csvfile.read_csv(CSV, Config().exclude)  # sentinel-1: CARD products are dropped
    assert len(kept) == 2 and not kept["Name"].str.contains("CARD").any()


def test_worker_skips_csvs_before_launch(monkeypatch):
    from cdsemeta import worker
    from cdsemeta.listing import CsvEntry

    entries = [CsvEntry("S1B/2015/06/S1B_20150601_COPERNICUS_catalogue_20261001.csv", "m", 1, "now"),
               CsvEntry("S1B/2016/06/S1B_20160601_COPERNICUS_catalogue_20261001.csv", "m", 1, "now")]
    monkeypatch.setattr(worker, "list_csvs", lambda *a, **k: entries)
    monkeypatch.setattr(worker.Registry, "tags", lambda self: set())
    seen = []
    monkeypatch.setattr(worker, "process", lambda entry, *a: seen.append(entry.key) or (_ for _ in ()).throw(worker.Stop()))
    monkeypatch.setenv("RUN_ONCE", "1")
    worker.run(Config.from_env())
    assert seen == [entries[1].key]


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


def test_push_passes_absolute_annotation_path(tmp_path, monkeypatch):
    from cdsemeta.registry import Registry

    calls = []
    reg = Registry("ghcr.io/konkrad/dataspace/sentinel-1", extra_args=[])
    monkeypatch.setattr(reg, "_run", lambda args, cwd=None: calls.append(args) or "")
    f = tmp_path / "ids" / "S1A" / "2014.parquet"
    f.parent.mkdir(parents=True)
    f.write_bytes(b"x")
    monkeypatch.chdir(tmp_path)
    reg.push("x", Path("ids") / "S1A" / "2014.parquet", {}, root=Path("ids"))
    args = calls[0]
    assert Path(args[args.index("--annotation-file") + 1]).is_absolute()
    assert "S1A/2014.parquet:application/vnd.apache.parquet" in args


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
    monkeypatch.setattr(worker, "list_csvs", lambda *a, **k: [CsvEntry(KEY, "md5", 1, "now")])
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
    monkeypatch.setattr(worker, "list_csvs", lambda *a, **k: [CsvEntry(KEY, "md5", 1, "now")])
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


class _FakeOData:
    """Fake day-window endpoint with CDSE's paging cap (stops at $skip=10000)."""

    def __init__(self, products):
        self.products = products  # list of (Id, datetime)
        self.requests = []

    def get(self, url, params=None, timeout=None):
        import re as _re
        from datetime import datetime as _dt
        from urllib.parse import parse_qs, urlparse

        if params is None:  # nextLink
            q = {k: v[0] for k, v in parse_qs(urlparse(url).query).items()}
        else:
            q = dict(params)
        self.requests.append(q)
        if q["$filter"].startswith("Id eq"):
            ids = set(_re.findall(r"Id eq ([0-9a-f-]+)", q["$filter"]))
            vals = [{"Id": i} for i, _ in self.products if i in ids]
            return _FakeResponse(200, {"value": vals})
        ge, lt = _re.findall(r"ge (\S+) and ContentDate/Start lt (\S+)", q["$filter"])[0]
        parse = lambda s: _dt.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        hits = sorted(i for i, t in self.products if parse(ge) <= t < parse(lt))
        skip = int(q.get("$skip", 0))
        body = {"value": [{"Id": i} for i in hits[skip:skip + 1000]], "@odata.count": len(hits)}
        if skip + 1000 < len(hits) and skip + 1000 <= 10000:
            body["@odata.nextLink"] = "https://example/odata?" + "&".join(
                f"{k}={v}" for k, v in {**q, "$skip": skip + 1000}.items())
        return _FakeResponse(200, body)


def _products(n, day=datetime(2018, 5, 7, tzinfo=timezone.utc)):
    import uuid
    return [(str(uuid.UUID(int=i)), day + timedelta(seconds=i * 86400 // n)) for i in range(n)]


def test_fetch_day_splits_windows_past_the_paging_cap():
    from cdsemeta.odata import ODataClient

    prods = _products(12416)
    client = ODataClient(_FakeOData(prods), "https://example/odata", rps=0)
    ids = [i for i, _ in prods]
    got = client.fetch_day("SENTINEL-2", "S2B", date(2018, 5, 7), ids)
    assert set(got) == set(ids)
    # day split once into two ~6k windows; far fewer than 125 ID batches
    assert len(client.session.requests) < 20


def test_fetch_day_checks_missing_ids_with_per_id_lookup():
    from cdsemeta.odata import ODataClient

    prods = _products(50)
    # one product the day query can't see (outside the day), one truly absent
    stray = ("00000000-0000-0000-0000-0000000000ff", datetime(2018, 5, 9, tzinfo=timezone.utc))
    fake = _FakeOData(prods + [stray])
    client = ODataClient(fake, "https://example/odata", rps=0)
    absent = "00000000-0000-0000-0000-0000000000fe"
    got = client.fetch_day("SENTINEL-2", "S2B", date(2018, 5, 7), [i for i, _ in prods] + [stray[0], absent])
    assert stray[0] in got          # found by the ID fallback
    assert absent not in got        # missing from both -> reported missing
    assert len(got) == 51


# -- per-day tags, ID files, per-product updates ----------------------------

KEY = "S1A/2024/01/S1A_20240101_COPERNICUS_catalogue_20260901.csv"
NEW_KEY = KEY.replace("20260901", "20261001")
ADDED_ID = "11111111-2222-3333-4444-555555555555"
ADDED_NAME = "S1A_IW_GRDH_1SDV_20240101T003214_20240101T003238_051906_064571_1F54.SAFE"
CARD = r"_CARD[-_][A-Z0-9]+$"


def test_day_tags(tmp_path):
    assert day_tag_for(KEY) == "S1A_20240101"
    info = parse_tag("S1A_20240101")
    assert (info.platform, info.day, info.generated) == ("S1A", "20240101", "")
    assert parse_tag(tag_for(KEY)).day_tag == "S1A_20240101"
    assert relpath_for_tag("S1A_20240101") is None
    for t in ("ids-2024-s1-s1a", "grd-2024", "all-20260925"):
        assert parse_tag(t) is None
    # the per-day tag wins over any legacy generation of the same day
    assert latest_per_day(["S1A_20240101_COPERNICUS_catalogue_20261001", "S1A_20240101",
                           "S1B_20240101_COPERNICUS_catalogue_20260901"]) == [
        "S1A_20240101", "S1B_20240101_COPERNICUS_catalogue_20260901"]
    d = tmp_path / "S1A/2024/01"
    d.mkdir(parents=True)
    for g in ("20260901", "20261001"):
        (d / f"S1A_20240101_COPERNICUS_catalogue_{g}.parquet").write_bytes(b"")
    assert find_day_file(tmp_path, "S1A", "20240101").name.endswith("20261001.parquet")
    assert find_day_file(tmp_path, "S1A", "20240102") is None


def test_todo_list_counts_any_tag_of_the_day():
    from cdsemeta.listing import CsvEntry
    from cdsemeta.worker import todo_list

    e = lambda d, g="20261001": CsvEntry(f"S1A/2024/01/S1A_{d}_COPERNICUS_catalogue_{g}.csv", "m", 1, "now")
    tags = {"S1A_20240101", "S1A_20240102_COPERNICUS_catalogue_20261001",
            "S1A_20240103_COPERNICUS_catalogue_20260901"}
    todo = todo_list([e("20240101"), e("20240102"), e("20240103"), e("20240104")], tags, "asc")
    # 03 was built from an older generation: still there, a regeneration is sync's job, not new work
    assert [x.key.split("/")[-1][:12] for x in todo] == ["S1A_20240104"]


def _modified_csv(df):
    """The test CSV a month later: row 0 (CARD-COH12) modified, row 2 removed, one GRD added."""
    new = df.copy()
    new.loc[0, "ModificationDate"] = "2026-10-01T00:00:00.000"
    new.loc[0, "Checksum:Value"] = "ffff"
    added = new.iloc[[0]].copy()
    added["Id"], added["Name"] = ADDED_ID, ADDED_NAME
    return pd.concat([new.drop(index=2), added], ignore_index=True)


def _artifact(path, df=None):
    df = csvfile.read_csv(CSV) if df is None else df
    table, geoms, _ = build_table(df, products())
    path.parent.mkdir(parents=True, exist_ok=True)
    return geoparquet.write(table, geoms, path)


def test_id_files_and_changes():
    from cdsemeta import ids

    assert ids.tag("sentinel-1", "S1A", "2024") == "ids-2024-s1-s1a"
    assert ids.tag("sentinel-5p", "S5P", "2019") == "ids-2019-s5p-s5p"
    df = csvfile.read_csv(CSV)
    old = ids.table_for(df, "20240101")
    assert old.column_names == ["Name", "ModificationDate", "day"]
    new = ids.table_for(_modified_csv(df), "20240101")
    got = ids.changes(old, new)
    assert got.to_dict("records") == [{"day": "20240101", "added": 1, "removed": 1, "modified": 1}]
    # a product that moves to another day touches both days
    moved = ids.table_for(df.iloc[[1]], "20240102")
    got = ids.changes(old, pa.concat_tables([ids.table_for(df.drop(index=1), "20240101"), moved]))
    assert got.to_dict("records") == [{"day": "20240101", "added": 0, "removed": 1, "modified": 0},
                                      {"day": "20240102", "added": 1, "removed": 0, "modified": 0}]
    assert ids.changes(old, old).empty
    # first run: no ID file yet, every day counts
    assert ids.changes(ids.empty(), old)["added"].tolist() == [3]


def test_plan_and_update_day_by_name(tmp_path):
    from cdsemeta.update import plan_day, read_artifact, update_day

    df = csvfile.read_csv(CSV)
    old, old_geoms = read_artifact(_artifact(tmp_path / "old.parquet"))
    new_df = _modified_csv(df)
    redo, lookup, counts = plan_day(old, new_df)
    assert redo == {df.loc[0, "Name"], ADDED_NAME}
    assert counts == {"added": 1, "modified": 1, "removed": 1}
    assert lookup == {ADDED_ID}  # the modified row was CSV-only before: stays CSV-only
    # the same CSV again: nothing to do (OData's µs vs the CSV's ms don't count)
    assert plan_day(old, df)[0] == set()

    new, new_geoms, missing = update_day(old, old_geoms, new_df, redo, {})
    assert set(new.column("name").to_pylist()) == set(new_df["Name"])
    assert missing == 2 and len(new_geoms) == new.num_rows == 3
    starts = new.column("content_start").to_pylist()
    assert starts == sorted(starts)
    # the untouched OData row is identical, attributes included
    kept = df.loc[1, "Name"]
    assert (new.filter(pc.equal(new.column("name"), kept)).to_pylist()
            == old.filter(pc.equal(old.column("name"), kept)).to_pylist())
    assert new.schema.field("orbitNumber").type == pa.int64()
    row = new.filter(pc.equal(new.column("id"), df.loc[0, "Id"])).to_pylist()[0]
    assert row["checksum_md5"] == "ffff" and row["odata_found"] is False


def _zip(path, members):
    import zipfile
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        for key, df in members.items():
            z.writestr(f"CatalogueVolume/{key}", df.to_csv(sep=";", index=False))
    return path


def test_catalogue_zip_reads_members(tmp_path):
    from cdsemeta.catzip import CatalogueZip

    df = csvfile.read_csv(CSV)
    cz = CatalogueZip(_zip(tmp_path / "c.zip", {NEW_KEY: df, "S2A/2024/01/S2A_20240101_COPERNICUS_catalogue_20261001.csv": df}))
    assert cz.keys("S1A") == [NEW_KEY]
    out = cz.extract(NEW_KEY, tmp_path / "x.csv")
    assert csvfile.read_csv(out)["Id"].tolist() == df["Id"].tolist()


def test_http_file_stops_when_the_zip_changes():
    from cdsemeta.catzip import HttpFile, ZipChanged

    class Session:
        def head(self, url, timeout=None):
            r = _FakeResponse(200, None)
            r.headers = {"Content-Length": "100", "ETag": "abc"}
            return r

        def get(self, url, headers=None, timeout=None):
            assert headers["If-Match"] == "abc"
            return _FakeResponse(412, None)

    with pytest.raises(ZipChanged):
        HttpFile(Session(), "https://example/c.zip").read(10)


def test_sync_updates_changed_days_then_the_id_file(tmp_path, monkeypatch):
    from cdsemeta import ids, update

    df = csvfile.read_csv(CSV)
    arts = tmp_path / "arts"
    _artifact(arts / parquet_relpath(KEY))  # built from the old CSV
    unchanged_key = "S1A/2024/01/S1A_20240102_COPERNICUS_catalogue_20261001.csv"
    day2 = df.assign(Id=df["Id"].str[:-1] + "0", Name="D2" + df["Name"])
    _artifact(arts / parquet_relpath(unchanged_key.replace("20261001", "20260901")), day2)
    no_artifact_key = "S1A/2024/01/S1A_20240103_COPERNICUS_catalogue_20261001.csv"
    day2 = day2[~day2["Name"].str.contains("CARD")]  # as it was stored: without CARD rows
    zip_path = _zip(tmp_path / "c.zip", {NEW_KEY: _modified_csv(df), unchanged_key: day2,
                                         no_artifact_key: df.assign(Name="D3" + df["Name"]),
                                         "S1B/2015/06/S1B_20150601_COPERNICUS_catalogue_20261001.csv": df})
    id_root = tmp_path / "ids"
    files = ids.IdFiles(id_root, "sentinel-1")
    ids.write(pa.concat_tables([ids.table_for(df, "20240101"), ids.table_for(day2, "20240102")]),
              files.path("S1A", "2024"))
    monkeypatch.setenv("PLATFORMS", "S1A,S1B")

    monkeypatch.setenv("WORK_DIR", str(tmp_path / "work"))
    pushed = []
    monkeypatch.setattr(update.Registry, "push", lambda self, tag, f, ann, root=None: pushed.append((tag, ann)))
    monkeypatch.setattr(update.Registry, "tags", lambda self: pytest.fail("local artifacts: no tag listing"))
    cfg = Config.from_env()
    out = tmp_path / "out"
    assert update.sync(cfg, tmp_path / "work", push=False, zip_source=str(zip_path), artifacts_dir=arts,
                       out_dir=out, use_odata=False, ids_dir=id_root) == 0
    # only day 1 changed; day 2 isn't even looked at; day 3 has no artifact (the worker's job)
    assert sorted(p.name for p in out.rglob("*.parquet")) == [Path(NEW_KEY).stem + ".parquet"]
    t = pq.read_table(out / parquet_relpath(NEW_KEY))
    expected = set(_modified_csv(df)["Name"]) - {df.loc[0, "Name"]}  # CARD-COH12 is excluded on reading
    assert set(t.column("name").to_pylist()) == expected
    # the ID file now describes the new CSVs, all three days
    saved = ids.read(files.path("S1A", "2024"))
    assert sorted(set(saved.column("day").to_pylist())) == ["20240101", "20240102", "20240103"]
    assert ADDED_NAME in saved.column("Name").to_pylist()
    assert not any("CARD" in n for n in saved.column("Name").to_pylist())
    assert not files.path("S1B", "2015").exists()  # dated before S1B's launch
    assert not pushed
    # a second run finds nothing to do
    shutil.rmtree(out)
    assert update.sync(cfg, tmp_path / "work", push=False, zip_source=str(zip_path), artifacts_dir=arts,
                       out_dir=out, use_odata=False, ids_dir=id_root) == 0
    assert not out.exists()


def test_sync_keeps_the_id_file_when_a_day_fails(tmp_path, monkeypatch):
    from cdsemeta import ids, update

    df = csvfile.read_csv(CSV)
    arts = tmp_path / "arts"
    _artifact(arts / parquet_relpath(KEY))
    id_root = tmp_path / "ids"
    files = ids.IdFiles(id_root, "sentinel-1")
    ids.write(ids.table_for(df, "20240101"), files.path("S1A", "2024"))
    before = files.path("S1A", "2024").read_bytes()
    zip_path = _zip(tmp_path / "c.zip", {NEW_KEY: _modified_csv(df)})
    monkeypatch.setenv("WORK_DIR", str(tmp_path / "work"))
    monkeypatch.setenv("PLATFORMS", "S1A")
    monkeypatch.setattr(update, "update_day", lambda *a: (_ for _ in ()).throw(ValueError("boom")))
    assert update.sync(Config.from_env(), tmp_path / "work", push=False, zip_source=str(zip_path),
                       artifacts_dir=arts, use_odata=False, ids_dir=id_root) == 1
    assert files.path("S1A", "2024").read_bytes() == before


def test_init_ids_only_takes_days_built_from_that_csv(tmp_path, monkeypatch):
    from cdsemeta import ids, update

    df = csvfile.read_csv(CSV)
    day2 = "S1A/2024/01/S1A_20240102_COPERNICUS_catalogue_20261001.csv"
    day3 = "S1A/2024/01/S1A_20240103_COPERNICUS_catalogue_20261017.csv"
    zip_path = _zip(tmp_path / "c.zip", {NEW_KEY: df, day2: df.assign(Name=df["Name"] + "_2"),
                                         day3: df.assign(Name=df["Name"] + "_3")})
    tags = {Path(NEW_KEY).stem,                                   # built from exactly this CSV
            "S1A_20240103_COPERNICUS_catalogue_20261001"}         # built from an older daily version
    monkeypatch.setenv("PLATFORMS", "S1A")
    assert update.init_ids(Config.from_env(), tmp_path / "w", tags, push=False, zip_source=str(zip_path),
                           ids_dir=tmp_path / "ids") == 0
    t = ids.read(ids.IdFiles(tmp_path / "ids", "sentinel-1").path("S1A", "2024"))
    assert set(t.column("day").to_pylist()) == {"20240101"} and t.num_rows == 2  # without the CARD row


def test_retag_adds_per_day_tags(monkeypatch):
    from cdsemeta.registry import Registry
    from cdsemeta.update import retag

    reg = Registry("r", extra_args=[])
    calls = []
    tags = {"S1A_20240101_COPERNICUS_catalogue_20260901", "S1A_20240101_COPERNICUS_catalogue_20261001",
            "S1A_20240102_COPERNICUS_catalogue_20260901", "S1A_20240102", "grd-2024", "ids-2024-s1-s1a"}
    monkeypatch.setattr(reg, "tags", lambda: tags)
    monkeypatch.setattr(reg, "_run", lambda args, cwd=None: calls.append(args) or "")
    assert retag(reg) == 0
    assert calls == [["tag", "r:S1A_20240101_COPERNICUS_catalogue_20261001", "S1A_20240101"]]


def test_merge_pulls_per_day_tags(tmp_path):
    from cdsemeta.merge import pull_all

    class FakeRegistry:
        def pull(self, tag, dest):
            f = dest / "S1A/2024/01/S1A_20240101_COPERNICUS_catalogue_20261001.parquet"
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_bytes(b"")

    files = pull_all(FakeRegistry(), ["S1A_20240101"], tmp_path)
    assert files == [tmp_path / "S1A/2024/01/S1A_20240101_COPERNICUS_catalogue_20261001.parquet"]


def test_odata_row_falls_back_to_csv_s3path():
    from cdsemeta.transform import from_odata

    rec, _, _ = from_odata({"Id": "x", "S3Path": None}, {"S3Path": "s3://eodata/Sentinel-1/RAW/x"})
    assert rec["s3_path"] == "s3://eodata/Sentinel-1/RAW/x"


def test_ids_from_artifacts(tmp_path):
    from cdsemeta import ids

    df = csvfile.read_csv(CSV)
    table, geoms, _ = build_table(df, products())
    d = tmp_path / "S1A" / "2024" / "01"
    d.mkdir(parents=True)
    f = d / "S1A_20240101_COPERNICUS_catalogue_20260901.parquet"
    geoparquet.write(table, geoms, f)

    t = ids.table_from_artifacts([f])
    assert sorted(t["Name"].to_pylist()) == sorted(df["Name"])
    assert set(t["day"].to_pylist()) == {"20240101"}
    assert ids.changes(t, ids.table_for(df, "20240101")).empty
    assert ids.canon_ts("2026-06-03T14:11:38.38") == "2026-06-03T14:11:38.380"
    assert ids.canon_ts(datetime(2026, 6, 3, 14, 11, 38, 386999, tzinfo=timezone.utc)) == "2026-06-03T14:11:38.386"


def test_merge_and_ids_exclude(tmp_path):
    from cdsemeta import ids
    from cdsemeta.merge import merge

    df = csvfile.read_csv(CSV)
    table, geoms, _ = build_table(df, products())
    a = geoparquet.write(table, geoms, tmp_path / "S1A_20240101_COPERNICUS_catalogue_20260901.parquet")
    card = [n for n in df["Name"] if "_CARD" in n]
    assert card
    out = tmp_path / "all.parquet"
    rows = merge([a], out, tmp_path, exclude=(r"_CARD[-_][A-Z0-9]+$",))
    assert rows == len(df) - len(card)
    t = ids.table_from_artifacts([a], (r"_CARD[-_][A-Z0-9]+$",))
    assert not set(card) & set(t["Name"].to_pylist()) and t.num_rows == rows
