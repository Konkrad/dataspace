"""Write GeoParquet 1.1 (WKB geometry, OGC:CRS84, bbox covering column)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Sequence

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import shapely

from . import __version__

BBOX_TYPE = pa.struct([(k, pa.float64()) for k in ("xmin", "ymin", "xmax", "ymax")])


def add_geometry(table: pa.Table, geoms: Sequence[shapely.Geometry | None]) -> tuple[pa.Table, dict]:
    arr = np.array(geoms, dtype=object)
    wkb = shapely.to_wkb(arr, output_dimension=2)
    bounds = shapely.bounds(arr)  # NaN rows for missing geometries
    valid = ~np.isnan(bounds[:, 0])

    bbox = pa.StructArray.from_arrays(
        [pa.array(bounds[:, i], mask=~valid) for i in range(4)],
        fields=list(BBOX_TYPE),
        mask=pa.array(~valid),
    )
    table = table.append_column("geometry", pa.array(list(wkb), type=pa.binary()))
    table = table.append_column("bbox", bbox)

    types = sorted({shapely.get_type_id(g) for g in arr if g is not None})
    names = {0: "Point", 1: "LineString", 3: "Polygon", 4: "MultiPoint", 5: "MultiLineString",
             6: "MultiPolygon", 7: "GeometryCollection"}
    col_meta: dict = {
        "encoding": "WKB",
        "geometry_types": [names[t] for t in types if t in names],
        "covering": {"bbox": {k: ["bbox", k] for k in ("xmin", "ymin", "xmax", "ymax")}},
    }
    if valid.any():
        b = bounds[valid]
        col_meta["bbox"] = [float(b[:, 0].min()), float(b[:, 1].min()), float(b[:, 2].max()), float(b[:, 3].max())]
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        # No "crs" key means OGC:CRS84 (lon/lat WGS84), which is what CDSE uses.
        "columns": {"geometry": col_meta},
        "creator": {"library": "cdsemeta", "version": __version__},
    }
    return table, geo


def write(table: pa.Table, geoms: Sequence[shapely.Geometry | None], path: Path) -> Path:
    table, geo = add_geometry(table, geoms)
    meta = dict(table.schema.metadata or {})
    meta[b"geo"] = json.dumps(geo).encode()
    table = table.replace_schema_metadata(meta)
    tmp = path.with_suffix(path.suffix + ".part")
    pq.write_table(table, tmp, compression="zstd", compression_level=9, row_group_size=100_000,
                   write_statistics=True)
    tmp.replace(path)
    return path


def check(path: Path, expected_rows: int) -> None:
    """Read the file back and make sure it is complete and valid GeoParquet."""
    pf = pq.ParquetFile(path)
    if pf.metadata.num_rows != expected_rows:
        raise ValueError(f"{path.name}: {pf.metadata.num_rows} rows written, expected {expected_rows}")
    meta = pf.schema_arrow.metadata or {}
    if b"geo" not in meta:
        raise ValueError(f"{path.name}: missing GeoParquet metadata")
    json.loads(meta[b"geo"])
