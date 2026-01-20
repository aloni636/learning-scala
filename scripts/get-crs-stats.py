#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from pyproj import CRS
from pyproj.proj import Proj


def factors_for_points(crs: CRS, lon, lat):
    """Compute PROJ distortion factors at lon/lat points (degrees)."""
    proj = Proj(crs)
    f = proj.get_factors(lon, lat)

    k_mer = np.asarray(f.meridional_scale)  # N-S
    k_par = np.asarray(f.parallel_scale)  # E-W
    k_areal = np.asarray(f.areal_scale)

    k_dist = np.sqrt(k_mer * k_par)

    # Prefer true Tissot-based anisotropy (direction independent)
    t_a = np.asarray(f.tissot_semimajor)
    t_b = np.asarray(f.tissot_semiminor)
    anisotropy = t_a / t_b

    angular_distortion = np.asarray(f.angular_distortion)

    return {
        "k_mer": k_mer,
        "k_par": k_par,
        "k_dist": k_dist,
        "anisotropy": anisotropy,
        "angular_distortion": angular_distortion,
        "k_areal": k_areal,
    }


def summarize(arr):
    """Return a stable set of summary stats for a numeric array."""
    a = arr[np.isfinite(arr)]

    def pct(p):
        return float(np.percentile(a, p))

    return {
        "min": float(a.min()),
        "p50": pct(50),
        "p95": pct(95),
        "p99": pct(99),
        "max": float(a.max()),
    }


def load_region_geometry(region_path: Path):
    gdf = gpd.read_file(region_path)
    if gdf.empty:
        raise ValueError(f"Region file has no geometries: {region_path}")

    # If CRS is missing, assume lon/lat WGS84.
    if gdf.crs is None:
        print(
            "WARNING: Region CRS is missing; assuming EPSG:4326 (lon/lat degrees).",
            file=sys.stderr,
        )
        gdf = gdf.set_crs("EPSG:4326")

    gdf = gdf.to_crs("EPSG:4326")

    # Avoid GeoPandas deprecation warning: prefer union_all() when available.
    geom = (
        gdf.geometry.union_all()
        if hasattr(gdf.geometry, "union_all")
        else gdf.geometry.unary_union
    )

    lon_min, lat_min, lon_max, lat_max = map(float, gdf.total_bounds)
    return geom, lon_min, lon_max, lat_min, lat_max


def build_parser():
    p = argparse.ArgumentParser(
        prog="get-crs-stats.py",
        description="Compute PROJ distortion factor stats over sampled lon/lat points inside a region geometry.",
    )

    p.add_argument("--proj4", required=True, help="Projection definition as a PROJ string")
    p.add_argument(
        "--region",
        type=Path,
        required=True,
        help="Vector file (GeoJSON, GPKG, etc). Samples only points inside geometry.",
    )
    p.add_argument("--step_deg", type=float, default=0.05, help="Sampling step in degrees (default: 0.05)")


    return p


def main():
    args = build_parser().parse_args()

    if args.step_deg <= 0:
        raise SystemExit("--step_deg must be > 0")

    crs = CRS.from_proj4(args.proj4)

    geom, lon_min, lon_max, lat_min, lat_max = load_region_geometry(args.region)

    lons = np.arange(lon_min, lon_max + 1e-12, args.step_deg)
    lats = np.arange(lat_min, lat_max + 1e-12, args.step_deg)
    Lon, Lat = np.meshgrid(lons, lats)

    lon_flat = Lon.ravel()
    lat_flat = Lat.ravel()

    pts = gpd.GeoSeries(gpd.points_from_xy(lon_flat, lat_flat), crs="EPSG:4326")
    mask = pts.within(geom).to_numpy()

    lon_in = lon_flat[mask]
    lat_in = lat_flat[mask]

    if lon_in.size == 0:
        raise SystemExit("No sample points fall inside the region (try smaller --step_deg)")

    factors = factors_for_points(crs, lon_in, lat_in)

    print("=== Projection WKT ===")
    print(crs.to_wkt())
    print()

    print("=== Region bounds (EPSG:4326 degrees) ===")
    print(
        f"lon_min={lon_min:.6f} lon_max={lon_max:.6f} lat_min={lat_min:.6f} lat_max={lat_max:.6f}"
    )
    print(f"step_deg={args.step_deg}")
    print()

    print("=== Sampling ===")
    print(f"candidate_grid={Lon.shape[1]}x{Lon.shape[0]} candidate_samples={int(Lon.size)}")
    print(f"samples_inside_region={int(lon_in.size)}")
    print()

    print("=== Distortion summary ===")

    rows = [
        {"key": "k_mer", "label": "k_mer (N-S scale)", "baseline": 1.0},
        {"key": "k_par", "label": "k_par (E-W scale)", "baseline": 1.0},
        {"key": "k_dist", "label": "k_dist (sqrt(k_mer*k_par))", "baseline": 1.0},
        {"key": "anisotropy", "label": "anisotropy (tissot a/b)", "baseline": 1.0},
        {"key": "angular_distortion", "label": "angular_distortion", "baseline": 0.0},
        {"key": "k_areal", "label": "k_areal (area scale)", "baseline": 1.0},
    ]

    def fmt_cell(v: float, baseline: float) -> str:
        """Format value and a compact percent next to it."""
        if not np.isfinite(v):
            return "nan"
        pct = (v - 1.0) * 100.0 if baseline == 1.0 else v * 100.0
        return f"{v:.6g}({pct:+.3g}%)"

    out_rows = []
    for r in rows:
        key = r["key"]
        label = r["label"]
        baseline = r["baseline"]

        stats = summarize(factors[key])

        max_abs_pct = ""
        if key.startswith("k_"):
            a = factors[key]
            aa = a[np.isfinite(a)]
            max_abs_pct = f"{float(np.max(np.abs(aa - 1.0)) * 100.0):.3g}%"

        out_rows.append(
            {
                "metric": label,
                "min": fmt_cell(stats["min"], baseline),
                "p50": fmt_cell(stats["p50"], baseline),
                "p95": fmt_cell(stats["p95"], baseline),
                "p99": fmt_cell(stats["p99"], baseline),
                "max": fmt_cell(stats["max"], baseline),
                "max_abs_pct": max_abs_pct,
            }
        )

    df = pd.DataFrame(out_rows, columns=["metric", "min", "p50", "p95", "p99", "max", "max_abs_pct"])
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
