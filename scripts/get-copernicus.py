#!/usr/bin/env python3
# ./scripts/get-copernicus.py --resolution 30 --region ./data/himalayas.geojson --output ./data/himalayas
# Generate Copernicus DEM tile grid, intersect with region, and download tiles using parallel curl.

import argparse
import math
import subprocess
from pathlib import Path

import geopandas as gpd
import numpy as np
from shapely.geometry import box


# Copernicus resolution → bucket / prefix mapping
def copernicus_params(resolution: int) -> dict:
    """
    Map DEM resolution (meters) to Copernicus AWS Open Data parameters.
    """
    if resolution == 30:
        return {
            "bucket": "copernicus-dem-30m",
            "arcsec": "10",  # 10 arc-second ≈ 30 m
        }
    if resolution == 90:
        return {
            "bucket": "copernicus-dem-90m",
            "arcsec": "30",  # 30 arc-second ≈ 90 m
        }
    raise argparse.ArgumentTypeError("resolution must be one of: 30, 90")


# Tile naming helpers
def fmt_lat(lat: int) -> str:
    return f"{'N' if lat >= 0 else 'S'}{abs(lat):02d}_00"


def fmt_lon(lon: int) -> str:
    return f"{'E' if lon >= 0 else 'W'}{abs(lon):03d}_00"


# CLI
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Download Copernicus DEM tiles intersecting a region GeoJSON, "
            "using curl parallel downloads (no custom async)."
        )
    )

    p.add_argument(
        "--resolution",
        type=int,
        choices=[30, 90],
        required=True,
        help="DEM resolution in meters (allowed: 30, 90)",
    )

    p.add_argument(
        "--region",
        type=Path,
        required=True,
        help="Path to input region file (GeoJSON, GeoPackage, etc.)",
    )

    p.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Output directory for downloaded .tif tiles",
    )

    p.add_argument(
        "--jobs",
        type=int,
        default=8,
        help="Maximum number of parallel curl jobs (default: 8)",
    )

    p.add_argument(
        "--yes",
        action="store_true",
        help="Do not prompt before starting downloads",
    )

    return p.parse_args()


# Main
def main() -> None:
    args = parse_args()

    params = copernicus_params(args.resolution)
    outdir: Path = args.output
    outdir.mkdir(parents=True, exist_ok=True)

    if not args.region.exists():
        raise SystemExit(f"Region file does not exist: {args.region}")

    # Load region and normalize CRS
    region = gpd.read_file(args.region)
    if region.empty:
        raise SystemExit("Region file contains no geometries")

    if region.crs is None:
        raise SystemExit("Region CRS is missing; must be defined")

    region = region.to_crs("EPSG:4326")
    geom = region.geometry.union_all()
    minx, miny, maxx, maxy = geom.bounds

    # Generate 1° tile grid using NumPy
    lons = np.arange(math.floor(minx), math.ceil(maxx))
    lats = np.arange(math.floor(miny), math.ceil(maxy))

    records = []
    for lat in lats:
        for lon in lons:
            records.append(
                {
                    "lat": lat,
                    "lon": lon,
                    "geometry": box(lon, lat, lon + 1, lat + 1),
                }
            )

    grid = gpd.GeoDataFrame(records, crs="EPSG:4326")

    # Spatial intersection
    grid = grid[grid.intersects(geom)].copy()

    if grid.empty:
        print("No Copernicus tiles intersect the given region")
        return

    # Build Copernicus URLs
    prefix = f"Copernicus_DSM_COG_{params['arcsec']}"

    grid["tile"] = grid.apply(
        lambda r: f"{prefix}_{fmt_lat(r.lat)}_{fmt_lon(r.lon)}_DEM", axis=1
    )

    grid["filename"] = grid["tile"] + ".tif"

    grid["url"] = (
        "https://"
        + params["bucket"]
        + ".s3.amazonaws.com/"
        + grid["tile"]
        + "/"
        + grid["filename"]
    )

    grid["out"] = grid["filename"].map(lambda f: outdir / f)

    # Persist grid for inspection / QGIS
    grid_path = outdir / "copernicus_tile_grid.geojson"
    grid.to_file(grid_path, driver="GeoJSON")
    print(f"Tile grid written to: {grid_path}")

    # Skip already-downloaded files
    missing = grid[~grid["out"].map(Path.exists)]

    if missing.empty:
        print("All intersecting tiles already exist on disk")
        return

    print(f"Tiles to download: {len(missing)}")

    if not args.yes:
        if input("Proceed with download? [y/N] ").strip().lower() != "y":
            print("Aborted")
            return

    # Build and run curl command
    cmd = [
        "curl",
        "--fail",
        "--location",
        "--parallel",
        "--parallel-max",
        str(args.jobs),
    ]

    for _, r in missing.iterrows():
        cmd += ["-o", str(r.out), r.url]

    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
