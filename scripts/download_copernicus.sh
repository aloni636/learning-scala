#!/usr/bin/env bash
set -euo pipefail

# Stage Copernicus DEM COG tiles (1x1 degree) by tile indices, using curl native parallelism.
#
# Usage:
#   ./download_copernicus.sh <30|90> <lon_min_deg> <lat_min_deg> <lon_max_deg> <lat_max_deg> <out_dir>
#
# Example:
#   ./scripts/download_copernicus.sh 30 80 25 95 35 ./data/download

res_choice="${1:?resolution (30 or 90)}"
lon_min="${2:?lon_min_deg (int)}"
lat_min="${3:?lat_min_deg (int)}"
lon_max="${4:?lon_max_deg (int)}"
lat_max="${5:?lat_max_deg (int)}"
out_dir="${6:?out_dir}"

if (( lon_min > lon_max )); then echo "ERROR: lon_min_deg ($lon_min) > lon_max_deg ($lon_max)" >&2; exit 2; fi
if (( lat_min > lat_max )); then echo "ERROR: lat_min_deg ($lat_min) > lat_max_deg ($lat_max)" >&2; exit 2; fi

case "$res_choice" in
  30) BUCKET="copernicus-dem-30m"; RES_ARCSEC=10 ;;
  90) BUCKET="copernicus-dem-90m"; RES_ARCSEC=30 ;;
  *)  echo "ERROR: resolution must be 30 or 90 (meters)" >&2; exit 2 ;;
esac

mkdir -p "$out_dir"

# Concurrency (override: CURL_MAX=32 ./script ...)
CURL_MAX="${CURL_MAX:-16}"

# Curl robustness (override if you want)
CURL_RETRY="${CURL_RETRY:-3}"
CURL_RETRY_DELAY="${CURL_RETRY_DELAY:-2}"

fmt_lat() {
  local v="$1" hemi="N"
  if (( v < 0 )); then hemi="S"; v=$(( -v )); fi
  printf "%s%02d_00" "$hemi" "$v"
}
fmt_lon() {
  local v="$1" hemi="E"
  if (( v < 0 )); then hemi="W"; v=$(( -v )); fi
  printf "%s%03d_00" "$hemi" "$v"
}

manifest="$out_dir/manifest_${res_choice}m_$(date +%Y%m%d_%H%M%S).txt"
: > "$manifest"

echo "bucket     : $BUCKET"
echo "resolution : ${res_choice}m (arcsec=${RES_ARCSEC})"
echo "tiles      : lon ${lon_min}..${lon_max}, lat ${lat_min}..${lat_max}"
echo "out_dir    : $out_dir"
echo "parallel   : curl --parallel-max ${CURL_MAX}"
echo "manifest   : $manifest"
echo

# Build curl args in-memory (no temp files).
# For cached files: add directly to manifest and skip.
declare -a CURL_ARGS=()

for ((y=lat_min; y<=lat_max; y++)); do
  for ((x=lon_min; x<=lon_max; x++)); do
    northing="$(fmt_lat "$y")"
    easting="$(fmt_lon "$x")"
    base="Copernicus_DSM_COG_${RES_ARCSEC}_${northing}_${easting}_DEM"
    out="${out_dir}/${base}.tif"

    if [[ -f "$out" ]]; then
      echo "HIT  : $base"
      echo "$out" >> "$manifest"
      continue
    fi

    url="https://${BUCKET}.s3.amazonaws.com/${base}/${base}.tif"
    echo "GET  : $base"
    CURL_ARGS+=("$url" "-o" "$out")
  done
done

# Nothing to fetch
if (( ${#CURL_ARGS[@]} == 0 )); then
  echo
  echo "done. downloaded 0 tiles (all cached)"
  echo "manifest written to: $manifest"
  exit 0
fi

# Download with curl's native parallelism.
# -f: fail on HTTP errors (404/403)
# -L: follow redirects (harmless here)
# -C -: resume partial
# Retries for transient failures
set +e
curl -fL -C - \
  --parallel --parallel-max "$CURL_MAX" \
  --retry "$CURL_RETRY" --retry-delay "$CURL_RETRY_DELAY" --retry-connrefused \
  "${CURL_ARGS[@]}"
rc=$?
set -e

# Manifest: record files that actually exist after the run
# (covers misses / failures without complicated parsing)
find "$out_dir" -maxdepth 1 -type f -name 'Copernicus_DSM_COG_*.tif' -print | sort >> "$manifest"

echo
echo "done. downloaded $(wc -l < "$manifest" | tr -d ' ') tiles (manifest may include cached)"
echo "manifest written to: $manifest"

exit "$rc"
