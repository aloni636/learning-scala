#!/usr/bin/env bash
set -euo pipefail

mkdir -p data/taxi

base=https://d37ci6vzurychx.cloudfront.net

# Concurrency (override: CURL_MAX=32 ./script ...)
CURL_MAX="${CURL_MAX:-16}"

# See https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

# Build curl arguments for parallel downloads
curl_args=()
for month in {01..05}; do
   output_file="data/taxi/yellow_tripdata_2023-$month.parquet"
   if [[ ! -f "$output_file" ]]; then
      curl_args+=(-o "$output_file")
      curl_args+=("$base/trip-data/yellow_tripdata_2023-$month.parquet")
   fi
done

# Add taxi zone lookup file
lookup_file="data/taxi/taxi_zone_lookup.csv"
if [[ ! -f "$lookup_file" ]]; then
   curl_args+=(-o "$lookup_file")
   curl_args+=("$base/misc/taxi_zone_lookup.csv")
fi

# Execute parallel download only if there are files to download
if [[ ${#curl_args[@]} -gt 0 ]]; then
   curl -L --parallel --parallel-max "$CURL_MAX" "${curl_args[@]}"
else
   echo "All files already exist, skipping download."
fi
