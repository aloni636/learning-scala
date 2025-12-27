package hello

/*
  ### 6. GeoTrellis + Spark

  Goal: understand the “tiled layer on Spark” model, because this is where your distributed GIS happens.

  Key abstractions:

 * `RDD[(K, V)]` layers where `K` is `SpatialKey` or `SpaceTimeKey`.
 * `TileLayerMetadata[K]` and `ContextRDD[K, V, TileLayerMetadata[K]]`.
 * `LayoutDefinition`, `ZoomedLayoutScheme`.
 * Tiling and retiling, pyramids, reading/writing from a catalog (e.g. S3 / HDFS).

  Suggested flow:

  1. Add spark + GeoTrellis spark modules to your sbt project (Spark 3.3 + 2.13).

  2. In `Main`:

 * Create a `SparkSession`.
 * Use `GeoTiffRDD` or similar helpers to read many GeoTIFF tiles into an RDD.
 * Tile them to a `TileLayerRDD[SpatialKey]`.
 * Run a simple local op across the whole layer (e.g. multiply all cells by 2).
 * Write out to a catalog or back to GeoTIFFs.

  3. Then, start mapping this to your future cost-distance / road-age / DEM work:

 * Think “DEM as `TileLayerRDD[SpatialKey]`”.
 * Think “OSM roads as vector layer, rasterized onto same layout”.
 */

/*
B. Regions (tile-friendly, Spark-justifying)

These were chosen to (a) have strong terrain signal and (b) justify tiling + distributed processing without cloud-scale storage.

1. Himalayas (excellent default)

   - Lon: `80 .. 95`
   - Lat: `25 .. 35`
   - ~150 tiles
   - Extreme relief → slopes, viewshed, cost-distance all become meaningful.

2. Andes (clean alternative, southern hemisphere)

   - Lon: `-80 .. -70`
   - Lat: `-20 .. 0`
   - Similar tile count
   - Long continuous mountain chains, fewer data quirks.

3. Alps (smaller but dense)

   - Lon: `5 .. 15`
   - Lat: `44 .. 48`
   - Fewer tiles, but very high signal density.
   - Good if you want faster iteration with real terrain complexity.
 */

object Ex06 extends Exercise {
  override def run(): Unit = {}

}
