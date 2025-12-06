// scala 2.13
package hello
import hello.Exercise

/*
  ---

  ### 4. Minimal concurrency

  You do not need heavy FP here; just enough to not fear `Future`:

 * `Future`, `ExecutionContext`, `map`/`flatMap` on `Future`.
 * When writing GeoTrellis + Spark jobs you’ll mostly rely on Spark’s own parallelism, but `Future` does appear in tooling and orchestrating code.

  Exercise:

 * Wrap a small blocking I/O call (e.g. reading a file) in a `Future` and sequence two of them with a `for`-comprehension.
 */

/*
  ---

  ### 5. GeoTrellis core (single-JVM, no Spark)

  Goal: be fluent with the basic data model and raster operations.

  Focus concepts:

 * `Extent`, `CRS`, `Raster[Tile]`, `MultibandTile`, `ProjectedRaster`.
 * Reading/writing GeoTIFFs (`SinglebandGeoTiff`, `MultibandGeoTiff`).
 * Local operations (map algebra on cell values).
 * Focal operations (filters, slope, hillshade), reclassification, resampling.
 * Reprojection between CRSs.

  Suggested path:

 * Add GeoTrellis core deps to your sbt project (`geotrellis-raster`, `geotrellis-vector`, `geotrellis-proj4`, `geotrellis-gdal` or GeoTIFF module).
 * In a tiny `Main` object:

 * Read a GeoTIFF into `SinglebandGeoTiff`.
 * Inspect `extent`, `crs`, `tile.cols`, `tile.rows`.
 * Run a simple local op: “set all negative values to NODATA”, or normalize values.
 * Write the result to a new GeoTIFF.

  This keeps you in pure Scala, no Spark yet, so the cognitive load stays low.
 */

/*
  ---

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
  ---

  ### 7. Project structure for your real pipeline

  Once the above is comfortable, structure your repo so learning scales:

 * `core/` module:

 * Pure Scala + GeoTrellis code, no Spark; types for DEM tiles, domain logic, utilities.
 * `spark/` module:

 * Spark/GeoTrellis integration, IO, tiling, catalog interactions.
 * `apps/`:

 * Tiny CLI entrypoints (`Main` objects) that stitch `core` and `spark` together.
 * `experiments/`:

 * Your playground: small `Main` or Scala worksheets mirroring the stuff you did in Python notebooks.

  This keeps Scala learning aligned with your actual goal instead of abstract exercises.
 */

/*
  ---

  If you want, next step I can sketch a concrete `build.sbt` and a minimal `Main.scala` that reads one GeoTIFF with GeoTrellis core and then the “same thing, but tiled on Spark” as a second app.

 */

/*
  .apply() method, like __call__ method in Python, is used to make a fundamentally OOP runtime support functions.
  Functions are syntactic sugar over objects with custom method call
  In scala2 functions can have up to 22 parameters because they are implemented as a trait Function<parameter count>, and they implemented up to Function22
 */

/*
  Questions:
  - What's the difference between traits and abstract objects, and why one support multiple inheritance while the other does not?
  - how lazy val works?
 */

object Main {
  def main(args: Array[String]): Unit = {
    val exercises: Map[String, Exercise] = List(
      hello.Ex01,
      hello.Ex02,
      hello.Ex03
    ).map(x => x.name -> x).toMap
    val exerciseName = args.headOption.getOrElse("").strip()
    exercises.get(exerciseName) match {
      case Some(ex) => ex.run()
      case None     => {
        println(
          s"Unknown exercise name '${exerciseName}'. Available exercises: [${exercises.keys.mkString(", ")}]"
        )
      }
    }
  }
}
