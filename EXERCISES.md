### 0. Tooling comfort (you are mostly here already)

Goal: be fluent with your devcontainer + sbt.

* Use:

  * `sbt run`, `sbt test`, `sbt console` (REPL), `sbt clean compile`.
  * Metals: hover types, go-to-definition, “Run / Test” code lens, worksheets (`.sc`).
* Tiny exercise: open `sbt console` and play with basic expressions and imports:

  * `import scala.util.Try`
  * `List(1,2,3).map(_ * 10)`

---

### 1. Core Scala syntax, but only what you need

Goal: be able to read and write idiomatic library-using code.

Focus topics:

* `val` vs `var`, type inference, `def` vs `val` methods.
* Case classes, pattern matching, companion objects.
* Traits vs classes, constructor parameters.
* For-comprehensions as sugar for `map`/`flatMap`.

Exercises (very small):

* Implement `case class TileId(col: Int, row: Int)` with a method `.neighbors: List[TileId]`.
* Write a function:

  ```scala
  def parseIntOpt(s: String): Option[Int] =
    try Some(s.toInt) catch { case _: NumberFormatException => None }
  ```

  and use it in a `for`-comprehension over a `List[String]`.

---

### 2. Collections + functional style (this is your daily driver)

Goal: be very comfortable with immutable collections and `map`/`flatMap` chains – this is how GeoTrellis, Spark, and all the glue code feel.

Focus topics:

* `List`, `Vector`, `Map`, `Seq`.
* `map`, `flatMap`, `filter`, `groupBy`, `foldLeft`, `reduceOption`.
* `Option`, `Either`, `Try` and pattern matching on them.

Exercises:

* Load a small CSV/text file with `scala.io.Source` and:

  * Split to lines, map to a case class `Record`.
  * Group by some field and compute an aggregate (mean, max, histogram).

---

### 3. Types, generics, and “implicits just enough”

Goal: understand library signatures and “why does this implicit not resolve”.

Focus topics (keep it pragmatic):

* Type parameters: `def f[A](xs: List[A]): Int = xs.size`.
* Traits with type parameters (`trait LayerReader[K, V]`).
* Implicits as:

  * “Values found automatically”: `implicit val ec = ExecutionContext.global`.
  * “Typeclass instances”: `implicit val ordering: Ordering[TileId] = ...`
* Context bounds syntax: `def f[A: Ordering](xs: List[A])`.

Exercise:

* Define a simple typeclass:

  ```scala
  trait Show[A] { def show(a: A): String }
  object Show {
    implicit val intShow: Show[Int] = a => s"Int($a)"
  }
  def printShow[A: Show](a: A): Unit =
    println(implicitly[Show[A]].show(a))
  ```

  and call it with an `Int`. Just to demystify the pattern you will see in libraries.

---

### 4. Minimal concurrency

You do not need heavy FP here; just enough to not fear `Future`:

* `Future`, `ExecutionContext`, `map`/`flatMap` on `Future`.
* When writing GeoTrellis + Spark jobs you’ll mostly rely on Spark’s own parallelism, but `Future` does appear in tooling and orchestrating code.

Exercise:

* Wrap a small blocking I/O call (e.g. reading a file) in a `Future` and sequence two of them with a `for`-comprehension.

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

---

If you want, next step I can sketch a concrete `build.sbt` and a minimal `Main.scala` that reads one GeoTIFF with GeoTrellis core and then the “same thing, but tiled on Spark” as a second app.
