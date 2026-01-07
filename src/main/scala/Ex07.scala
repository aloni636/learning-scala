// NOTE: `import geotrellis.spark.store.hadoop._` is used to extend SparkContext with .hadoopGeoTiffRDD()

package learningscala
import geotrellis.raster.Tile
import geotrellis.spark._
import geotrellis.spark.store.hadoop._
import geotrellis.vector.ProjectedExtent
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/*
  ### 7. Distributed, Spark based, raster exercises

  Goal: build intuition for raster-as-distributed-data, not just API usage.
  Focus on tiled computation, global aggregation, and layout-aware algorithms.

  All exercises assume:
  - DEM loaded as `TileLayerRDD[SpatialKey]`
  - Fixed `LayoutDefinition`
  - Explicit awareness of tile boundaries and shuffles
 */

/*
  A. Terrain-derived layers (local neighborhood ops)

  Purpose:
  - Understand cell neighborhoods
  - See how local ops scale cleanly across tiles

  Steps:
  1. Load DEM → `TileLayerRDD[SpatialKey]`
  2. Compute derived rasters:
     - slope
     - aspect
  3. Reclassify slope into bins (e.g. 0–5, 5–15, 15–30, 30+ degrees)
  4. Aggregate:
     - per-tile histograms
     - global histogram across all tiles

  Notes:
  - Neighborhood ops stay local to tiles
  - Global aggregation forces shuffle
  - Compare per-partition vs global results
 */

/*
  B. Zonal statistics (raster × vector)

  Purpose:
  - Exercise raster/vector alignment
  - Understand rasterization cost and layout constraints

  Data:
  - DEM raster
  - Polygon zones (admin areas, watersheds, etc.)

  Steps:
  1. Reproject vectors to DEM CRS
  2. Rasterize polygons to DEM layout
  3. For each zone compute:
     - mean elevation
     - elevation variance
     - max slope
  4. Rank zones by terrain roughness

  Notes:
  - CRS mismatch is the main failure mode
  - Rasterization must match layout exactly
  - Naïve approaches explode memory
 */

/*
  C. Cost surface + cost-distance

  Purpose:
  - Implement a global raster algorithm
  - Justify Spark beyond embarrassingly-parallel ops

  Concept:
  - Treat raster as weighted graph
  - Cell values represent traversal cost

  Steps:
  1. Build cost raster from slope
     - flat = low cost
     - steep = high cost
  2. Add constraints:
     - forbidden cells
     - altitude thresholds
  3. Select:
     - single source
     - many destinations
  4. Compute:
     - cost-distance surface
     - least-cost paths

  Notes:
  - Requires iterative expansion (Dijkstra / wavefront)
  - Driver coordination vs executor work becomes visible
  - Clear limits of Spark for graph-like raster algorithms
 */

/*
  D. Sampling strategy

  Purpose:
  - Separate algorithm validation from scale effects

  Approach:
  - Sample small spatial windows to:
    - debug CRS and layout
    - validate logic
  - Run full extent to:
    - observe shuffle cost
    - test executor memory limits
    - justify distributed execution

  Rule:
  - Sampling is for correctness
  - Full run is for system behavior
 */

object Ex07 extends SparkExercise {
  val programs = Seq(
    "Ex07RddProgram"
  )
}

object Ex07RddProgram extends SparkProgram {
  override def application(
      spark: SparkSession,
      sc: SparkContext,
      log: Logger
  ): Unit = {
    def where(c: Class[?]): String =
      Option(c.getProtectionDomain)
        .flatMap(pd => Option(pd.getCodeSource))
        .map(_.getLocation.toString)
        .getOrElse("<no code source>")

    println(
      "[Ex07RddProgram Probe] Driver spire.Integral from: " + where(
        classOf[spire.math.Integral[?]]
      )
    )

    sc.parallelize(Seq(1), 1).foreach { _ =>
      println(
        "[Ex07RddProgram Probe] Executor spire.Integral from: " + where(
          classOf[spire.math.Integral[?]]
        )
      )
    }

    val geoTiffPath =
      "/workspaces/learning-scala/data/himalayas/Copernicus_DSM_COG_10_N25_00_E081_00_DEM.tif"

    val rdd = sc.hadoopGeoTiffRDD(geoTiffPath)

    val count = rdd.count()
    val (pe, tile) = rdd.first()

    println(s"count=$count")
    println(s"crs=${pe.crs}")
    println(s"extent=${pe.extent}")
    println(s"cols=${tile.cols} rows=${tile.rows} cellType=${tile.cellType}")
  }

}
