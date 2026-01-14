// NOTE: `import geotrellis.spark.store.hadoop._` is used to extend SparkContext with .hadoopGeoTiffRDD()

package learningscala
import geotrellis.layer._
import geotrellis.proj4.CRS
import geotrellis.proj4.util.UTM
import geotrellis.raster.{Tile, _}
import geotrellis.raster.{mapalgebra => M}
import geotrellis.spark._
import geotrellis.spark.store.hadoop._
import geotrellis.vector.ProjectedExtent
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import pprint.pprintln
/*
  ### 7. Distributed, Spark based, raster exercises

  Goal: build intuition for raster-as-distributed-data, not just API usage.
  Focus on tiled computation, global aggregation, and layout-aware algorithms.

  All exercises assume:
  - DEM loaded as `TileLayerRDD[SpatialKey]`
  - Fixed `LayoutDefinition`
  - Explicit awareness of tile boundaries and shuffles
 */

object Ex07 extends SparkExercise {
  val programs = Seq(
    "Ex07RddProgram"
  )
}

object Ex07RddProgram extends SparkProgram {
  // I used this expression-block / function to diagnose
  // `java.lang.NoSuchMethodError: 'spire.math.IntIsIntegral spire.math.Integral$.IntIsIntegral()'`
  // error to figure out if there's a race between sbt-assembly provided Spire library
  // and Spark's MLib provided Spire.
  // See build.sbt's ShadeRules
  def diagnoseSparkRuntimeClasspathRace(sc: SparkContext): Unit = {
    // `?` is an "existential type", a wildcard type capturing all types
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
  }
  override def application(
      spark: SparkSession,
      sc: SparkContext,
      log: Logger
  ): Unit = {
    // diagnoseSparkRuntimeClasspathRace(sc)
    val TIFF_PATH =
      "file:///workspaces/learning-scala/data/himalayas/Copernicus_DSM_COG_10_N25_00_E081_00_DEM.tif"

    // NOTE: sc.hadoopGeoTiffRDD reads TIFF in 256x256 tiles, regardless of the TIFF internal tiling
    //       As for Copernicus, it's TIFF tiling is 1024 for GLO-30,
    //       meaning geotrellis will chop it into smaller tiles to increase parallelism
    //       To force geotrellis to create bigger RDD tiles, define maxTileSize explicitly:
    // val rdd = HadoopGeoTiffRDD.spatial(
    //   new Path(TIFF_PATH),
    //   HadoopGeoTiffRDD.Options(maxTileSize = Some(1024))
    // )(sc)
    val rdd: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD(TIFF_PATH)
    val count = rdd.count()
    val (pe, tile) = rdd.first()

    println("### DEM RDD Metadata ###")
    println(s"[RDD] count=$count")
    println(s"[1st] CRS: ${pe.crs}")
    println(
      s"[1st] Columns: ${tile.cols} | Rows: ${tile.rows} | Cell type: ${tile.cellType}"
    )
    println(s"[1st] Extent: ${pe.extent}")
    println()

    // From docs: "FloatingLayoutScheme will discover the native resolution and extent and partition it by given tile size without resampling"
    // We use SpatialKey instead of SpaceTimeKey because Copernicus is not spatiotemporal
    val layout = FloatingLayoutScheme(tileSize = 256)
    val (zoom, metadata) = rdd.collectMetadata[SpatialKey](
      layout
    ) // basically does rdd.map() followed by rdd.reduce() to merge the tiff metadata of each tile
    val layer = rdd.tileToLayout[SpatialKey](metadata)

    // Print metadata information
    println("### DEM Layer Metadata ###")
    println(
      s"Zoom level: $zoom | Cell type: ${layer.metadata.cellType}"
    )
    // NOTE: Width and Height are in CRS units, and for EPSG:4326 those are angular offsets per cell/pixel
    println(
      s"CRS: ${layer.metadata.crs} | Cell width: ${layer.metadata.cellwidth} | Cell height: ${layer.metadata.cellheight}"
    )
    pprintln(layer.metadata.extent)
    pprintln(layer.metadata.bounds)
    pprintln(layer.metadata.layout)

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

    val projectedRdd = rdd.map { case (pe, raster) =>
      (
        pe,
        raster.reproject(
          pe.extent,
          pe.crs,
          // NOTE: I assume `pe` is of EPSG:4326, which it is (as of all Copernicus COGs)
          UTM.getZoneCrs(
            lon = pe.extent.center.getX(),
            lat = pe.extent.center.getY()
          )
        )
      )
    }
    val aspectRdd = projectedRdd.mapValues { raster =>
      raster.mapTile(tile => tile.aspect(raster.cellSize))
    }
    val slopeRdd = projectedRdd.mapValues { raster =>
      raster.mapTile(tile => tile.slope(raster.cellSize))
    }

    val SLOPE_CLASSES = Array(5, 15, 30)
    val slopeClassRdd = slopeRdd.mapValues { raster =>
      raster.mapTile(tile =>
        tile.mapDouble(c => {
          // `java.util.Arrays.binarySearch` returns:
          // [+] positive array index for exact matches, and
          // [-] negative range index for inexact matches
          val i = java.util.Arrays.binarySearch(SLOPE_CLASSES, c.toInt)
          if (i >= 0) i else -i - 1
        })
      )
    }
    val histogram = slopeClassRdd
      .map { case (_, raster) =>
        raster.tile.histogram
      }
      .reduce { case (a, b) => a.merge(b) }

    println("### Slope RDD Statistics ###")
    val slopeClassStrings = {
      val strings = SLOPE_CLASSES.map(_.toString())
      strings.prepended("0").zip(strings.appended("Inf")).map { case (a, b) =>
        s"${a}-${b}"
      }
    }
    println(
      histogram
        .binCounts()
        .sortBy { case (bin, count) => bin }
        .map { case (bin, count) =>
          s"${slopeClassStrings(bin)}: ${count}"
        }
        .mkString("\n")
    )

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
  }
}
