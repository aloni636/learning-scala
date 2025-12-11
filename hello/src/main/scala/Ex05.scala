package hello
// Geotrellis
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.proj4._
import geotrellis.raster.reproject._
import geotrellis.proj4.util.UTM
import geotrellis.raster.histogram._
// General purpose utilities
import org.slf4j.{Logger, LoggerFactory}
import pprint.pprintln
import geotrellis.raster.mapalgebra.focal.Slope
import cats.instances.unit

/*
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
object Ex05 extends Exercise {
  def describeTiff(src: GeoTiff[Tile]): Unit = {
    // (alignment, reprojection, vector overlay)
    println(
      "\n+---- Projection & Georeferencing-----"
    )
    println(s"| CRS: ${src.crs}")
    println(s"| Extent: ${src.extent}")
    println(s"| Columns: ${src.cols}")
    println(s"| Rows: ${src.rows}")
    println(s"| Cell size: ${src.cellSize}")
    // Equivalent to rasterExtent

    // (local ops, focal ops, nodata behavior)
    println(
      "\n+---- Pixel Schema & Data Semantics -----"
    )
    println(s"| CellType: ${src.tile.cellType}")
    println(s"| Statistics: " + (src.tile.statistics match {
      case None    => "<not_available>"
      case Some(s) =>
        "{ " + List(
          s"dataCells: ${s.dataCells}",
          s"mean: ${s.mean}",
          s"median: ${s.median}",
          s"mode: ${s.mode}",
          s"stddev: ${s.stddev}",
          s"zmin: ${s.zmin}",
          s"zmax: ${s.zmax}"
        ).mkString(", ") + " }"
    }))
    // https://github.com/locationtech/geotrellis/blob/7ec19811fa6230faeb531c185d627195ad7b1ec0/docs/guide/core-concepts.rst#working-with-cell-values
    println("| NoData value: " + (src.tile.cellType match {
      case c: HasNoData[_] => c.noDataValue
      case _               => "<all_valid>"
    }))

    // (performance, pyramids, streaming relevance)
    println(
      "\n+---- Storage & I/O Characteristics -----"
    )
    // NOTE: Tiled(256,256) means the TIFF block partitioning of the raster are of size 256x256,
    // NOT how geotrellis stores them in memory
    println(s"| StorageMethod: ${src.options.storageMethod}")
    println(s"| Compression: ${src.options.compression}")
    println(s"| Overviews count: ${src.overviews.length}")
    println(
      s"| Overviews dimensions: ${src.overviews.map(o => "${o.dimensions}").mkString(", ")}"
    )
    println(s"| Additional metadata:")
    println(s"|   - Tiff tags: ${src.tags}")
    println(s"|   - Subfile type: ${src.options.subfileType}")
    println(s"|   - Interleave Method: ${src.options.interleaveMethod}")

    // (sanity check before heavy ops)
    println("\n+---- Raw Tile Shape -----")
    println(
      s"| Tile cols: ${src.tile.cols}, Tile rows: ${src.tile.rows}"
    )
    println() // Empty padding line
  }

  val DISPLAY_MAX_WIDTH = 1200
  val DISPLAY_MAX_HEIGHT = 1200
  val RESAMPLING_METHOD = ResampleMethods.Bilinear

  def toTargetScale(cols: Int, rows: Int) = {}

  def visualizeTile(tile: Tile, name: String = "tile")(implicit
      log: Logger
  ): Unit = {
    // Downsample (i.e. don't make huge PNGs)
    val scale = math
      .min(
        // NOTE: toDouble is used to force floating division instead of int floor division
        DISPLAY_MAX_WIDTH.toDouble / tile.cols,
        DISPLAY_MAX_HEIGHT.toDouble / tile.rows
      )
      .min(1.0) // never upscale
    val (targetCols, targetRows) =
      ((tile.cols * scale).round.toInt, (tile.rows * scale).round.toInt)
    val smallTile =
      if (scale < 1) {
        log.info(
          s"Downsampling tile of size '${tile.dimensions}' to fit to max dimensions (DISPLAY_MAX_WIDTH=${DISPLAY_MAX_WIDTH},DISPLAY_MAX_HEIGHT=${DISPLAY_MAX_HEIGHT}) "
        )
        tile.resample(targetCols, targetRows, RESAMPLING_METHOD)
      } else tile

    // Define color-ramp (pretty visualizations)
    val hist = smallTile.histogramDouble()
    val ramp = ColorRamps.Viridis
    val colorMap =
      ColorMap.fromQuantileBreaks(hist, ramp)

    // Write to filesystem
    val filename: String = if (name.endsWith(".png")) name else s"${name}.png"
    log.info(
      s"Rendering tile of size '${smallTile.dimensions}' with ColorMap '${colorMap}'"
    )
    val png = smallTile.renderPng(colorMap)
    log.info(s"Writing visualization '${filename}' to filesystem")
    png.write(filename)
  }

  override def run(): Unit = {
    implicit val log: Logger = LoggerFactory.getLogger(getClass())

    // See: https://github.com/locationtech/geotrellis/blob/master/docs/raster/ReadingGeoTiffs.md
    // NOTE: Using the old (pre geotrellis-3) raster readers
    val everestTiff = SinglebandGeoTiff(
      "../data/Everest_COP30.tif"
    )
    val everestWgs = everestTiff.projectedRaster

    describeTiff(everestTiff)

    // project the raster to utm zone 45N
    // See: https://geotrellis.readthedocs.io/en/latest/guide/core-concepts.html#coordinate-reference-systems
    val utmCrs: CRS = UTM.getZoneCrs(
      // X is always the HORIZONTAL axis of the CRS
      // Y is always the VERTICAL axis of the CRS
      lon = everestTiff.extent.center.getX(),
      lat = everestTiff.extent.center.getY()
    )

    // NOTE: GeoTrellis reprojection entry points (depends on the abstraction level you’re holding):
    // 1) ProjectedRaster.reproject(targetCrs)     - CRS is embedded; recommended when coming from GeoTIFF or RasterSource.
    // 2) Raster.reproject(srcCrs, dstCrs)         - Generic raster; must supply both source and target CRS explicitly.
    // 3) GeoTiff.projectedRaster.reproject(...)   - Access reprojection via the embedded CRS on the GeoTIFF.
    // 4) RasterSource.reproject(targetCrs).read() - Reproject at the source level before materializing a Raster.
    log.info(
      s"Projecting Everest DEM from '${everestTiff.crs}' to '${utmCrs}'"
    )
    val everest = everestTiff.projectedRaster.reproject(
      utmCrs,
      Reproject.Options.methodToOptions(RESAMPLING_METHOD)
    )
    // NOTE: Converting from geographic coordinate system (GCS) to projected coordinate system (PCS)
    // oftentimes modify the tile dimensions (cols & rows) because GCS cells can span across
    // one or multiple PCS cells (that's the core reason why PCS projection introduces distortion)
    println("Reprojected raster dimensions metadata:")
    pprintln(everest.crs)
    pprintln(everest.rasterExtent)
    println()

    // NOTE: I use `GeoTiff.mapTile` / `ProjectedRaster.mapTile` / `GeoTiff.copy(tile = ...)`
    // to avoid accessing deeply nested geotrellis objects properties each time I want to do some operation
    val slopes = everest.mapTile(t => t.slope(everest.cellSize))
    visualizeTile(everest, "../visualizations/everest")
    visualizeTile(slopes, "../visualizations/slopes")
  }
}
