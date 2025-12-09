package hello
import geotrellis.raster._
import geotrellis.raster.{mapalgebra => M}
import geotrellis.raster.io.geotiff._
import pprint.pprintln
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
  override def run(): Unit = {
    // See: https://github.com/locationtech/geotrellis/blob/master/docs/raster/ReadingGeoTiffs.md
    val everest = SinglebandGeoTiff(
      "../data/Everest_COP30.tif"
    )

    def describeRaster(tiffRaster: GeoTiff[Tile]): Unit = {
      // (alignment, reprojection, vector overlay)
      println(
        "\n+---- Projection & Georeferencing-----"
      )
      println(s"| CRS: ${tiffRaster.crs}")
      println(s"| Extent: ${tiffRaster.extent}")
      println(s"| Columns: ${tiffRaster.cols}")
      println(s"| Rows: ${tiffRaster.rows}")
      println(s"| Cell size: ${tiffRaster.cellSize}")
      // Equivalent to rasterExtent

      // (local ops, focal ops, nodata behavior)
      println(
        "\n+---- Pixel Schema & Data Semantics -----"
      )
      println(s"| CellType: ${tiffRaster.tile.cellType}")
      println(s"| Statistics: " + (tiffRaster.tile.statistics match {
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
      println("| NoData value: " + (tiffRaster.tile.cellType match {
        case c: HasNoData[_] => c.noDataValue
        case _               => "<all_valid>"
      }))

      // (performance, pyramids, streaming relevance)
      println(
        "\n+---- Storage & I/O Characteristics -----"
      )
      println(s"| StorageMethod: ${tiffRaster.options.storageMethod}")
      println(s"| Compression: ${tiffRaster.options.compression}")
      println(s"| Overviews count: ${tiffRaster.overviews.length}")
      println(
        s"| Overviews dimensions: ${tiffRaster.overviews.map(o => "${o.dimensions}").mkString(", ")}"
      )

      // (sanity check before heavy ops)
      println("\n+---- Raw Tile Shape -----")
      println(
        s"| Tile cols: ${tiffRaster.tile.cols}, Tile rows: ${tiffRaster.tile.rows}"
      )
      println("")
    }
    describeRaster(everest)
  }

}
