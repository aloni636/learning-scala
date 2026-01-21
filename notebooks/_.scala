// ---
// jupyter:
//   jupytext:
//     text_representation:
//       extension: .scala
//       format_name: percent
//       format_version: '1.3'
//       jupytext_version: 1.19.0
//   kernelspec:
//     display_name: Scala
//     language: scala
//     name: scala
// ---

// %% [markdown]
// # Dependencies & Utilities

// %%
import $cp.`/workspaces/learn-scala/hello/target/scala-2.13/classes`
import $ivy.`org.locationtech.geotrellis::geotrellis-raster:3.8.0`

// %%
import java.nio.file.{Files, Paths}
import java.util.Base64
import almond.display._

def showPng(path: String, maxWidthPx: Int = 900): Html = {
  val bytes = Files.readAllBytes(Paths.get(path))
  val b64 = Base64.getEncoder.encodeToString(bytes)
  Html(s"""<img src="data:image/png;base64,$b64" style="max-width:${maxWidthPx}px;height:auto;" />""")
}

// showPng("/workspaces/learn-scala/visualizations/everest.png", maxWidthPx = 800)

// %%
import geotrellis.raster._
import almond.display._
import java.util.Base64

def showTile(
  tile: Tile,
  ramp: render.ColorRamp = ColorRamps.Viridis,
  breaks: Int = 256,
  noDataColor: Int = 0x00000000,   // transparent
  maxWidthPx: Int = 900
): Html = {
  val hist = tile.histogramDouble(breaks)
  val cm = ColorMap.fromQuantileBreaks(hist, ramp, ColorMap.Options(noDataColor = noDataColor))

  val pngBytes = tile.renderPng(cm).bytes
  val b64 = Base64.getEncoder.encodeToString(pngBytes)

  Html(s"""<img src="data:image/png;base64,$b64" style="max-width:${maxWidthPx}px;height:auto;" />""")
}


// %% [markdown]
// ---

// %% [markdown]
// # Local GeoTrellis Interactive Analysis
// Was used in conjunction with `Ex05.scala` for exploring GeoTrellis raster capabilities.

// %%
import geotrellis.raster._
import geotrellis.proj4.util.UTM
import geotrellis.raster.io.geotiff._
import geotrellis.proj4._
import geotrellis.raster.reproject._
// For GeoTrellis logging
import org.slf4j.{Logger, LoggerFactory}

// %%
val RESAMPLING_METHOD = ResampleMethods.Bilinear

// %%
val everestTiff = SinglebandGeoTiff(
      "../data/Everest_COP30.tif"
    )

// %%
val utmCrs: CRS = UTM.getZoneCrs(
      lon = everestTiff.extent.center.getX(), // X is always the HORIZONTAL axis of the CRS
      lat = everestTiff.extent.center.getY() // Y is always the VERTICAL axis of the CRS
    )
  
val everest = everestTiff.projectedRaster.reproject(
      utmCrs,
      Reproject.Options.methodToOptions(RESAMPLING_METHOD)
    )

// %%
showTile(everest.tile)

// %%
import geotrellis.raster._
import geotrellis.raster.{isData, isNoData}
import geotrellis.raster.io.geotiff._
import geotrellis.proj4._
import geotrellis.raster.reproject._
import geotrellis.proj4.util.UTM
import geotrellis.raster.histogram._
import geotrellis.raster.mapalgebra.focal.Slope
import geotrellis.raster.regiongroup.RegionGroupOptions
val log: Logger = LoggerFactory.getLogger(getClass())

// NOTE: I use `GeoTiff.mapTile` / `ProjectedRaster.mapTile` / `GeoTiff.copy(tile = ...)`
// to avoid accessing deeply nested geotrellis objects properties each time I want to do some operation
val slopes = everest.mapTile(t => t.slope(everest.cellSize))
// visualizeTile(everest, "./visualizations/everest")
// visualizeTile(slopes, "./visualizations/slopes")

/* Compute all slope based islands which are at least 70% visible from the highest point in mount everest
  * - Local, focal, zonal & global operations:
  * - Focal: slopes
  * - Iterative-Focal-Propagation: viewshed (ray wavefront expansion, resembling Dijkstra wavefront but with max slope accumulation as cost metric)
  * - Zonal: regionGroup island creation, viewshed aggregation
  * - Local: merging viewshed with islands, filtering islands
  * - Global: reporting and debugging
  */
val MAX_SLOPE: Double = 15.0
val MIN_VISIBILITY_PERCENTAGE: Double = 70
val CONNECTIVITY_METHOD: Connectivity = FourNeighbors

log.info("Computing slopeRegions...")
val slopeRegions = slopes.mapTile(t =>
  t.mapDouble(x => if (isNoData(x) || x > MAX_SLOPE) Double.NaN else 1)
    .regionGroup(RegionGroupOptions(CONNECTIVITY_METHOD))
    .tile
)

log.info("Getting DTM max height index...")
val (maxCol, maxRow, maxZ) = {
  // NOTE: GeoTrellis does not provide functional fold operation over tiles
  var (maxCol, maxRow, maxZ) = (0, 0, 0.0)
  everest.tile.foreachDouble { (col: Int, row: Int, z: Double) =>
    if (z > maxZ) {
      maxCol = col
      maxRow = row
      maxZ = z
    }
  }
  (maxCol, maxRow, maxZ)
}
log.info(s"DTM max index: maxCol=${maxCol} maxRow=${maxRow} maxZ=${maxZ})")

log.info("Computing DTM viewshed...")
val viewshedRaster = everest.mapTile(t => t.viewshed(maxCol, maxRow, exact = false))

val viewshedRegions = viewshedRaster.mapTile(t=>t.zonalPercentage(slopeRegions))


// %%
showTile(viewshedRaster.tile)

// %%
showTile(viewshedRegions.tile)

// %%
viewshedRaster

// %%
viewshedRaster.tile.zonalHistogramDouble(slopeRegions)
