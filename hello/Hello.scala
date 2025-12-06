// scala 2.13
import scala.io
import java.{io => jio}
import java.io.{File, FileNotFoundException}
import HelloWorld.NeighborhoodMode.Plus
import HelloWorld.NeighborhoodMode.Box
import java.time.{LocalDate, Duration, Period}
import java.time.format.DateTimeParseException
import java.time.temporal.ChronoUnit
import com.github.tototoshi.csv._
import org.slf4j.LoggerFactory
import pprint.pprintln

object HelloWorld extends App {
  /*
  ---
  ### 0. Tooling comfort (you are mostly here already)

  Goal: be fluent with your devcontainer + sbt.

   * Use:

   * `sbt run`, `sbt test`, `sbt console` (REPL), `sbt clean compile`.
   * Metals: hover types, go-to-definition, “Run / Test” code lens, worksheets (`.sc`).
   * Tiny exercise: open `sbt console` and play with basic expressions and imports:

   * `import scala.util.Try`
   * `List(1,2,3).map(_ * 10)`
   */

  println(List(1, 2, 3).map(_ * 10))

  /*
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
   */

  val meaningOfLife: Int = 42
  val outString: String = {
    s"Hello, world!\nThe meaning of life is: ${meaningOfLife}"
  }
  println(outString)

  var binaryUnion: Either[Boolean, Int] =
    if (meaningOfLife < 42) Left(false) else Right(1)

  binaryUnion = binaryUnion match {
    case Left(bool) => Right(2)
    case Right(int) => Left(true)
  }

  binaryUnion match {
    case Left(x)  => println("Disjoint type is Boolean")
    case Right(x) => println("Disjoint type is Int")
    case _        => println("WTF did you do?!")
  }

  // NOTE: only def method support generics, as oppose to val method which can't
  def prettifyList[T](
      x: Iterable[T]
  ): String = {
    // NOTE: java.lang.UnsupportedOperationException on empty iterable
    // s"[${x.map(x => x.toString()).reduce((a, b) => a + "," + b)}]"
    // correct versions:
    // s"[${x.map(x => x.toString()).reduceOption((a, b) => a + "," + b).getOrElse("")}]"
    x.map(x => x.toString()).mkString("[", ",", "]")
  }

  val someList = List(1, 2, 3)
  val summation = someList.reduce((a, b) => a + b)
  println(s"Sum of ${prettifyList(someList)} is $summation")

  print("Enter a list of numbers delimited by ',': ")
  if (false) {
    val userNumbers = io.StdIn.readLine()
    val parsedNumbers =
      userNumbers
        .split(",")
        .map(_.strip().toIntOption)
        .collect({ case Some(x) => x })
    println(prettifyList(parsedNumbers))
  }

  // Case Classes
  // Companionship is between class and object definitions sharing the same name.
  // This separates instance level methods & attributes from class level methods and attributes.
  // This works because objects are singletons, meaning (by definition) class level, while classes work at the instance level

  // Sealed Trait & Case Objects
  // - `sealed` means that inheritance is *sealed* to the current file, allowing scala to verify at compile time exhaustive pattern matching
  // - `case object` means a singleton with additional boilerplate to be used in a pattern matching statement
  // - More generally, `case` is used to denote that a class or object can participate in scala's advanced pattern matching (which use the `case` keyword), including `unapply` and other syntax features
  sealed trait NeighborhoodMode
  object NeighborhoodMode {
    case object Plus extends NeighborhoodMode
    case object Box extends NeighborhoodMode
  }

  case class TileId(col: Int, row: Int) {
    def neighbors(mode: NeighborhoodMode): List[TileId] = {
      mode match {
        case Plus =>
          List(
            TileId(col + 1, row),
            TileId(col - 1, row),
            TileId(col, row + 1),
            TileId(col, row - 1)
          )
        case Box =>
          List(
            TileId(col + 1, row),
            TileId(col - 1, row),
            TileId(col, row + 1),
            TileId(col + 1, row + 1),
            TileId(col - 1, row + 1),
            TileId(col, row - 1),
            TileId(col + 1, row - 1),
            TileId(col - 1, row - 1)
          )

      }
    }
  }

  def asGrid(tileIds: List[TileId]): String = {
    val minCol = tileIds.minBy(_.col).col
    val minRow = tileIds.minBy(_.row).row

    val normalized =
      tileIds.distinct.map(x => x.copy(x.col - minCol, x.row - minRow))

    val maxCol = normalized.maxBy(_.col).col
    val maxRow = normalized.maxBy(_.row).row

    // Build a grid of size (maxRow + 1) x (maxCol + 1)
    val grid = Array.fill(maxRow + 1, maxCol + 1)('□')
    normalized.foreach { tile =>
      grid(tile.row)(tile.col) = '■'
    }

    grid.map(_.mkString).mkString("\n")
  }

  val tileId = TileId(1, 2)
  val neighbors = tileId.neighbors(NeighborhoodMode.Plus)
  println(s"Plus footprint neighbors for tile ${tileId} are: ${neighbors}")

  println(s"Neighbors as grid:\n${asGrid(neighbors)}")

  /* TODO: Implement an infix notation, scala-valid SQL subset expression language
  // Object Orientation
  // SqlExpression
  // +- select[columns]
  //    SelectExpression
  //    +- from[table]
  //       FromExpression
  //       +- where[column]
  //          Column[ == Column]
  class SqlExpression {
    def select(columns: String) = {}
  }
  class SelectExpression {
    def from(table: String) = {}
  }
  class Col(val name: String) {
    private var expression = ""
    def +(column: Col) = {}
    def ==(column: Col) = { _expression += "" }
  }
  class FromExpression {
    def where(column: Col) = {}
  }

  sql select "*" from "abc" where Col("A") == 2
   */

  println()

  /*
  ---

  ### 2. Collections + functional style (this is your daily driver)

  Goal: be very comfortable with immutable collections and `map`/`flatMap` chains - this is how GeoTrellis, Spark, and all the glue code feel.

  Focus topics:

   * `List`, `Vector`, `Map`, `Seq`.
   * `map`, `flatMap`, `filter`, `groupBy`, `foldLeft`, `reduceOption`.
   * `Option`, `Either`, `Try` and pattern matching on them.

  Exercises:

   * Load a small CSV/text file with `scala.io.Source` and:

   * Split to lines, map to a case class `Record`.
   * Group by some field and compute an aggregate (mean, max, histogram).
   */

  private val log = LoggerFactory.getLogger(getClass())

  case class Customer(
      Index: Int,
      CustomerId: Long,
      FirstName: String,
      LastName: String,
      Company: String,
      City: String,
      Country: String,
      Phone1: String,
      Phone2: String,
      Email: String,
      SubscriptionDate: LocalDate,
      Website: String
  )
  // QUESTION: Why we had to use new here but not in List()
  // ANSWER: Because List has a companion singleton List instance responsible for
  //         instantiating List instances via static constructor wrappers
  println(s"Current working directory: ${new File(".").getAbsoluteFile()}")
  val csvFile = new File("../data/customers-100.csv")
  if (!csvFile.exists())
    throw new FileNotFoundException(
      s"Couldn't find ${csvFile}\nMake sure to download it from https://github.com/datablist/sample-csv-files"
    )
  val reader = CSVReader.open(csvFile)
  val records = reader.allWithHeaders()

  val someCustomers: List[Option[Customer]] = records.map { record =>
    try
      Some(
        Customer(
          Index = record("Index").toInt,
          CustomerId = java.lang.Long.parseLong(record("Customer Id"), 16),
          FirstName = record("First Name"),
          LastName = record("Last Name"),
          Company = record("Company"),
          City = record("City"),
          Country = record("Country"),
          Phone1 = record("Phone 1"),
          Phone2 = record("Phone 2"),
          Email = record("Email"),
          SubscriptionDate = LocalDate.parse(record("Subscription Date")),
          Website = record("Website")
        )
      )
    catch {
      case e @ (_: NumberFormatException | _: DateTimeParseException |
          _: NoSuchElementException) => {
        log.warn(
          s"Failed to parse record due to ${e.getClass().getName()}: ${record}"
        )
        None
      }
      case e: Throwable => {
        log.error(
          s"Unexpected exception during customer record parsing: ${e.getClass().getName()}"
        )
        throw e
      }
    }
  }
  val customers = someCustomers.flatten
  println(
    s"Parsed ${someCustomers.length} CSV records into ${customers.length} customer entries"
  )
  val customersByCompany = customers.groupMapReduce(_.Company)(_ => 1)(_ + _)
  println("Consumers per company:")
  pprintln(customersByCompany)

  // fan-out based count rollup: we explode each item into all levels of the hierarchy, and then aggregate count
  // NOTE: Seq() is a trait which is implemented in Scala 2.13 using List
  val customersByLocation = customers
    .flatMap(x =>
      Seq(
        (x.Country, None),
        (x.Country, Some(x.City))
      )
    )
    .groupMapReduce(identity)(_ => 1)(_ + _)
  println(s"Fan-out based location based customer count:")
  customersByLocation.toList
    .sortBy(_._2)
    // NOTE: case is used for tuple unpacking, and is the preferable way to handle tuples/products
    .foreach { case ((country, cityOption), count) =>
      println(s"${(country +: cityOption.toList).mkString(" / ")} -> ${count}")
    }

  val subscriptionTimeDifference =
    customers
      .map(_.SubscriptionDate)
      .sortBy(identity)
      .sliding(2)
      .map(x =>
        (
          Period.between(x(0), x(1)), // calender aware time difference
          ChronoUnit.DAYS.between(x(0), x(1)) // total days difference
        )
      )
      .toList
  // See: https://stackoverflow.com/a/40487511
  val regexPattern = """\d+[YMD]""".r
  val prettyDifferences = subscriptionTimeDifference
    .sortBy(x => x._2)
    .map { case (cal, total) =>
      val totalPlural = if (total == 1) "" else "s"
      val periodItems = regexPattern.findAllIn(cal.toString()).toList
      periodItems
        .map { item =>
          val numeric = item.dropRight(1).toInt
          val calPlural = if (numeric == 1) "" else "s"
          val unit = item.last
          numeric.toString + (unit match {
            case 'Y' => " Year"
            case 'M' => " Month"
            case 'D' => " Day"
          }) + calPlural
        }
        .mkString(", ") + s" (Total: ${total} Day" + totalPlural + ")"
    }
  println("Subscription time differences, ordered by max difference:")
  pprintln(prettyDifferences)

  /*
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
   */

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

}
