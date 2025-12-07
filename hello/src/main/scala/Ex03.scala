package hello
import hello.Exercise
import org.slf4j.LoggerFactory
import org.slf4j.Logger
// NOTE: For some reason this makes Numeric types actually behave at the type system like numerics
import scala.math.Fractional.Implicits._
import java.io.File
import java.nio.file.Path
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.NotDirectoryException

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

object Ex03 extends Exercise {
  override def run(): Unit = {
    /*
    The four practical “types” of implicit things:
        1. Values → the things that get injected
        2. Parameters → the holes they fill
        3. Classes/Conversions → add methods / syntax
        4. Evidence → enforce type rules
     */

    // ===== Values & Parameter Implicits =====
    implicit val log: Logger = LoggerFactory.getLogger(getClass())
    // NOTE: Injecting implicit log allows the caller to customize the logger
    //       to be used in computeHistogram
    def computeHistogram(items: Iterable[Double], bins: Int)(implicit
        log: Logger
    ): Map[Double, Int] = {
      val min = items.min
      val max = items.max
      val width = (max - min) / bins

      log.info(s"min=$min, max=$max, width=$width")

      def toBin(x: Double): Double = {
        if (max == min) min
        else if (x == max) min + (bins - 1) * width
        else min + math.floor((x - min) / width) * width
      }
      items.groupBy(toBin).map { case (k, v) => k -> v.count(_ => true) }
    }

    // NOTE: Injecting implicit ordering allows the caller to
    //       modify the ordering of the displayed histogram
    def printHistogram(hist: Map[Double, Int], max: Double)(implicit
        ordering: Ordering[Double]
    ): Unit = {
      println("{")
      hist.toList.sortBy(_._1).appended((max, 0)).sliding(2).foreach {
        case current :: next :: Nil =>
          println(s"  (${current._1} -> ${next._1}): ${current._2}")
        case _ =>
      }
      println("}")
    }

    val aList = List(0, 1, 2, 3, 4, 5, 6, 7).map(_.toDouble)
    val hist = computeHistogram(aList, 3)
    println(hist)
    // NOTE: This will impact every Ordering requiring function from here on out
    implicit val ordering = Ordering[Double].reverse
    printHistogram(hist, aList.max)

    // ===== Implicit Methods =====
    // Instead of injecting values from outside scope, we inject class instance wrappers
    /*
    Only these two forms trigger syntax enrichment:
    1. implicit def A => B
    2. implicit class RichA(a: A) { ... }
     */

    /*
    Exercise 1 – Feel the “missing method → wrap receiver” conversion

    Goal: use an implicit conversion as syntax enrichment
    (i.e., the “wrap class instances with some other class” feeling)

    Decide on one or two convenience methods that you wish TileKey had, but you will not add them directly to TileKey.

    Example ideas (pick your own):

    neighbor(dx, dy): TileKey

    isAtSameZoom(other: TileKey): Boolean

    toStringShort: String like "z/col/row"

    Write some “desired” usage code in run() (or similar), before you implement any conversion:

    Something like:

    Create a few TileKey values.

    Call your convenience methods on them in a way that does not compile yet.

    Do not change the original TileKey definition.

    Your task:
    Make those method calls compile by introducing a suitable conversion that enriches TileKey with those methods, without changing the call sites and without modifying TileKey itself.

    The key feeling you want:

    “Compiler complains that the method doesn’t exist on TileKey”

    After your work, it suddenly does, even though the case class stayed dumb.

    As a final step, manually desugar one call in comments:

    Write the “real” call you believe the compiler is rewriting to
    (e.g. “at this point, it’s effectively Wrapper(tileKey).neighbor(…)”).

    This exercise is meant to force you into the “missing method → wrap receiver” mental model.
     */

    case class TileKey(col: Int, row: Int, zoom: Int)
    case class TileIndex(index: String)

    def neighbor(colOffset: Int, rowOffset: Int): TileKey = ???
    def isAtSameZoom(other: TileKey): Boolean = ???
    def toStringShort(tileKey: TileKey): String = ??? // String like "z/col/row"

    val tileKey1 = TileKey(1, 2, 3)
    val tileKey2 = TileKey(3, 1, 2)
    val tileKey3 = TileKey(1, 2, 1)
    val tileKey4 = TileKey(4, 2, 3)

    implicit class TileKeyOps(self: TileKey) {

      /** Get another TileKey in the same zoom level at an offset from the
        * current tile
        *
        * @param colOffset
        *   Columns offset from current TileKey
        * @param rowOffset
        *   Rows offset from current TileKey
        * @return
        */
      def neighbor(colOffset: Int, rowOffset: Int): TileKey =
        TileKey(colOffset + self.col, rowOffset + self.row, self.zoom)
      def isAtSameZoom(other: TileKey): Boolean = self.zoom == other.zoom

      /** @param tileKey
        * @return
        *   String like "z/col/row"
        */
      def toStringShort(): String =
        s"${self.zoom}z/${self.col}c/${self.row}r"

    }

    println(s"tileKey1.neighbor: ${tileKey1.neighbor(3, 4)}")
    println(
      s"TileKeyOps(tileKey1).isAtSameZoom (explicit desugar): ${TileKeyOps(tileKey1).isAtSameZoom(tileKey2)}"
    )
    println(
      "Current tile keys:\n" + List(
        tileKey1,
        tileKey2,
        tileKey3,
        tileKey4
      ).map("  " + _.toStringShort()).mkString(",\n")
    )

    /*
    Exercise 2 – Feel the “wrong type → convert argument” conversion

    Goal: use an implicit A => B conversion as a type adapter, not just syntax

    Keep using the same TileKey and TileIndex domain.

    Define one or two functions that are explicitly written in terms of TileIndex only.
    Important constraints:

    Their parameter types mention TileIndex, not TileKey.

    Their return types also mention TileIndex (or something else, but not TileKey).

    Conceptual ideas for such functions:

    “Simulate reading a tile from disk by its index”

    “Compute neighbors of an index in 1D” (just for fun)

    “Log or pretty-print a TileIndex”

    In your “usage” code, deliberately write calls that pass TileKey where a TileIndex is expected, without changing the function definitions.

    For example (in spirit, not exact code):

    Create a TileKey.

    Try to call your TileIndex-based function with that TileKey.

    Let it fail to compile first.

    Your task:
    Make those calls compile by defining a conversion from your “coordinate-like” type to the “index-like” type, without changing:

    the function signatures

    the call sites

    The feeling you want here:

    “This function says it takes TileIndex, but I’m passing TileKey, and the compiler figures it out.”

    Again, manually desugar one of those calls in comments:

    Show the conceptual rewrite:
    “At this point this is effectively myFunction(tileKeyToIndex(tileKey))”

    This exercise is meant to force you into the “value of type A is required, but I have B → apply A => B conversion” mental model.
     */
    def readFromDisk(tileStore: Path, tileIndex: TileIndex)(implicit
        log: Logger
    ): Array[Byte] = {
      if (!Files.isDirectory(tileStore))
        throw new NotDirectoryException("tileStore should be directory")
      val resolved = tileStore.resolve(tileIndex.index + ".tiff")
      val tileBytes = Files.readAllBytes(resolved)
      log.info(
        s"Loaded tile '${tileIndex.index}' as ${tileBytes.length} bytes into memory"
      )
      tileBytes
    }

    implicit def tileKeyToTileIndex(
        tileKey: TileKey
    )(implicit log: Logger): TileIndex = {
      val toHexPadded: Int => String = { x =>
        val hex = x.toHexString
        "0x" + "0".repeat(8 - hex.length()) + hex
      }
      val index =
        s"${toHexPadded(tileKey.col)}-${toHexPadded(tileKey.row)}-${toHexPadded(tileKey.zoom)}"
      log.info(s"Casting TileKey to TileIndex with index of $index")
      TileIndex(index)
    }

    val tileStore: Path = Paths.get("../data/")
    println(s"Read tileKey1 into disk: ${readFromDisk(tileStore, tileKey1)}")
    println(
      s"Read tileKey3 into disk (explicit desugar): ${readFromDisk(tileStore, tileKeyToTileIndex(tileKey3))}"
    )

    // ===== Implicit Traits And Typeclasses =====
    /*
    Exercise 1 – First typeclass: trait + companion + evidence

    Goal: feel what “evidence” means: “I won’t call you unless I can prove A has capability X”.

    Define a trait that represents “this type can be rendered as a tile ID string”.

    Concept (you choose exact name and method signature):

    Trait name idea: something like TileIdFormat[A]

    It should have one method that, given a value of type A, returns a String which you will treat as “tile id representation”.

    Example behaviors you might choose (just concepts):

    For TileKey, produce something like "z/col/row"

    For TileIndex, produce something like "idx=<value>"

    But you decide what the method is called and how it formats.

    Create a companion singleton object for that trait.

    In that object, define at least two implicit values:

    One instance of your trait for TileKey

    One instance for TileIndex

    The important part:
    they live inside the companion of the trait, so they are in implicit search scope.

    Define a generic function that prints a tile using a context bound:

    It should be parameterized on A

    It should require evidence that A has your formatting trait using a context bound syntax (the A: Something style)

    Inside the function, you must:

    use implicitly to summon your typeclass instance for A

    call its method to build the string

    print/log that string

    In your run() (or similar):

    Call this generic function with a TileKey

    Call it with a TileIndex

    Confirm:

    you don’t pass any formatter explicitly

    it still works (once you wired everything correctly)

    Then, break it on purpose:

    Call your function with some type that does not have an instance (e.g. Int or String)

    Observe the compile error about missing implicit evidence

    Read the error and try to map it in your head to:
    “Compiler couldn’t find evidence TileIdFormat[Int]”.

    This exercise is the “hello world” of:

    trait as typeclass

    implicit instances in a singleton companion

    implicitly as “fetch me the evidence that A supports this operation”
     */

    trait TileFormatter[A] {
      def format(a: A): String
    }
    // NOTE: This has to be companion of the trait, if not it would not participate when resolving TileFormatter for a concrete type
    object TileFormatter {
      // "Evidence provider" for Scala, specifying how to convert from concrete types to types necessary for `format`
      implicit val tileKey: TileFormatter[TileKey] = x => x.toStringShort()
      implicit val tileIndex: TileFormatter[TileIndex] = x => s"id:${x.index}"
    }

    def printTile[A: TileFormatter](tileSupporting: A): Unit = {
      // NOTE: The actual value of `fmt` is "summoned" via implicits resolution, matching the input concrete type T at call-site
      val fmt: TileFormatter[A] = implicitly[TileFormatter[A]]
      val formattedTile = fmt.format(tileSupporting)
      println(
        s"Formatted representation of type '${tileSupporting.getClass().getName()}': ${formattedTile}"
      )
    }
    val tileIndex1: TileIndex = tileKey4
    printTile(tileKey4)
    printTile(tileIndex1)
  }
}
