package hello
import scala.io

object Ex01 extends App {
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
        case NeighborhoodMode.Plus =>
          List(
            TileId(col + 1, row),
            TileId(col - 1, row),
            TileId(col, row + 1),
            TileId(col, row - 1)
          )
        case NeighborhoodMode.Box =>
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
}
