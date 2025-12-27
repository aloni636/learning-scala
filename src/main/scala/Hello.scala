// scala 2.13
package hello
import hello.Exercise
import scala.io
import scala.collection.immutable.ListMap
import scala.collection.immutable.ListMapBuilder

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
    val exercises = ListMap.from(
      List(
        // TODO: Find a way to auto-discover Exercise extenders
        hello.Ex01,
        hello.Ex02,
        hello.Ex03,
        hello.Ex04,
        hello.Ex05,
        hello.Ex05_5,
        hello.Ex06
      ).map(x => x.name -> x)
    )
    val availableExercises = s"[${exercises.keys.mkString(", ")}]"
    val exerciseName = args.headOption
      .getOrElse(
        io.StdIn
          .readLine(s"Select exercise ${availableExercises}: ")
      )
      .strip()
    exercises.get(exerciseName) match {
      case Some(ex) => ex.run()
      case None     => {
        println(
          s"Unknown exercise name '${exerciseName}'. Available exercises: ${availableExercises}"
        )
      }
    }
  }
}
