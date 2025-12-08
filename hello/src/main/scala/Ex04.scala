package hello
// Concurrent
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.blocking
import scala.concurrent.TimeoutException
// IO
import java.nio.file.Files
import java.nio.file.Paths
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/*
  ---

  ### 4. Minimal concurrency

  You do not need heavy FP here; just enough to not fear `Future`:

 * `Future`, `ExecutionContext`, `map`/`flatMap` on `Future`.
 * When writing GeoTrellis + Spark jobs you’ll mostly rely on Spark’s own parallelism, but `Future` does appear in tooling and orchestrating code.

  Exercise:

 * Wrap a small blocking I/O call (e.g. reading a file) in a `Future` and sequence two of them with a `for`-comprehension.
 */

object Ex04 extends Exercise {
  override def run(): Unit = {
    implicit val log: Logger = LoggerFactory.getLogger(getClass())

    // ===== User input (just like Ex01), but with timeout =====
    val TIMEOUT = 5.seconds
    val userInputFuture = Future {
      // Hint to the EC (ExecutionContext) that this may be blocking,
      // allowing it to better adapt to the situation
      blocking {
        scala.io.StdIn.readLine(
          s"Provide a list of integers (separated by ',' | timeout in $TIMEOUT): "
        )
      }
    }
    try {
      val userNumbers = Await
        .result(userInputFuture, TIMEOUT)
        .split(",")
        .map(_.strip().toIntOption)
        .collect({ case Some(x) => x })
      println(s"[${userNumbers.mkString(", ")}]")
    } catch { case _: TimeoutException => println("Timeout") }

    val tiffPaths = List(
      "../data/0x00000001-0x00000002-0x00000001.tiff",
      "../data/0x00000001-0x00000002-0x00000003.tiff"
    )
    def scheduleTiffRead(path: String)(implicit log: Logger) = Future {
      val tiffFile = Paths.get(path)
      log.info(s"Reading bytes from tiff: '$tiffFile'")
      Files.readAllBytes(tiffFile)
    }
    // `gather` like synchronization
    val tiffFutures = Future.traverse(tiffPaths)(scheduleTiffRead)
    val printFuture = tiffFutures.map { x => x.foreach(println(_)) }

    // `asyncio.run` top level execution
    Await.result(printFuture, Duration.Inf)
  }
}
