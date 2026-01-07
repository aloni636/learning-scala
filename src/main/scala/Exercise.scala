package learningscala
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

trait Exercise {
  final def name: String = {
    // NOTE: We .stripSuffix("$") because singletons in scala are represented in JVM as "<classname>$"
    //       in order to differentiate them from classes sharing the same name and forming companionship
    this.getClass().getSimpleName().stripSuffix("$")
  }
  def run(): Unit
}

/** Exercise runner for Spark-based exercises.
  *
  * Assembles the fat JAR using sbt-assembly and submits each specified program
  * to a Spark cluster using `spark-submit`.
  *
  * @param programs
  *   Sequence of fully qualified object names containing Spark programs to run.
  */
trait SparkExercise extends Exercise {
  // NOTE: We use a dedicated object to run Spark jobs because Spark library is `% "provided"`,
  //       meaning we can't load objects dependent on its existence within sbt run.
  //       This means the runner must be a dedicated object.
  val programs: Seq[String]
  private def assemble() {
    println("[SparkExercise] Assembling fat JAR...")
    val sbtAssembly = os
      .proc(
        "sbt",
        "assembly"
      )
      .spawn(cwd = os.pwd, stdout = os.Inherit, stderr = os.Inherit)
    sbtAssembly.waitFor(-1)
  }
  final def run(): Unit = {
    this.assemble()

    this.programs.foreach { program =>
      println(s"[Spark Exercise] Submitting program '${program}' to Spark...")
      val driver = os
        .proc(
          "spark-submit",
          "--master",
          "spark://localhost:7077",
          "--class",
          s"learningscala.${program}",
          "--deploy-mode",
          "client",
          "/workspaces/learning-scala/target/scala-2.13/learning-scala-assembly-0.1.0-SNAPSHOT.jar"
        )
        .spawn(cwd = os.pwd, stdout = os.Inherit, stderr = os.Inherit)
      driver.waitFor(-1)
      println(s"[Spark Exercise] Finished program '${program}'")
    }
  }
}

/** Trait to be extended by all Spark-based programs.
  *
  * Provides a standard `main` method that initializes a SparkSession and
  * SparkContext, and passes them along with a Logger to the `application`
  * method to be implemented by the extending object.
  */
trait SparkProgram {
  def application(spark: SparkSession, sc: SparkContext, log: Logger): Unit

  final def main(args: Array[String]): Unit = {
    val name: String = this.getClass().getSimpleName().stripSuffix("$")

    val spark = SparkSession
      .builder()
      .appName(name)
      .master(s"spark://localhost:7077")
      .config("spark.executor.memory", "4g")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    val log = Logger.getLogger(this.getClass())
    application(spark, sc, log)
  }
}
