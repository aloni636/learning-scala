package learningscala

trait Exercise {
  final def name: String = {
    // NOTE: We .stripSuffix("$") because singletons in scala are represented in JVM as "<classname>$"
    //       in order to differentiate them from classes sharing the same name and forming companionship
    this.getClass().getSimpleName().stripSuffix("$")
  }
  def run(): Unit
}

// NOTE: We use a dedicated object to run Ex05_5 Spark job because Spark library is `% "provided"`,
//       meaning we can't load objects dependent on its existence within sbt run.
//       This means the runner must be a dedicated object.
trait SparkExercise extends Exercise {
  val jobs: Seq[String]
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
  def run(): Unit = {
    this.assemble()

    this.jobs.foreach { job =>
      println(s"[SparkExercise] Submitting job '${job}' to Spark...")
      val driver = os
        .proc(
          "spark-submit",
          "--master",
          "spark://spark-localhost:7077",
          "--class",
          s"learningscala.${job}",
          "--deploy-mode",
          "client",
          "/workspaces/learning-scala/target/scala-2.13/learning-scala-assembly-0.1.0-SNAPSHOT.jar"
        )
        .spawn(cwd = os.pwd, stdout = os.Inherit, stderr = os.Inherit)
      driver.waitFor(-1)
      println(s"[SparkExercise] Finished job '${job}'")
    }
  }
}
