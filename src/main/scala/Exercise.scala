package hello

trait Exercise {
  final def name: String = {
    // NOTE: We .stripSuffix("$") because singletons in scala are represented in JVM as "<classname>$"
    //       in order to differentiate them from classes sharing the same name and forming companionship
    this.getClass().getSimpleName().stripSuffix("$")
  }
  def run(): Unit
}

object SparkRunner {
  def runSparkJob(className: String): Unit = {
    println("Creating JAR file...")
    val sbtPackage = os
      .proc(
        "sbt",
        "package"
      )
      .spawn(cwd = os.pwd, stdout = os.Inherit, stderr = os.Inherit)
    sbtPackage.waitFor(-1)

    println("Submitting spark job...")
    val driver = os
      .proc(
        "spark-submit",
        "--master",
        "spark://spark-localhost:7077",
        "--class",
        s"hello.${className}",
        "--deploy-mode",
        "client",
        "/workspaces/learning-scala/target/scala-2.13/learning-scala_2.13-0.1.0-SNAPSHOT.jar"
      )
      .spawn(cwd = os.pwd, stdout = os.Inherit, stderr = os.Inherit)
    driver.waitFor(-1)
  }
}
