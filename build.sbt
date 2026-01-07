// Learn more in: https://www.scala-sbt.org/1.x/docs/sbt-by-example.html

import sbtassembly.AssemblyPlugin.autoImport._

ThisBuild / scalaVersion := "2.13.16"
ThisBuild / version := "0.1.0-SNAPSHOT"

lazy val shadePrefix = "gtshade"
// NOTE: Why we shade Spire (and algebra)
//       I got hit with `java.lang.NoSuchMethodError: 'spire.math.IntIsIntegral spire.math.Integral$.IntIsIntegral()'`
//       when submitting a geotrellis-spark dependent program to my standalone Spark cluster (Ex07RddProgram)
//       After numerous debugging sessions, and after analyzing the dependency graph (using `sbt dependencyDot`),
//       I (and ChatGPT) figured out that the problem was in a "dependency-race"
//       between Spark's MLib bundled Spire library (0.18.0) and Geotrellis `sbt-assembly` bundled Spire library (0.17.0).
//       Because Spark is `% provided` this conflict didn't surface in `sbt evicted`, but only at runtime.
//       At runtime, Spark would load MLib's Spire (0.18.0) BEFORE the driver had a chance to
//       load geotrellis-spark and it's Spire (0.17.0) library from the fat-jar.
//
//       One of the ways ChatGPT diagnosed this is by looking at the JARs bundled in the Spark distribution, using this command:
//       ```
//       ls -1 $SPARK_HOME/jars | grep -i spire || true
//       sbt evicted | grep -i spire -n
//       ```
//       To get better context into this hellish bug, view the dependency graph dot file interactively 
//       using `sbt dependencyDot` and the installed `Graphviz Interactive Preview` VSCode extension
ThisBuild / assemblyShadeRules := Seq(
  // relocate Spire
  ShadeRule.rename("spire.**" -> s"$shadePrefix.spire.@1").inAll,
  // Spire depends on algebra; relocating it avoids similar collisions
  ShadeRule.rename("algebra.**" -> s"$shadePrefix.algebra.@1").inAll
)
// WARNING: Technically we got another problem like this between 
//          `scala-collection-compat` 2.2.0 (expected by Geotrellis, packaged in the fat jar) 
//          and Spark provided jar `scala-collection-compat` 2.7.0
//          Run `ls -1 $SPARK_HOME/jars | grep -i 'scala-collection-compat' || true` to see the conflicting jar file.
// WARNING: Uncomment if you see any sign of trouble!
// dependencyOverrides += "org.scala-lang.modules" %% "scala-collection-compat" % "2.7.0"


lazy val hello = (project in file("."))
  .settings(
    name := "learning-scala",
    // NOTE: Using 2.13.8, we hit eviction issues with GeoTrellis requiring 2.13.16,
    //       but I have to use 2.13.8 because I want Spark 3.5.5 to be rock solid with Almond.
    allowUnsafeScalaLibUpgrade := true,
    Compile / mainClass := Some("learningscala.Main"),
    libraryDependencies ++= Seq(
      // For Ex02 and beyond
      "ch.qos.logback" % "logback-classic" % "1.5.23",
      "org.slf4j" % "slf4j-api" % "2.0.17",
      "com.github.tototoshi" %% "scala-csv" % "2.0.0",
      "com.lihaoyi" %% "pprint" % "0.9.6",

      // For Ex05 and Ex07; See: https://github.com/locationtech/geotrellis#getting-started
      "org.locationtech.geotrellis" %% "geotrellis-raster" % "3.8.0",

      // For Ex05_5 and beyond
      // NOTE: "provided" is used to avoid packaging Spark with the application JAR
      //       as it is expected to be available in the Spark runtime environment.
      //       See: https://www.scala-sbt.org/1.x/docs/Scopes.html
      "org.apache.spark" %% "spark-core" % "3.5.5" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.5.5" % "provided",
      // Used to access OS-Lib (os.<*>) and launch sbt processes in Spark exercises
      "org.scala-lang" %% "toolkit" % "0.7.0",

      // For Ex07
      "org.locationtech.geotrellis" %% "geotrellis-layer" % "3.8.0",
      "org.locationtech.geotrellis" %% "geotrellis-spark" % "3.8.0"
    ),

    // sbt-assembly configuration
    // Name the resulting fat jar predictably: learning-scala-assembly-<version>.jar
    assembly / assemblyJarName := s"${name.value}-assembly-${version.value}.jar",
    // Ensure the assembled jar has the right entrypoint when needed
    assembly / mainClass := Some("learningscala.Main"),
    // Merge strategy to avoid weird conflicts (META-INF, services, HOCON, module-info)
    // See https://github.com/sbt/sbt-assembly?tab=readme-ov-file#merge-strategy
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) =>
        xs match {
          case Seq("MANIFEST.MF") | Seq("INDEX.LIST") | Seq("DEPENDENCIES") =>
            MergeStrategy.discard
          case Seq("versions", _ @_*) => MergeStrategy.first
          case Seq("plexus", _ @_*)   => MergeStrategy.discard
          case Seq("services", _ @_*) => MergeStrategy.concat
          case _                      => MergeStrategy.discard
        }
      case "module-info.class"                         => MergeStrategy.discard
      case "reference.conf"                            => MergeStrategy.concat
      case "application.conf"                          => MergeStrategy.concat
      case PathList("META-INF", "native-image", _ @_*) => MergeStrategy.discard
      case PathList("META-INF", "maven", _ @_*)        => MergeStrategy.discard
      case PathList("META-INF", "licenses", _ @_*)     => MergeStrategy.discard
      case "META-INF/NOTICE" | "META-INF/NOTICE.txt"   => MergeStrategy.discard
      case "META-INF/LICENSE" | "META-INF/LICENSE.txt" => MergeStrategy.discard
      // Prefer the first occurrence for everything else
      case _ => MergeStrategy.first
    }
  )
