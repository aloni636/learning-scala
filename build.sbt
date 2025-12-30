// Learn more in: https://www.scala-sbt.org/1.x/docs/sbt-by-example.html

import sbtassembly.AssemblyPlugin.autoImport._

ThisBuild / scalaVersion := "2.13.8"
ThisBuild / version := "0.1.0-SNAPSHOT"

lazy val hello = (project in file("."))
  .settings(
    name := "learning-scala",
    // NOTE: Using 2.13.8, we hit eviction issues with GeoTrellis requiring 2.13.16,
    //       but I have to use 2.13.8 because I want Spark 3.3.4 to be rock solid with Almond.
    allowUnsafeScalaLibUpgrade := true,
    Compile / mainClass := Some("learningscala.Main"),
    libraryDependencies ++= Seq(
      // For Ex02 and beyond
      "ch.qos.logback" % "logback-classic" % "1.4.14",
      "org.slf4j" % "slf4j-api" % "2.0.9",
      "com.github.tototoshi" %% "scala-csv" % "2.0.0",
      "com.lihaoyi" %% "pprint" % "0.9.6",

      // For Ex05 & beyond; See: https://github.com/locationtech/geotrellis#getting-started
      "org.locationtech.geotrellis" %% "geotrellis-raster" % "3.8.0",

      // For Ex05_5 and beyond
      // NOTE: "provided" is used to avoid packaging Spark with the application JAR
      //       as it is expected to be available in the Spark runtime environment.
      //       See: https://www.scala-sbt.org/1.x/docs/Scopes.html
      "org.apache.spark" %% "spark-core" % "3.3.4" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.3.4" % "provided",
      "org.scala-lang" %% "toolkit" % "0.7.0"
    ),
    // TODO: Understand WTF is going on...

    // sbt-assembly configuration
    // Name the resulting fat jar predictably: learning-scala-assembly-<version>.jar
    assembly / assemblyJarName := s"${name.value}-assembly-${version.value}.jar",
    // Ensure the assembled jar has the right entrypoint when needed
    assembly / mainClass := Some("learningscala.Main"),
    // Robust merge strategy to avoid common conflicts (META-INF, services, HOCON, module-info)
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
