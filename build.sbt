// Learn more in: https://www.scala-sbt.org/1.x/docs/sbt-by-example.html

ThisBuild / scalaVersion := "2.13.8"
ThisBuild / version := "0.1.0-SNAPSHOT"

lazy val hello = (project in file("."))
  .settings(
    name := "learning-scala",
    // NOTE: Using 2.13.8, we hit eviction issues with GeoTrellis requiring 2.13.16,
    //       but I have to use 2.13.8 because I want Spark 3.3.4 to be rock solid with Almond.
    allowUnsafeScalaLibUpgrade := true,
    Compile / mainClass := Some("hello.Main"),
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
    )
  )
