scalaVersion := "2.13.8"
// NOTE: Using 2.13.8, we hit eviction issues with GeoTrellis requiring 2.13.16,
// but I have to use 2.13.8 because I want Spark 3.3.4 to be rock solid with Almond.
allowUnsafeScalaLibUpgrade := true

// For Ex02
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "2.0.0"

// For Ex02 and beyond
libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.4.14",
  "org.slf4j" % "slf4j-api" % "2.0.9"
)
libraryDependencies += "com.lihaoyi" %% "pprint" % "0.9.6"

// For Ex05 and beyond
// https://mvnrepository.com/artifact/org.locationtech.geotrellis/geotrellis-raster
// https://github.com/locationtech/geotrellis?tab=readme-ov-file#getting-started
libraryDependencies += "org.locationtech.geotrellis" %% "geotrellis-raster" % "3.8.0"

// For Ex05_5 and beyond
// NOTE: "provided" is used to avoid packaging Spark with the application JAR
//       as it is expected to be available in the Spark runtime environment.
//       See: https://www.scala-sbt.org/1.x/docs/Scopes.html
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.4" % "provided",
  "org.apache.spark" %% "spark-sql"  % "3.3.4" % "provided"
)
