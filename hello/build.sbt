scalaVersion := "2.13.18"
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "2.0.0"
libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.4.14",
  "org.slf4j" % "slf4j-api" % "2.0.9"
)
libraryDependencies += "com.lihaoyi" %% "pprint" % "0.9.6"