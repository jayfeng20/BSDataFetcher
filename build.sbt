ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.6"

Compile / mainClass := Some("Main")

lazy val root = (project in file("."))
  .settings(
    name := "BSDataFetcher"
  )

libraryDependencies ++= Seq(
  // config
  "com.github.pureconfig" %% "pureconfig-core" % "0.17.9",

  "com.softwaremill.sttp.client3" %% "core" % "3.11.0",
  "com.softwaremill.sttp.client3" %% "circe" % "3.11.0",
  "io.circe" %% "circe-generic" % "0.14.14",

  // logging
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "ch.qos.logback" % "logback-classic" % "1.5.18",

  // command line args parsing
  "com.github.scopt" %% "scopt" % "4.1.1-M3",

  // Kafka
  "org.apache.kafka" % "kafka-clients" % "4.1.0",
  "org.apache.kafka" % "kafka-streams" % "4.1.0",

  // Parquet and Avro
  "org.apache.parquet" % "parquet-avro" % "1.16.0"
)

scalacOptions ++= Seq("-Xmax-inlines", "64")

scalafmtOnCompile := true