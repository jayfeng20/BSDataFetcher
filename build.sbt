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
  "org.apache.parquet" % "parquet-avro" % "1.16.0",

  // Spark -> needs explicit Spark dependencies because Scala 3 is not officially supported by Spark yet
  ("org.apache.spark" %% "spark-core" % "4.0.1").cross(CrossVersion.for3Use2_13),
  ("org.apache.spark" %% "spark-sql" % "4.0.1").cross(CrossVersion.for3Use2_13),

  // https://www.reddit.com/r/scala/comments/1j2rw0p/migrating_a_codebase_to_scala_3/ -> Scala3 with Spark4
  "io.github.vincenzobaz" %% "spark4-scala3-encoders" % "0.3.2",
"io.github.vincenzobaz" %% "spark4-scala3-udf" % "0.3.2"

)

// Force Jackson 2.18.2 to ensure compatibility with Spark 4
dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.2",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.18.2",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.18.2"
)

scalacOptions ++= Seq("-Xmax-inlines", "64")

scalafmtOnCompile := true