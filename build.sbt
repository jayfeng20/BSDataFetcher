ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.6"

lazy val root = (project in file("."))
  .settings(
    name := "BSDataFetcher"
  )

// Only in dev: Enable forking of a new JVM process to run the application so environment variables are picked up
fork := true