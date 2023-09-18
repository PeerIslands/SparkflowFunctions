ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.2" % "provided",
  "org.apache.spark" %% "spark-core" % "3.3.2" % "provided",
  // Delta lake lib
  "io.delta" %% "delta-core" % "2.1.0" % "provided",
)
lazy val root = (project in file("."))
  .settings(
    name := "sparkflow_functions"
  )
organization := "io.peerislands"