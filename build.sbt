name := "spark_playground"

version := "0.1"

scalaVersion := "2.12.8"

val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.scalatest" %% "scalatest" % "3.0.5" % Test
)