ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "untitled1"
  )
val sparkVersion = "3.2.2"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.1"
libraryDependencies += "com.typesafe" % "config" % "1.3.3"