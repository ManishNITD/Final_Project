ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "air-quality-analysis"
  )

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql" % "3.5.1",
  "org.apache.hadoop" % "hadoop-common" % "3.3.4",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
  "mysql" % "mysql-connector-java" % "8.0.19",
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.2.18" % "test",
  "com.github.jnr" % "jnr-posix" % "3.1.19",
  "joda-time" % "joda-time" % "2.12.7"
)