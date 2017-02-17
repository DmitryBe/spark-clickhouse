
name := """clickhouse-spark-connector"""

version := "1.1"

scalaVersion := "2.11.7"

publishTo := Some("jFrog" at "http://10.2.95.5:8080/artifactory/libs-release")
//credentials += Credentials("jFrog", "10.2.95.5", "admin", "password")

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.0.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.0.0",
  "ru.yandex.clickhouse" % "clickhouse-jdbc" % "0.1.14",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.7.4"
)

fork in run := true

test in assembly := {}

assemblyMergeStrategy in assembly := {
  case n if n.startsWith("META-INF/MANIFEST.MF") => MergeStrategy.discard
  case "reference.conf"                          => MergeStrategy.concat
  case x => MergeStrategy.first
}

packAutoSettings