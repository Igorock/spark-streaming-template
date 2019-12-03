name := "Spark Streaming Template"
version := "1.0"
scalaVersion := "2.11.1"

val sparkVersion = "2.2.1"

import com.github.sbtliquibase.SbtLiquibase
import com.typesafe.config.{Config, ConfigFactory}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided" withSources(),
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "com.amazonaws" % "aws-java-sdk" % "1.7.4",
  "org.apache.hadoop" % "hadoop-aws" % "2.7.3",

  "com.memsql" %% "memsql-connector" % "2.0.5",
  "mysql" % "mysql-connector-java" % "5.1.34",
  "org.postgresql" % "postgresql" % "42.2.2",

  "org.slf4j" % "slf4j-log4j12" % "1.7.25",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.8.0",

  "com.typesafe" % "config" % "1.3.2",
  "com.groupon.dse" % "spark-metrics" % "2.0.0",

  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "org.mockito" % "mockito-all" % "1.10.19" % Test,
  "com.googlecode.json-simple" % "json-simple" % "1.1.1" % Test
)

mainClass in assembly := some("com.dziuba.SparkApplication")
test in assembly := {}
assemblyJarName := "sparkcalcengine_v3.jar"
val meta = """META.INF(.)*""".r
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.first
}

enablePlugins(ScoverageSbtPlugin, SonarRunnerPlugin, ScalastylePlugin, SbtLiquibase)

val appProperties = settingKey[Config]("The application properties")
appProperties := {
  val resourceDir = (resourceDirectory in Compile).value
  val appConfig = ConfigFactory.parseFile(resourceDir / "application.properties")
  ConfigFactory.load(appConfig)
}

parallelExecution in Test := false
fork in Test := true