package com.dziuba.common

import com.dziuba.common.AppConfig._
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._

class AppConfig private(c: Config) extends Serializable {

  val KafkaBootstrapServer: String = c.getString("kafka.bootstrap-server")
  val ZookeeperServer: String = c.getString("kafka.zookeeper-server")
  val JdbcConnectionUrl: String = c.getString("datasource.url")
  val UserName: String = c.getString("datasource.username")
  val Password: String = c.getString("datasource.password")
  val JdbcDriver: String = c.getString("datasource.driver")
  val SparkSqlShufflePartitions: String = c.getString("spark.sql-shuffle-partitions")
  val IncomeTopicPrefix: String = c.getString("app.persistence.income-topic-prefix")
  val ExportTopicPrefix: String = c.getString("app.persistence.export-topic-prefix")
  val TopicTables: Seq[String] = c.getStringAsSeq("app.persistence.topic-tables")
  val PartialKeyTopicTables: Seq[String] = c.getStringAsSeq("app.persistence.partial-key-topic-tables")
  val ProcessingTime: Long = c.getLong("spark.processing-time")
  val ProcessIncomeTopic: String = c.getString("app.process.income-topic")
  val ProcessErrorTopic: String = c.getString("app.process.error-topic")
  val UndefinedOutcomeTopic: String = "Undefined"
  val CheckpointLocation: String = c.getString("spark.checkpoint-location")
  val KinesisStreamName: String = c.getString("kinesis.stream-name")
  val KinesisRegion: String = c.getString("kinesis.region")
  val StreamSource: String = c.getString("app.process.stream-source")
  val SparkMaster: Option[String] = c.opt[String]("spark.master")

}

object AppConfig {

  val TestingProperty = "testing"

  implicit class ConfigExt(c: Config) {
    def getStringAsSeq(key: String): Seq[String] = c.getString(key).split(",").map(_.trim).filter(_.nonEmpty).toSeq

    def opt[A](key: String): Option[A] = if (c.hasPath(key)) Some(c.getAnyRef(key).asInstanceOf[A]) else None
  }

  lazy val DefaultConfig: Config = ConfigFactory.load
  lazy val instance = new AppConfig(DefaultConfig)

  def apply(): AppConfig = instance

  def apply(head: (String, String), tail: (String, String)*): AppConfig = {
    val map = (head +: tail).groupBy(_._1).map {
      case (k, v) => (k, v.map(_._2).last)
    }
    val fallbackConfig = ConfigFactory.parseMap(map).withFallback(DefaultConfig).resolve()
    new AppConfig(fallbackConfig)
  }
}