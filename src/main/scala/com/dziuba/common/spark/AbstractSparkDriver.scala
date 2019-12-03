package com.dziuba.common.spark

import com.dziuba.common.AppConfig
import com.dziuba.common.metrics.MetricsSystem._
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder
import org.slf4j.LoggerFactory

abstract class AbstractSparkDriver {

  @transient protected lazy val logger =
    Logger(LoggerFactory.getLogger(getClass.getName))

  lazy val config: AppConfig = AppConfig()

  implicit class SparkBuilder(b: Builder) {

    def setMaster(url: Option[String]): Builder = if (url.isDefined) b.master(url.get) else b

  }

  def runApp(): Unit = {

    val spark = SparkSession.builder()
      .config("spark.sql.shuffle.partitions", config.SparkSqlShufflePartitions)
      .setMaster(config.SparkMaster)
      .getOrCreate().enableAppMetrics()

    run(spark, config)
  }

  def run(implicit spark: SparkSession, config: AppConfig): Unit

}