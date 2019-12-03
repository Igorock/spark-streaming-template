package com.dziuba.common.spark

import java.sql.{Connection, SQLException}

import com.dziuba.common.AppConfig
import com.dziuba.common.exceptionhandling.{ExceptionHandler, ParamNotFoundException}
import com.dziuba.db.QueryRunner
import com.dziuba.db.SqlConnector.ConnectorFactory
import com.dziuba.db.connector.JdbcConnector._
import com.dziuba.db.entity.EntityNotFoundException
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.Trigger

abstract class AbstractKafkaToSqlStreamDriver extends AbstractSparkDriver {

  protected lazy val sqlConnectorFactory: ConnectorFactory = connectorFactory()
  val kafkaServer: Seq[String] = Seq(config.KafkaBootstrapServer)
  val zookeeperServer: String = config.ZookeeperServer
  val KinesisSource = "kinesis"

  override def run(implicit spark: SparkSession, config: AppConfig): Unit

  def readStream[T](groupId: String, kafkaTopic: String, messageReader: MessageDataFrameReader[T])(implicit spark: SparkSession): Dataset[T] = {
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer.mkString(","))
      .option("zookeeper.connect", zookeeperServer)
      .option("subscribe", kafkaTopic)
      .option("group.id", groupId)
      .option("startingOffsets", "latest")
      .option("auto.offset.reset", "latest")
      .option("enable.auto.commit", config.CheckpointLocation.isEmpty)
      .load()
    messageReader.read(df)
  }

  def readKinesisStream[T](groupId: String, kinesisStream: String, messageReader: MessageDataFrameReader[T])(implicit spark: SparkSession): Dataset[T] = {
    val df = spark
      .readStream
      .format("kinesis")
      .option("streamName", kinesisStream)
      .option("endpointUrl", s"https://kinesis.${config.KinesisRegion}.amazonaws.com")
      .option("startingPosition", "LATEST")
      .load()

    df.printSchema
    messageReader.read(df)
  }

  def execute[K, A](keysDF: Dataset[K], writer: ForeachWriter[A], groupId: String)
                   (f: (K, Connection) => Option[A])
                   (implicit encoder: Encoder[A], spark: SparkSession, config: AppConfig): Unit = {

    val calculationDF = createCalculationDF(keysDF, f)

    writeDF(calculationDF, writer, groupId)
  }

  def getConnectorFactory: ConnectorFactory = sqlConnectorFactory

  private[dziuba] def createCalculationDF[A, K](keysDF: Dataset[K], f: (K, Connection) => Option[A])(implicit encoder: Encoder[A]): Dataset[A] = {

    val factory = getConnectorFactory
    val connectionPool = SharedVariable {
      factory(config)
    }

    keysDF.flatMap {
      rowObj => {
        val exceptionHandler: ExceptionHandler = ExceptionHandler()
        val connection = connectionPool.get.getConnection
        try {
          f(rowObj, connection)
        } catch {
          case e: ParamNotFoundException =>
            exceptionHandler.handleMissedParam(e)
            None
          case e: EntityNotFoundException =>
            exceptionHandler.handleMissedEntity(e)
            None
          case e: SQLException =>
            exceptionHandler.handleSQLException(e, rowObj)
            None
          case e: Exception =>
            exceptionHandler.handleOtherException(e, rowObj)
            None
        } finally {
          QueryRunner.close(connection)
        }
      }
    }
  }

  protected[dziuba] def writeDF[A](calculationDF: Dataset[A], writer: ForeachWriter[A], groupId: String)(implicit config: AppConfig): Unit = {
    val writeStream = calculationDF.writeStream
      .foreach(writer)
      .outputMode("update")

    val checkpointDF =
      if (!config.CheckpointLocation.isEmpty) writeStream.option("checkpointLocation", config.CheckpointLocation + "-" + groupId)
      else writeStream

    val writeDF = checkpointDF
      .trigger(Trigger.ProcessingTime(config.ProcessingTime))
      .start()

    writeDF.awaitTermination()
  }
}