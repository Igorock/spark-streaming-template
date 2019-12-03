package com.dziuba.common.spark

import com.dziuba.common.messagebroker.MessageProcessors.{computationMessageProcessor, persistenceMessageProcessor}
import com.dziuba.common.messagebroker.MessageType._
import org.apache.spark.sql.{Encoder, _}


trait MessageDataFrameReader[T] {
  val systemName: String

  def read(df: DataFrame): Dataset[T]
}

object MessageDataFrameReader {

  type PersistenceMessageEncoder = Encoder[PersistenceMessage]

  type ComputationMessageEncoder = Encoder[ComputationMessage]

  def createInputMessageKey(df: DataFrame)(implicit spark: SparkSession): Dataset[InputMessageKey] = {
    import spark.implicits._
    df.selectExpr("CAST(topic AS STRING)", "CAST(key AS STRING)", "CAST(value AS STRING)")
      .groupBy("topic", "key", "value")
      .count().as[InputMessageKey]
  }

  def createKinesisInputMessageKey(df: DataFrame)(implicit spark: SparkSession): Dataset[InputMessageKey] = {
    import spark.implicits._
    df.selectExpr("CAST(streamName AS STRING)", "CAST(partitionKey AS STRING)", "CAST(data AS STRING)")
      .groupBy("streamName", "partitionKey", "data")
      .count().as[InputMessageKey]
  }

  def getSystemName(implicit spark: SparkSession): String = if (spark.sparkContext != null) spark.sparkContext.appName else Undefined

  case class ComputationMessageReader(implicit encoder: ComputationMessageEncoder, spark: SparkSession) extends MessageDataFrameReader[ComputationMessage] {
    val systemName: String = getSystemName

    override def read(df: DataFrame): Dataset[ComputationMessage] = {
      createInputMessageKey(df).map((message: (String, String, String, BigInt)) => computationMessageProcessor.read(systemName, message))
    }
  }

  case class ComputationMessageKinesisReader(implicit encoder: ComputationMessageEncoder, spark: SparkSession) extends MessageDataFrameReader[ComputationMessage] {
    val systemName: String = getSystemName

    override def read(df: DataFrame): Dataset[ComputationMessage] = {
      createKinesisInputMessageKey(df).map((message: (String, String, String, BigInt)) => computationMessageProcessor.read(systemName, message))
    }
  }

  case class PersistenceMessageReader(implicit encoder: PersistenceMessageEncoder, spark: SparkSession) extends MessageDataFrameReader[PersistenceMessage] {
    val systemName: String = getSystemName

    override def read(df: DataFrame): Dataset[PersistenceMessage] = {
      createInputMessageKey(df).map((message: (String, String, String, BigInt)) => persistenceMessageProcessor.read(systemName, message))
    }
  }

}