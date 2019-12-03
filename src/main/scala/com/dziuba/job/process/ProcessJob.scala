package com.dziuba.job.process

import com.dziuba.common.AppConfig
import com.dziuba.common.messagebroker.MessageType._
import com.dziuba.common.spark.MessageDataFrameReader.{ComputationMessageKinesisReader, ComputationMessageReader}
import com.dziuba.common.spark.{AbstractKafkaToSqlStreamDriver, SharedVariable}
import com.dziuba.db.QueryRunner.withConnector
import com.dziuba.db.service.ComputationKeyProcessor._
import com.dziuba.job.process.ProcessTask.ProcessOutput
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

case class ProcessJob() extends AbstractKafkaToSqlStreamDriver {

  private lazy val keyProcessorFactory: Processor = processComputationKeyProcessorFactory()

  override def run(implicit spark: SparkSession, config: AppConfig): Unit = {

    val factory = getConnectorFactory
    val connectorPool = SharedVariable {
      factory(config)
    }

    val keyProcessorFactory: Processor = getKeyProcessorFactory
    implicit val messageEncoder: Encoder[ComputationMessage] = Encoders.javaSerialization[ComputationMessage]

    val groupId = this.getClass.getSimpleName

    val stream =
      if (config.StreamSource == KinesisSource)
        readKinesisStream(groupId, config.KinesisStreamName, ComputationMessageKinesisReader())
      else
        readStream(groupId, config.ProcessIncomeTopic, ComputationMessageReader())

    val keys = stream.flatMap { key =>
      withConnector(connectorPool.get) {
        keyProcessorFactory(key.body, _).map(ComputationMessage(key, _))
      }
    }

    val writer = ProcessWriter(config)

    implicit val outputEncoder: Encoder[ProcessOutput] = Encoders.javaSerialization[ProcessOutput]

    execute[OutputComputationKey, ProcessOutput](keys, writer, groupId) { (rowObj, connection) =>
      ProcessTask().calculate(rowObj, connection)
    }
  }

  def getKeyProcessorFactory: Processor = keyProcessorFactory
}

object ProcessJob {
  def main(args: Array[String]): Unit = new ProcessJob().runApp()
}
