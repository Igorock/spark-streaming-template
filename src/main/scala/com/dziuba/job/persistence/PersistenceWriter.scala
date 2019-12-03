package com.dziuba.job.persistence

import com.dziuba.common.AppConfig
import com.dziuba.common.exceptionhandling.InvalidPersistenceMessageException
import com.dziuba.common.messagebroker.MessageProducer
import com.dziuba.common.messagebroker.MessageType.ComputationMessage
import com.dziuba.job.persistence.PersistenceTask.PersistenceOutput
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.ForeachWriter

case class PersistenceWriter(config: AppConfig)(implicit val messageProducer: MessageProducer) extends ForeachWriter[PersistenceOutput] {
  @transient lazy val logger: Logger = Logger(classOf[PersistenceWriter])

  override def open(partitionId: Long, version: Long): Boolean = true

  override def process(output: PersistenceOutput): Unit = {
    val record = output.outputKey.body
    if (record.computationKey.isEmpty) throw InvalidPersistenceMessageException(s"Invalid record: ${record.rawMessage}")

    if (!isExportTopic(output.topic, output.table)) {
      try {

        val message: ComputationMessage =
          ComputationMessage(output.outputKey.system, System.currentTimeMillis(), record.computationKey.get, output.outputKey.details)
        messageProducer.send(config.FlowThroughIncomeTopic, message)
      }
      catch {
        case e: Exception =>
          logger.error("Error while producing data for record " + record.key, e)
          throw new Exception(e)
      }
    } else
      logger.info(s"Ignore publishing record ${record.key} for export topic ${record.topic}")
  }

  def isExportTopic(topic: String, table: String): Boolean =
    topic.startsWith(config.ExportTopicPrefix) || !(config.TopicTables ++ config.PartialKeyTopicTables).contains(table)

  override def close(errorOrNull: Throwable): Unit = {}
}
