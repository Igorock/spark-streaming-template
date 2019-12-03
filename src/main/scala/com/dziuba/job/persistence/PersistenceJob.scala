package com.dziuba.job.persistence

import com.dziuba.common.AppConfig
import com.dziuba.common.messagebroker.MessageProducer._
import com.dziuba.common.messagebroker.MessageType.{OutputPersistenceKey, PersistenceMessage}
import com.dziuba.common.spark.AbstractKafkaToSqlStreamDriver
import com.dziuba.common.spark.MessageDataFrameReader.PersistenceMessageReader
import com.dziuba.db.QueryRunner.withConnector
import com.dziuba.db.SqlConnector
import com.dziuba.db.dao.DbDAO
import com.dziuba.job.persistence.PersistenceTask.PersistenceOutput
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

case class PersistenceJob() extends AbstractKafkaToSqlStreamDriver {

  override def run(implicit spark: SparkSession, config: AppConfig): Unit = {
    val topics: String = getTopics(config)

    implicit val messageEncoder: Encoder[PersistenceMessage] = Encoders.javaSerialization[PersistenceMessage]

    val groupId = this.getClass.getSimpleName
    val keys = readStream(groupId, topics, PersistenceMessageReader())
    val writer = PersistenceWriter(config)

    implicit val outputEncoder: Encoder[PersistenceOutput] = Encoders.javaSerialization[PersistenceOutput]

    execute[OutputPersistenceKey, PersistenceOutput](keys, writer, groupId) { (rowObj, connection) =>
      PersistenceTask.insertRecord(rowObj, connection)
    }
  }

  private def getTopics(config: AppConfig): String = {
    val allTables = getListOfTables(sqlConnectorFactory(config))
    val incomeTopics = allTables.map(config.IncomeTopicPrefix + _.toUpperCase)
    val exportTopics = allTables.map(config.ExportTopicPrefix + _.toUpperCase)
    (incomeTopics ++ exportTopics).mkString(",")
  }

  private def getListOfTables(connector: SqlConnector): Seq[String] = {
    withConnector(connector) {
      new DbDAO(_).findAllTables
    }
  }
}

object PersistenceJob {
  def main(args: Array[String]): Unit = new PersistenceJob().runApp()
}
