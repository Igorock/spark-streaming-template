package com.dziuba.job.persistence

import java.sql.Connection
import java.util.Calendar

import com.dziuba.common.AppConfig
import com.dziuba.common.messagebroker.MessageProcessors.JsonMessageHelper
import com.dziuba.common.messagebroker.MessageType
import com.dziuba.common.messagebroker.MessageType.OutputPersistenceKey
import com.dziuba.common.metrics.MetricsSystem._
import com.dziuba.db.dao.DbDAO
import com.typesafe.scalalogging.Logger


object PersistenceTask {

  @transient lazy val logger: Logger = Logger("PersistenceTask")
  @transient lazy val config: AppConfig = AppConfig()

  case class PersistenceOutput(topic: String, table: String, outputKey: OutputPersistenceKey)

  def insertRecord(outputKey: OutputPersistenceKey, connection: Connection): Option[PersistenceOutput] = {
    val record = outputKey.body
    try {
      val dbDAO = new DbDAO(connection)
      val startTime = Calendar.getInstance().getTimeInMillis
      logger.info("Inserting data for record " + record.key + " into " + record.table)
      measure(record.table, record.topic.startsWith(config.ExportTopicPrefix)) {
        val jsonMap = JsonMessageHelper.parseJsonToMap(record.rawMessage)
        val colValues: Map[String, Any] = jsonMap
          .filter(kv => !(kv._1.equals(MessageType.CREATED_TIME) && !kv._2.isInstanceOf[String]))

        dbDAO.insertGeneratedRecord(record.table, colValues, connection)
      }
      val endTime = Calendar.getInstance().getTimeInMillis
      logger.info(s"Finished inserting data for record ${record.key} in ${endTime - startTime} ms.")

      Some(PersistenceOutput(record.topic, record.table, outputKey))
    } catch {
      case e: Exception =>
        logger.error("Error while inserting data for record " + record, e)
        None
    }
  }


  def measure(tableName: String, isExport: Boolean)(f: => Any): Unit = {
    val timer = if (!isExport) insertedRecordTimer.start() else NoneTimer()
    val timerPerTable = if (!isExport) insertedRecordToTableTimer(tableName).start() else NoneTimer()
    val exportedTimer = if (isExport) insertedExportRecordTimer.start() else NoneTimer()
    try {
      f
    } finally {
      timer.stop()
      timerPerTable.stop()
      exportedTimer.stop()
    }
  }
}