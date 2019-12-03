package com.dziuba.job.process

import java.sql.SQLException
import java.util.Calendar

import com.dziuba.common.AppConfig
import com.dziuba.common.messagebroker.MessageProducer
import com.dziuba.common.spark.DatabaseWriter
import com.dziuba.job.process.ProcessTask._

case class ProcessWriter(config: AppConfig)(implicit val messageProducer: MessageProducer) extends DatabaseWriter[ProcessOutput](config) with HandlerExceptionResolver {

  override def process(output: ProcessOutput): Unit = {
    connection.setAutoCommit(false)

    val computationKey = output.outputKey.body
    try {
      insertGraphs(output.graphOutputs, computationKey)

      connection.commit()
      messageProducer.send(config.UndefinedOutcomeTopic, output.outputKey)

      if (output.graphOutputs.nonEmpty) {
        val timeTaken = Calendar.getInstance().getTimeInMillis - output.startTime
        Log.info(s"Total time taken for $computationKey: $timeTaken")
      }

    } catch {
      case e: SQLException =>
        connection.rollback()
        Log.error("Error while inserting data", e)
        exceptionHandler.handleSQLException(e, computationKey)
    }
  }

  private def insertGraphs(graphOutputs: Seq[GraphOutput], row: ComputationKey): Unit = {
    //implementation
  }

}