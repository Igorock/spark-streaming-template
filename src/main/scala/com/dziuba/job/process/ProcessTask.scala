package com.dziuba.job.process

import java.sql.Connection

import com.dziuba.common.messagebroker.MessageType.OutputComputationKey
import com.dziuba.common.util.LogWrapper
import com.dziuba.job.process.ProcessTask.ProcessOutput

class ProcessTask private {

  def calculate(outputKey: OutputComputationKey, connection: Connection): Option[ProcessOutput] = {
    //implementation
  }

}

object ProcessTask {

  @transient lazy val Log: Logger = org.apache.log4j.LogManager.getRootLogger
  @transient val logger: LogWrapper = new LogWrapper(Log)

  case class GraphOutput()

  case class ProcessOutput(outputKey: OutputComputationKey, graphOutputs: Seq[GraphOutput], startTime: Long)

  def apply(): ProcessTask = new ProcessTask()

}
