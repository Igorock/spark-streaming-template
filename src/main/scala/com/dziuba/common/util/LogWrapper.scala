package com.dziuba.common.util

import java.util.Calendar

import com.dziuba.db.entity.ComputationKey
import org.apache.log4j.Logger

class LogWrapper(log: Logger) {

  def startLogRowInfo(message: String, row: ComputationKey, reportType: Int = 0, stateId: Int = 0): Long = {
    logRowInfo("Started " + message, row, reportType, stateId)
    Calendar.getInstance().getTimeInMillis
  }

  def finishLogRowInfo(startTime: Long, message: String, row: ComputationKey, reportType: Int = 0, stateId: Int = 0): Long = {
    val endTime = Calendar.getInstance().getTimeInMillis
    logRowInfo("Finished " + message + " in: " + (endTime - startTime) + " ms", row, reportType, stateId)
    Calendar.getInstance().getTimeInMillis
  }

  def logRowInfo(message: String, row: ComputationKey, reportType: Int, stateId: Int): Unit = {
    val stateStr = if (stateId > 0) ", stateid = " + stateId else ""
    val reportTypeStr = if (reportType > 0) ", reportTypeId = " + reportType else ""
    log.info(message + ". Key: " + row + reportTypeStr + stateStr)
  }

}
