package com.dziuba.common.util

import com.dziuba.db.entity.ComputationKey
import org.apache.log4j.Logger
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, FlatSpec}

class LogWrapperTest extends FlatSpec with MockitoSugar with BeforeAndAfter {

  it should "test logging" in {
    val Log = mock[Logger]
    val logWrapper = new LogWrapper(Log)
    val row = ComputationKey(1, 2, 3, 4, "c0")
    val reportType = 1
    val stateId = 3
    val startTime = logWrapper.startLogRowInfo("building", row, reportType, stateId)
    Thread.sleep(1050)
    logWrapper.finishLogRowInfo(startTime, "building", row, reportType, stateId)

    verify(Log).info(s"Started building. Key: ${classOf[ComputationKey].getSimpleName}(1,2,3,4,c0), reportTypeId = 1, stateid = 3")

    val captor = ArgumentCaptor.forClass(classOf[String])
    verify(Log, times(2)).info(captor.capture())
    assert(captor.getValue.startsWith("Finished building in: 1"))
    assert(captor.getValue.endsWith(s"Key: ${classOf[ComputationKey].getSimpleName}(1,2,3,4,c0), reportTypeId = 1, stateid = 3"))

    val startTime2 = logWrapper.startLogRowInfo("building", row)
    Thread.sleep(1050)
    logWrapper.finishLogRowInfo(startTime2, "building", row)

    verify(Log).info(s"Started building. Key: ${classOf[ComputationKey].getSimpleName}(1,2,3,4,c0)")
  }
}
