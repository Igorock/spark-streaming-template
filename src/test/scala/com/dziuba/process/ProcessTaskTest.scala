package com.dziuba.process

import java.sql.{Connection, ResultSet}

import com.dziuba.common.util.TestConfig
import com.dziuba.job.process.ProcessTask

class ProcessTaskTest extends TestConfig {

  implicit val conn: Connection = mock[Connection]
  val mockProcessTask = mock[ProcessTask]

  it should "test no processing if results not found" in {
    implicit val key = ComputationKey(1, 1, 1, 1, "c0")
    val outputMessage = ComputationMessage("Test", System.currentTimeMillis(), key)
    val rs = mock[ResultSet]
    when(rs.next).thenReturn(false)
    val rs2 = mock[ResultSet]
    when(rs2.next).thenReturn(false)
    when(conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)).thenReturn(stmt)
    when(stmt.executeQuery(UnitStateDataSetsQueries().FindQuery)).thenReturn(rs)
    when(stmt.executeQuery(UnitDataSetQueries().FindQuery)).thenReturn(rs2)
    when(mockProcessTask.calculate(outputMessage, conn)).thenCallRealMethod()


    val output = mockProcessTask.calculate(outputMessage, conn)
    assert(output.isEmpty)
  }
}
