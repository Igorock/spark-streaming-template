package com.dziuba.common.util

import java.sql.Connection

import com.dziuba.db.SqlConnector
import com.dziuba.db.SqlConnector.ConnectorFactory
import org.mockito.Mockito.{mock, when, withSettings}

object stubs {

  case class StubSqlConnectorStub() extends SqlConnector {

    override def getConnection: Connection = null
  }

  def stubSqlConnectorFactory: ConnectorFactory = (_) => {
    val mockConnection = mock(classOf[Connection], withSettings().serializable())
    val mockSqlConnector = mock(classOf[StubSqlConnectorStub], withSettings().serializable())
    when(mockSqlConnector.getConnection).thenReturn(mockConnection)
    mockSqlConnector
  }
}