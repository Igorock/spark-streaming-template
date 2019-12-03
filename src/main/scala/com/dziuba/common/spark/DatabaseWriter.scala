package com.dziuba.common.spark

import com.dziuba.common.AppConfig
import com.dziuba.db.QueryRunner
import com.dziuba.db.connector.JdbcConnector
import org.apache.log4j.Logger
import org.apache.spark.sql.ForeachWriter

abstract class DatabaseWriter[T](config: AppConfig) extends ForeachWriter[T] {

  @transient lazy val Log: Logger = org.apache.log4j.LogManager.getRootLogger
  implicit var connection: java.sql.Connection = _

  override def open(partitionId: Long, version: Long): Boolean = {
    val jdbcConnector = JdbcConnector(config.JdbcDriver, config.JdbcConnectionUrl, config.UserName, config.Password)
    connection = jdbcConnector.getConnection
    !connection.isClosed
  }

  override def close(errorOrNull: Throwable): Unit = QueryRunner.close(connection)
}
