package com.dziuba.db.connector

import java.sql.Connection

import com.dziuba.db.SqlConnector
import com.dziuba.db.SqlConnector.ConnectorFactory
import org.apache.commons.dbcp2.BasicDataSource

case class JdbcConnector(driver: String, url: String, user: String, password: String) extends SqlConnector {
  val connectionPool = new BasicDataSource()
  Class.forName(driver)
  connectionPool.setDriverClassName(driver)
  connectionPool.setUsername(user)
  connectionPool.setPassword(password)
  connectionPool.setUrl(url)
  connectionPool.setInitialSize(1)

  def getConnection: Connection = {
    connectionPool.getConnection
  }
}

object JdbcConnector {

  def connectorFactory(): ConnectorFactory = (config) =>
    JdbcConnector(config.JdbcDriver, config.JdbcConnectionUrl, config.UserName, config.Password)

}