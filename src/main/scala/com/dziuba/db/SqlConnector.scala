package com.dziuba.db

import java.sql.Connection

import com.dziuba.common.AppConfig

trait SqlConnector {

  def getConnection: Connection
}


object SqlConnector{
  type ConnectorFactory = (AppConfig) => SqlConnector
}