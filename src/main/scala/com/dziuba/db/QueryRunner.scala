package com.dziuba.db

import java.math.BigDecimal
import java.sql.{Connection, ResultSet, Statement}

import com.dziuba.db.QueryRunner.{close, getStatement}
import com.dziuba.db.repository.DbMetaData.DbColumn
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

class QueryRunner(connection: Connection) {

  def loadString(query: String, field: String): String = {
    val stmt = getStatement(connection)
    val rs = stmt.executeQuery(query)

    val result =
      if (rs.next()) rs.getString(field)
      else ""

    close(rs, stmt)

    result
  }

  def loadBigDecimal(query: String, field: String): BigDecimal = {
    val stmt = getStatement(connection)
    val rs = stmt.executeQuery(query)

    val result =
      if (rs.next()) QueryRunner.getBigDecimal(field, rs)
      else BigDecimal.ZERO

    close(rs, stmt)

    result
  }

}

object QueryRunner {

  @transient protected lazy val logger =
    Logger(LoggerFactory.getLogger(getClass.getName))

  def close(connections: AutoCloseable*): Unit = connections.flatMap(Option(_)).foreach(_.close())

  def getLabel(rs: ResultSet): String = rs.getString(DbColumn.Code) + ": " + rs.getString(DbColumn.Name)

  def getStatement(connection: Connection): Statement = {
    connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
  }

  def withConnector[U](connector: SqlConnector)(f: (Connection) => U): U = {
    val connection = connector.getConnection
    try f(connection)
    finally QueryRunner.close(connection)
  }

  def executeBatch(connection: Connection)(createSqlQueries: => Seq[String]): Unit = {
    val statement = connection.createStatement
    createSqlQueries.foreach {
      q =>
        logger.debug(s"Execution query: $q")
        statement.addBatch(q)
    }
    statement.executeBatch()
    close(statement)
  }

  def getBigDecimal(field: String, rs: ResultSet): BigDecimal = {
    val value = rs.getBigDecimal(field)
    if (value != null) value
    else BigDecimal.ZERO
  }


  def count(selectSql: String)(implicit connector: SqlConnector): Int = {
    val conn = connector.getConnection
    val st = conn.createStatement()
    val rs = st.executeQuery(selectSql)
    var count = 0
    try {
      while (rs.next()) count += 1
      count
    } finally {
      QueryRunner.close(rs, st, conn)
    }
  }

  def collect[A](selectSql: String)(f: ResultSet => A)(implicit connector: SqlConnector): Seq[A] = {
    val conn = connector.getConnection
    val st = conn.createStatement()
    logger.info(">> SQL: " + selectSql)
    val rs = st.executeQuery(selectSql)
    try
      Iterator.continually(rs.next())
        .takeWhile(identity)
        .map(_ => f(rs))
        .toList
    finally {
      QueryRunner.close(rs, st, conn)
    }
  }
}