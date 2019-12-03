package com.dziuba.common.util

import java.sql.{Connection, ResultSet, ResultSetMetaData, Statement}

import scala.math.BigDecimal.RoundingMode

class TestConfig extends FlatSpec with MockitoSugar with BeforeAndAfter with LazyLogging {

  val AssertionBias = 0.02

  val stmt: Statement = mock[Statement]
  val rs: ResultSet = mock[ResultSet]
  val rsMetaData: ResultSetMetaData = mock[ResultSetMetaData]

  def roundUp(value: BigDecimal, scale: Int = 1): BigDecimal =
    if (value > 0) value.setScale(scale, RoundingMode.HALF_UP)
    else value.setScale(scale, RoundingMode.HALF_UP)

  def mockStatement(conn: Connection, stmt: Statement) =
    when(conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)).thenReturn(stmt)

//  def compare(expected: Map[Int, BigDecimal], actual: Map[ComputationField, BigDecimal])(field: String) =
//    ResultsComparator.compare(expected(field), actual(field))(field).result

  def mergeMap[A, B](ms: Seq[Map[A, B]])(f: (B, B) => B): Map[A, B] =
    (Map[A, B]() /: (for (m <- ms; kv <- m) yield kv)) { (a, kv) =>
      a + (if (a.contains(kv._1)) kv._1 -> f(a(kv._1), kv._2) else kv)
    }
}
