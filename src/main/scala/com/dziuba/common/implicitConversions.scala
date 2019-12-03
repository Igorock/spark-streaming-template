package com.dziuba.common

import com.dziuba.common.messagebroker.MessageType.Undefined

object implicitConversions {

  implicit def double2BigDecimalWithScale(b: Double): BigDecimal = BigDecimal(b)

  implicit class MapWithAnyValuesConversion(map: Map[String, Any]) {
    def tryAsString(name: String, default: String = Undefined): String =
      map.getOrElse(name, default).asInstanceOf[String]

    def tryAsBigInt(name: String, default: Int = -1): BigInt = {
      def convert(value: Option[Any], default: Int = -1): BigInt = {
        value match {
          case Some(value: BigInt) => value
          case Some(value: Int) => BigInt(value)
          case Some(value: Long) => BigInt(value)
          case Some(value: String) if value.forall(_.isDigit) => BigInt(value)
          case None => default
        }
      }

      convert(map.get(name), default)
    }

    def tryAsInt(name: String, default: Int = -1): Int = tryAsBigInt(name, default).toInt

    def tryAsLong(name: String, default: Int = -1): Long = tryAsBigInt(name, default).toLong
  }

}