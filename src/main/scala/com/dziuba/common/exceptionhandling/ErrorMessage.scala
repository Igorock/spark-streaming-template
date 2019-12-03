package com.dziuba.common.exceptionhandling

import java.text.SimpleDateFormat
import java.util.Date

case class ErrorMessage[K](key: K, errorType: String, timestamp: String, message: String, value: String)

object ErrorMessage {

  val DateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def apply[K](key: K, errorType: String, timestamp: Date, message: String, value: String): ErrorMessage[K] = {
    ErrorMessage(key, errorType, DateTimeFormat.format(timestamp), message, value)
  }
}
