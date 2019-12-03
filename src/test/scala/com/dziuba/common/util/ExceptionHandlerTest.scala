package com.dziuba.common.util

import java.util.concurrent.Future

import com.dziuba.common.exceptionhandling.{ErrorMessage, ExceptionHandler, ParamNotFoundException}
import com.dziuba.common.messagebroker.MessageProcessors.JsonMessageHelper
import com.dziuba.common.messagebroker.MessageProducer
import com.dziuba.db.entity.{ComputationField, ComputationKey, EntityNotFoundException}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.mockito.Matchers.any
import org.mockito.Mockito.{never, verify, when}

class ExceptionHandlerTest extends TestConfig {
  var mockKafkaProducer: KafkaProducer[String, String] = _
  var mockMessageProducer: MessageProducer = _
  var mockHandler: ExceptionHandler = _
  var mockedException: Exception = _
  var mockedParamException: ParamNotFoundException = _
  var mockedEntityNotFound: EntityNotFoundException = _
  var mockComputationField: ComputationField = _
  private val errorLogTopic = "ERROR_LOG"
  val key = ComputationKey(1, 1, 1, 1, "c0")

  before {

    mockedException = mock[Exception]
    mockedParamException = mock[ParamNotFoundException]
    mockComputationField = mock[ComputationField]
    mockKafkaProducer = mock[KafkaProducer[String, String]]
    mockMessageProducer = mock[MessageProducer]

    mockHandler = mock[ExceptionHandler]
    when(mockHandler.getMessageProducer).thenReturn(mockMessageProducer)
    when(mockHandler.getTime).thenReturn(ErrorMessage.DateTimeFormat.parse("2018-04-01 00:00:00"))
    when(mockHandler.errorTopic).thenReturn(errorLogTopic)
    when(mockHandler.logger).thenReturn(logger)
  }

  it should "bigdecimal doesn't exist" in {
    when(mockedException.getMessage).thenReturn("missedBigDecimalException")
    when(mockHandler.handleMissedBigDecimal(key, "test", "table1")).thenCallRealMethod()
    when(mockKafkaProducer.send(any())).thenReturn(mock[Future[RecordMetadata]])

    mockHandler.handleMissedBigDecimal(key, "test", "table1")
    assertResult("""{"key":"key","errorType":"Big decimal values doesn't exist","timestamp":"2018-04-01 00:00:00","message":"missedBigDecimalException","value":"test"}""")(JsonMessageHelper.serialize(
      ErrorMessage("key", "Big decimal values doesn't exist", mockHandler.getTime, mockedException.getMessage, "test")))
    verify(mockKafkaProducer, never()).send(new ProducerRecord(errorLogTopic, mockedException.getMessage, ""))
  }


  it should "check division by zero" in {
    when(mockedException.getMessage).thenReturn("divisionException")
    when(mockHandler.handleDivisionByZero(mockedException, mockComputationField, key)).thenCallRealMethod()
    when(mockKafkaProducer.send(any())).thenReturn(mock[Future[RecordMetadata]])
    mockHandler.handleDivisionByZero(mockedException, mockComputationField, key)
    assertResult(s"""{"key":"","errorType":"Division by zero","timestamp":"2018-04-01 00:00:00","message":"divisionException","value":"$mockComputationField"}""")(JsonMessageHelper.serialize(
      ErrorMessage("", "Division by zero", mockHandler.getTime, mockedException.getMessage, mockComputationField.toString)))
    verify(mockKafkaProducer, never()).send(new ProducerRecord(errorLogTopic, mockedException.getMessage, ""))
  }

  it should "param value not found" in {
    when(mockedParamException.getMessage).thenReturn("paramNofFoundException")
    when(mockHandler.handleMissedParam(mockedParamException)).thenCallRealMethod()
    when(mockKafkaProducer.send(any())).thenReturn(mock[Future[RecordMetadata]])

    mockHandler.handleMissedParam(mockedParamException)
    assertResult(s"""{"key":"key","errorType":"Param doesn't exist for value","timestamp":"2018-04-01 00:00:00","message":"paramNofFoundException","value":"value"}""")(JsonMessageHelper.serialize(
      ErrorMessage("key", "Param doesn't exist for value", mockHandler.getTime, mockedParamException.getMessage, "value")))
    verify(mockKafkaProducer, never()).send(new ProducerRecord(errorLogTopic, mockedParamException.getMessage, ""))
  }

  it should "send and log SQLException" in {
    when(mockedParamException.getMessage).thenReturn("SQLException")
    when(mockHandler.handleMissedParam(mockedParamException)).thenCallRealMethod()
    when(mockKafkaProducer.send(any())).thenReturn(mock[Future[RecordMetadata]])

    mockHandler.handleSQLException(mockedException, "key")
    assertResult(s"""{"key":"key","errorType":"Error inserting data","timestamp":"2018-04-01 00:00:00","message":"SQLException","value":""}""")(JsonMessageHelper.serialize(
      ErrorMessage("key", "Error inserting data", mockHandler.getTime, mockedParamException.getMessage, "")))
    verify(mockKafkaProducer, never()).send(new ProducerRecord(errorLogTopic, mockedParamException.getMessage, ""))
  }

  it should "send and log MissedEntity" in {
    mockedEntityNotFound = new EntityNotFoundException(getClass, key, mockedException)
    when(mockHandler.handleMissedEntity(mockedEntityNotFound)).thenCallRealMethod()
    when(mockKafkaProducer.send(any())).thenReturn(mock[Future[RecordMetadata]])

    mockHandler.handleMissedEntity(mockedEntityNotFound)
    assertResult(s"""{"key":"key","errorType":"Error inserting data","timestamp":"2018-04-01 00:00:00","message":"Entity com.dziuba.common.util.ExceptionHandlerTest not found for ComputationKey(1,1,1,1,c0)","value":""}""")(JsonMessageHelper.serialize(
      ErrorMessage("key", "Error inserting data", mockHandler.getTime, mockedEntityNotFound.getMessage, "")))
    verify(mockKafkaProducer, never()).send(new ProducerRecord(errorLogTopic, mockedEntityNotFound.getMessage, ""))
  }

  it should "send and log OtherException" in {
    when(mockedParamException.getMessage).thenReturn("OtherException")
    when(mockHandler.handleOtherException(mockedException, "key")).thenCallRealMethod()
    when(mockKafkaProducer.send(any())).thenReturn(mock[Future[RecordMetadata]])

    mockHandler.handleOtherException(mockedException, "key")
    assertResult(s"""{"key":"key","errorType":"Error inserting data","timestamp":"2018-04-01 00:00:00","message":"OtherException","value":""}""")(JsonMessageHelper.serialize(
      ErrorMessage("key", "Error inserting data", mockHandler.getTime, mockedParamException.getMessage, "")))
    verify(mockKafkaProducer, never()).send(new ProducerRecord(errorLogTopic, mockedParamException.getMessage, ""))
  }
}

