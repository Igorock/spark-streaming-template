package com.dziuba.job.persistence

import java.util.concurrent.Future

import com.dziuba.common.AppConfig
import com.dziuba.common.messagebroker.MessageProcessors._
import com.dziuba.common.messagebroker.MessageProducer.KafkaMessageProducer
import com.dziuba.common.messagebroker.MessageType._
import com.dziuba.common.util.TestConfig
import com.dziuba.db.entity.{ComputationKey, PersistenceKey}
import com.dziuba.job.persistence.PersistenceTask.PersistenceOutput
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, Matchers}

class PersistenceWriterTest extends TestConfig {

  var config: AppConfig = _
  var mockKafkaProducer: KafkaProducer[String, String] = _
  var mockMessageProducer: KafkaMessageProducer = _
  var mockPersistenceWriter: PersistenceWriter = _
  val key = ComputationKey(1, 2, -1, -1, "c0")
  before {
    config = AppConfig(
      "kafka.bootstrap-server" -> "1kafka:9092",
      "app.persistence.income-topic-prefix" -> "PERSISTENCE_",
      "app.persistence.export-topic-prefix" -> "EXPORT_",
      "app.persistence.topic-tables" -> "table",
      "spark.processing-time" -> "1000",
      "app.flow-through.income-topic" -> "FLOW_THROUGH",
      "app.process.income-topic" -> "REPORT_CALC"
    )

    mockKafkaProducer = mock[KafkaProducer[String, String]]
    mockMessageProducer = mock[KafkaMessageProducer]
    when(mockMessageProducer.getProducer).thenReturn(mockKafkaProducer)
    when(mockMessageProducer.logger).thenReturn(logger)

    mockPersistenceWriter = mock[PersistenceWriter]
    when(mockPersistenceWriter.config).thenReturn(config)
    when(mockPersistenceWriter.logger).thenReturn(logger)
    when(mockPersistenceWriter.messageProducer).thenReturn(mockMessageProducer)

  }

  it should "success w/ message PERSISTENCE_table" in {

    val record = ("PERSISTENCE_TABLE_NAME", "TABLE_NAME",
      s"""{"value":2, "id":${key.id}, "did": ${key.did}, "cid":"${key.cid}", "timestamp":1526308579659}""", 1)
      .asInstanceOf[InputMessageKey]
    val persistenceMessage = persistenceMessageProcessor.read(Undefined, record)
    val persistenceKey = persistenceMessage.body
    val output = PersistenceOutput(persistenceKey.topic, persistenceKey.table, persistenceMessage)

    when(mockPersistenceWriter.process(output)).thenCallRealMethod()
    when(mockMessageProducer.send(Matchers.eq(config.FlowThroughIncomeTopic), any[ComputationMessage])).thenCallRealMethod()
    when(mockMessageProducer.send(Matchers.eq(config.FlowThroughIncomeTopic), anyString(), anyString(), anyBoolean())).thenCallRealMethod()
    when(mockKafkaProducer.send(any())).thenReturn(mock[Future[RecordMetadata]])

    mockPersistenceWriter.process(output)
    val messageCaptor = ArgumentCaptor.forClass(classOf[ProducerRecord[String, String]])
    verify(mockKafkaProducer).send(messageCaptor.capture())

    val value = messageCaptor.getValue
    val message = computationMessageProcessor.read(Undefined, ("", "", value.value(), 1))
    assertResult(key)(message.body)


  }
  it should "success w/ message EXPORT_table" in {

    val record = PersistenceKey("EXPORT_table", "table", "key", "{\"value\":2}", None, Some(key))
    val output = PersistenceOutput(record.topic, record.table, PersistenceMessage("Test", System.currentTimeMillis(), record))

    when(mockPersistenceWriter.process(output)).thenCallRealMethod()
    when(mockKafkaProducer.send(any())).thenReturn(mock[Future[RecordMetadata]])

    mockPersistenceWriter.process(output)
    verify(mockKafkaProducer, never()).send(new ProducerRecord("EXPORT_table", record.key, record.rawMessage))

  }
  it should "success w/ message PERSISTENCE_other" in {
    val record = PersistenceKey("PERSISTENCE_other", "other", "key", "{\"value\":2}", None, Some(key))
    val output = PersistenceOutput(record.topic, record.table, PersistenceMessage("Test", System.currentTimeMillis(), record))

    when(mockPersistenceWriter.process(output)).thenCallRealMethod()
    when(mockKafkaProducer.send(any())).thenReturn(mock[Future[RecordMetadata]])

    mockPersistenceWriter.process(output)
    verify(mockKafkaProducer, never()).send(new ProducerRecord("PERSISTENCE_other", record.key, record.rawMessage))
  }

}