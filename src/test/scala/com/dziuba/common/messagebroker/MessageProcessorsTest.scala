package com.dziuba.common.messagebroker

import com.dziuba.common.exceptionhandling.ErrorMessage
import com.dziuba.common.messagebroker.MessageProcessors.{JsonComputationMessageProcessor, JsonMessageHelper, JsonPersistenceMessageProcessor, MessageProcessor}
import com.dziuba.common.messagebroker.MessageType._
import com.dziuba.common.util.TestConfig
import com.dziuba.db.entity.{ComputationKey, PersistenceKey}

class MessageProcessorsTest extends TestConfig {


  "Writing-PersistenceMessage" should "serialize Row with lowercase field names" in {
    val row = ComputationKey(1, 2, 3, 4, "5")
    val jsonKey = JsonMessageHelper.serialize(row)
    assertResult("""{"id":1,"did":2,"eid":3,"seid":4,"cid":"5"}""")(jsonKey)
  }

  val persistenceMessageProcessor: MessageProcessor[PersistenceMessage] = JsonPersistenceMessageProcessor()

  it should "serialize partial ComputationKey without negative values" in {
    val key = ComputationKey(1, 2, -1, -1, "c0")
    val actualJson = s"""{"id": 1, "did": 2, "eid": -1, "seid": -1, "cid": "c0"}"""
    val expectedJson = s"""{"id": 1, "did": 2, "eid": -1, "seid": -1, "cid": "c0"}"""

    val jsonKey = JsonMessageHelper.serialize(key)

    assertResult("""{"id":1,"did":2,"cid":"c0"}""")(jsonKey)
  }

  it should "serialize PersistenceMessage with old version" in {
    val key = PersistenceKey("PERSISTENCE_TEST_TABLE", "TEST_TABLE", "Key", """{"id":1,"did":2,"eid":3,"seid":4,"cid":"5", "timestamp": 1526025353145}""")
    val jsonPersistenceMessage = JsonMessageHelper.serialize(key)
    val persistenceMessage = persistenceMessageProcessor.read(Undefined, ("", "", jsonPersistenceMessage, 0))
    assertResult(jsonPersistenceMessage)(persistenceMessage.body.rawMessage)
  }

  it should "serialize PersistenceMessage with version 0.1" in {
    val key = PersistenceKey("PERSISTENCE_TEST_TABLE", "TEST_TABLE", "Key", """{"id":1,"did":2,"eid":3,"seid":4,"cid":"5"}""")
    val jsonPersistenceMessage = persistenceMessageProcessor.write(PersistenceMessage("Test", System.currentTimeMillis(), key))
    println(jsonPersistenceMessage)
    val persistenceMessage = persistenceMessageProcessor.read(Undefined, ("", "", jsonPersistenceMessage, 0))
    println(persistenceMessage)
    assertResult(key)(persistenceMessage.body)
  }

  "Writing-ErrorMessage" should "serialize ErrorMessage" in {
    val error = ErrorMessage("1", "1", ErrorMessage.DateTimeFormat.parse("2018-04-01 00:00:00"), "1", "1")
    val jsonError = JsonMessageHelper.serialize(error)
    assertResult("""{"key":"1","errorType":"1","timestamp":"2018-04-01 00:00:00","message":"1","value":"1"}""")(jsonError)
  }

  val computationMessageProcessor: MessageProcessor[ComputationMessage] = JsonComputationMessageProcessor()

  "Reading-ComputationMessage" should "test creation of ComputationKey from json string" in {
    val json1 = s"""{ "system":"REMOTE", "timestamp":1,  "body":{"id": 1, "did": 2, "eid": 3, "seid": 4, "cid": "c0"}, "version":"${MessageType.CurrentVersion}" }"""
    val json2 = s"""{ "system":"REMOTE", "timestamp":1,  "body":{"id": 1, "did": 2, "eid": 3, "seid": 4, "cid": "c1"}, "version":"${MessageType.CurrentVersion}" }""""
    val json3 = s"""{ "system":"REMOTE", "timestamp":1,  "body":{"id": 1, "did": 2, "eid": 3, "seid": 4, "cid": "c1"}, "version":"${MessageType.CurrentVersion}" }""""

    val expectedKey1 = ComputationKey(1, 2, 3, 4, "c0")
    val expectedKey2 = ComputationKey(1, 2, 3, 4, "c1")

    val kafkaMessage1 = ("", "", json1, 1).asInstanceOf[InputMessageKey]
    val kafkaMessage2 = ("", "", json2, 1).asInstanceOf[InputMessageKey]
    val kafkaMessage3 = ("", "", json3, 1).asInstanceOf[InputMessageKey]

    assertResult(expectedKey1)(computationMessageProcessor.read(Undefined, kafkaMessage1).body)
    assertResult(expectedKey2)(computationMessageProcessor.read(Undefined, kafkaMessage2).body)
    assertResult(expectedKey2)(computationMessageProcessor.read(Undefined, kafkaMessage3).body)
  }

}
