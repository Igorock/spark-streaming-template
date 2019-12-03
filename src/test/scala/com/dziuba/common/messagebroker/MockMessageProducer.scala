package com.dziuba.common.messagebroker

import com.dziuba.common.AppConfig
import com.dziuba.common.exceptionhandling.ErrorMessage
import com.dziuba.common.messagebroker.MessageProducer.KafkaMessageProducer
import com.dziuba.common.messagebroker.MessageType.OutputComputationKey
import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.producer.{MockProducer, Producer}
import org.apache.kafka.common.serialization.StringSerializer

case class MockMessageProducer(p: MessageProducer) extends MessageProducer {
  override def send(topic: String, key: String, value: String, async: Boolean): Unit = p.send(topic, key, value, async)

  override def send(topic: String, message: OutputComputationKey): OutputComputationKey = p.send(topic, message)

  override def send[K](topic: String, message: ErrorMessage[K]): ErrorMessage[K] = p.send(topic, message)

  override def stop(): Unit = p.stop()
}

object MockMessageProducer {

  implicit lazy val messageProducer: MessageProducer = new MockMessageProducer(new KafkaMessageProducer(AppConfig()) {
    override lazy val producer: Producer[String, String] = new MockProducer[String, String](true, new StringSerializer(), new StringSerializer())
    override lazy val logger: Logger = Logger(classOf[MockMessageProducer])
  })

}
