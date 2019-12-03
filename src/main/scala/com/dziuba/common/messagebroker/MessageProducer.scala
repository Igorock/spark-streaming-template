package com.dziuba.common.messagebroker

import com.dziuba.common.AppConfig
import com.dziuba.common.exceptionhandling.ErrorMessage
import com.dziuba.common.messagebroker.MessageProcessors._
import com.dziuba.common.messagebroker.MessageType.OutputComputationKey
import com.dziuba.common.metrics.MetricsSystem._
import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}


trait MessageProducer extends Serializable {

  def send(topic: String, key: String, value: String, async: Boolean = true): Unit

  def send(topic: String, message: OutputComputationKey): OutputComputationKey

  def send[K](topic: String, message: ErrorMessage[K]): ErrorMessage[K]

  def stop(): Unit
}

object MessageProducer {

  case class KafkaMessageProducer(config: AppConfig) extends MessageProducer {

    lazy val logger: Logger = Logger(classOf[KafkaMessageProducer])

    import scala.collection.JavaConversions._

    @transient lazy val producer: Producer[String, String] = {
      val kafkaProducer = new KafkaProducer[String, String](
        Map(
          "bootstrap.servers" -> config.KafkaBootstrapServer,
          "client.id" -> "DZ",
          "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
          "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"))

      sys.addShutdownHook {
        kafkaProducer.close()
      }

      kafkaProducer
    }

    def isStarted: Boolean = true

    override def send(topic: String, key: String, value: String, async: Boolean = true): Unit = {
      logger.info(s"Sending message w/ key [$key] to topic [$topic] ")

      val future = getProducer.send(new ProducerRecord[String, String](topic, key, value))
      if (!async) future.get()

      logger.info(s"Message w/ key [$key] was sent.")
      logger.info(s"Message w/ value [$value] was sent.")
    }

    override def send(topic: String, message: OutputComputationKey): OutputComputationKey = {
      val computationKey = message.body
      send(topic, message.uid, computationMessageProcessor.write(message))
      logger.info(s"Publishing computation message")
      logger.info(s"Computation time for ${message.system} was ${message.totalDuration} ms, initial time is ${message.initialDateTime} (${message.initialTimeMillis.getOrElse(-1L)}), start time is ${message.startDateTime} (${message.timestamp})")
      if (message.totalDuration > 0)
        cdcRecordComputationTime.update(message.totalDuration)
      message
    }

    override def send[K](topic: String, message: ErrorMessage[K]): ErrorMessage[K] = {
      send(topic, message.message, JsonMessageHelper.serialize(message))
      message
    }

    override def stop(): Unit = {
      getProducer.flush()
      getProducer.close()
    }


    def getProducer: Producer[String, String] = producer
  }

  implicit lazy val messageProducer: MessageProducer = KafkaMessageProducer(AppConfig())

}

