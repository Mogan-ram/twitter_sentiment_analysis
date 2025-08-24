package IngestionUtils

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties

object KafkaProducerUtil {
  def createProducer(): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers","localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all")
    new KafkaProducer[String, String](props)
  }

  def senToKafka(producer: KafkaProducer[String, String], topic: String, message: String): Unit = {
    val record = new ProducerRecord[String, String](topic, message)
    producer.send(record)
  }
}