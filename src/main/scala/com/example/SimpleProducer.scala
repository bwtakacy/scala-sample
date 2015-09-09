package com.example

import java.util.Properties
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}

object SimpleProducer {
    def main(args: Array[String]): Unit = {

	val topicName = "testTopic"
	println("connecting to %s".format(topicName))

	val props = new Properties()
	props.put("metadata.broker.list", "localhost:9092")
	props.put("serializer.class", "kafka.serializer.StringEncoder")
	props.put("request.required.acks", "1")

	val config = new ProducerConfig(props)
	val producer = new Producer[String, String](config)

	val data = new KeyedMessage[String, String](topicName, "one", "This is message from my simple producer") 
	producer.send(data)

	producer.close()
    }
}
