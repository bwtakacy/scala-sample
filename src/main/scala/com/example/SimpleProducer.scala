package com.example

import java.util.Properties
import java.util.concurrent.{CountDownLatch, Executors}
import java.util.concurrent.atomic.AtomicLong
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}

object SimpleProducer {
	def main(args: Array[String]): Unit = {

		System.out.println("args.length = " + args.length)
		if (args.length != 4) {
			System.err.println(s"""
				|Usage: SimpleProducer <brokers> <topic> <numThreads> <numMessages>
				|  <brokers> is a list of one or more Kafka brokers
				|  <topic> is a kafka topic to produce into
				|  <numThreads> is a number of worker threads 
				|  <numMessages> is a number of messages produced in each threads 
				|
				""".stripMargin)
			System.exit(1)
		}
		val Array(brokerList, topicName, numThreadsStr, numMessagesStr) = args
		val numThreads = numThreadsStr.toInt
		val numMessages = numMessagesStr.toLong

		val executor = Executors.newFixedThreadPool(numThreads)
		val allDone = new CountDownLatch(numThreads)

		for ( i <- 0 until numThreads) {
			executor.execute(new ProducerThread(i, brokerList, topicName, numMessages, allDone))
		}

		allDone.await()
	}

	class ProducerThread(val threadId: Int, 
			val brokerList: String,
			val topic: String,
			val numMessages: Long,
			val allDone: CountDownLatch)
		extends Runnable {

		// generate Kafka producer
		val props = new Properties()
		props.put("metadata.broker.list", brokerList)
		props.put("serializer.class", "kafka.serializer.StringEncoder")
		props.put("request.required.acks", "1")

		val config = new ProducerConfig(props)
		val producer = new Producer[String, String](config)

		// generate message ID
		private val SEP = ":" // message field separator
		private val messageIdLabel = "MID"
		private val threadIdLabel = "TID"
		private val topicLabel = "Topic"

		private def generateMessageWithSeqId 
				(topic: String, msgId: Long) : KeyedMessage[String, String] = {
			val msg = new StringBuilder
			msg.append(topicLabel)
			msg.append(SEP)
			msg.append(topic)
			msg.append(threadIdLabel)
			msg.append(SEP)
			msg.append(threadId)
			msg.append(messageIdLabel)
			msg.append(SEP)
			msg.append(msgId)
			msg.append("<This is message from my simple producers.>")
			new KeyedMessage[String, String](topic, "SP", msg.result)
		}

		override def run {
			var i: Long = 0L

			while (i < numMessages) {
				try {
					producer.send(generateMessageWithSeqId(topic, i))
					Thread.sleep(100)
				} catch {
					case e: Throwable => sys.error("Error when sending message ")
				}
				i += 1
			}
			try {
				producer.close()
			} catch {
				case e: Throwable => sys.error("Error when closing producer")
			}
			allDone.countDown()
		}
	}
}
