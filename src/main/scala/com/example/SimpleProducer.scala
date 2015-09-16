package com.example

import java.util.Random
import java.util.Properties
import java.util.concurrent.{CountDownLatch, Executors}
import java.util.concurrent.atomic.AtomicLong
import java.net.InetAddress
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
		val hostName = InetAddress.getLocalHost().getHostName()
		val executor = Executors.newFixedThreadPool(numThreads)
		val allDone = new CountDownLatch(numThreads)
		val rand = new java.util.Random

		for ( i <- 0 until numThreads) {
			executor.execute(new ProducerThread(hostName, i, brokerList, topicName, numMessages, rand, allDone))
		}

		allDone.await()
	}

	class ProducerThread(val hostName: String,
			val threadId: Int, 
			val brokerList: String,
			val topic: String,
			val numMessages: Long,
			val rand: Random,
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
		private val SEP = ":" // separator between field name and value
		private val FSEP = " " // separator between fields
		private val messageIdLabel = "MessageID"
		private val threadIdLabel = "ThreadID"
		private val topicLabel = "Topic"
		private val contentLabel = "CONTENT"
		private val hostNameLabel = "Host"

		private def generateMessageWithSeqId 
				(topic: String, msgId: Long) : KeyedMessage[String, String] = {
			val msg = new StringBuilder
			msg.append(hostNameLabel)
			msg.append(SEP)
			msg.append(hostName)
			msg.append(FSEP)
			msg.append(threadIdLabel)
			msg.append(SEP)
			msg.append(threadId)
			msg.append(FSEP)
			msg.append(messageIdLabel)
			msg.append(SEP)
			msg.append(String.format("%05d", long2Long(msgId)))
			msg.append(FSEP)
			//msg.append(contentLabel)
			//msg.append(SEP)
			//msg.append("<Zamzar>")
			new KeyedMessage[String, String](topic, msg.result, rand.nextInt.toString)
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
