# Classes

* SimpleProducer

```
Usage: SimpleProducer <brokers> <topic> <numThreads> <numMessages>
  <brokers> is a list of one or more Kafka brokers
  <topic> is a kafka topic to produce into
  <numThreads> is a number of worker threads
  <numMessages> is a number of messages produced in each threads
```

This is a simple Kafka producer implementation.
Scala Version: 2.10.5
Kafka Version: 0.8.2.1

# How to buid and run

* get typesafe-activator or sbt
* set build.sbt and source files
* execute below command 

```
$ activator run "localhost:9092 test 2 10"
```
or

```
$ sbt "run localhost:9092 test 2 10"
```
