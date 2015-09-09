name := """simple-kafka"""

version := "1.0"

scalaVersion := "2.10.5"

// Change this to another test framework if you prefer
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"
libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.2.1" 

// Uncomment to use Akka
//libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.11"

