name := "database-saver"

version := "0.1"

scalaVersion := "2.12.7"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.21",
  "com.typesafe.akka" %% "akka-stream-kafka" % "1.0.2",
  "com.lightbend.akka" %% "akka-stream-alpakka-mongodb" % "1.0.0",
  "org.apache.kafka" % "kafka_2.11" % "2.1.1",
  "io.spray" %%  "spray-json" % "1.3.5",
  "org.mongodb" % "mongodb-driver-reactivestreams" % "1.11.0",
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.2.1"
)
