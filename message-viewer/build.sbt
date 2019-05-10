name := "message-viewer"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka_2.11" % "2.1.1",
  "com.typesafe.akka" %% "akka-stream" % "2.5.21",
  "com.typesafe.akka" %% "akka-stream-kafka" % "1.0.2",
  "io.spray" %%  "spray-json" % "1.3.5"
)
