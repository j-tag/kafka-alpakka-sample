name := "heatmap-data-calculator"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.21",
  "com.lightbend.akka" %% "akka-stream-alpakka-mongodb" % "1.0.0",
  "org.mongodb" % "mongodb-driver-reactivestreams" % "1.11.0",
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.2.1"
)
