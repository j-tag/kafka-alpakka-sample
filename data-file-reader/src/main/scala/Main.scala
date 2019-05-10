import java.nio.file.Paths

import InsuranceJsonProtocol._
import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, IOResult, Materializer}
import akka.util.ByteString
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import spray.json._

object Main {

  def main(args: Array[String]): Unit = {

    println("Hi, This software will read CSV file and send messages to Kafka broker.")

    println("Opening CSV file ...")
    val file = Paths.get("../data/FL-insurance-sample.csv")

    println("Creating Akka actor system and materializer ...")
    implicit val system: ActorSystem = ActorSystem("insuranceProducerSystem")
    implicit val materializer: Materializer = ActorMaterializer()

    val bootstrapServers = "localhost:9094"
    val topic = "insurance-fl"

    println("Initializing Kafka producer ...")

    val config = system.settings.config.getConfig("akka.kafka.producer")
    val producerSettings =
      ProducerSettings(config, new StringSerializer, new StringSerializer)
        .withBootstrapServers(bootstrapServers)

    println("Sending data to Kafka broker ...")

    val done =
      FileIO.fromPath(file)
        .via(Framing.delimiter(ByteString("\n"), 256, allowTruncation = true))
        .drop(1) // First line is CSV headers, so we drop it
        .map { line =>
        val cols = line.utf8String.split(",").map(_.trim)
        val insuranceItem = InsuranceItem(cols(0).toInt, cols(2), cols(3).toDouble, cols(15), Point(cols(13).toDouble, cols(14).toDouble))
        println("Sending item to kafka broker: " + insuranceItem)
        new ProducerRecord[String, String](topic, insuranceItem.policyID.toString, insuranceItem.toJson.compactPrint)
        }
        .log("error logging")
        .runWith(Producer.plainSink(producerSettings))

  }

}
