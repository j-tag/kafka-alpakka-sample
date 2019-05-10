import InsuranceJsonProtocol._
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.{ActorAttributes, ActorMaterializer, Materializer, Supervision}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import spray.json._

object Main {

  def main(args: Array[String]): Unit = {

    println("Hi, This software will read messages from Kafka broker and then shows the high amount population mid points.")

    // Actor system and materializer needed by Akka streams
    println("Creating Akka actor system and materializer ...")
    implicit val system: ActorSystem = ActorSystem("highValueFinderSystem")
    implicit val materializer: Materializer = ActorMaterializer()

    // Kafka options
    val bootstrapServers = "localhost:9094"
    val topic = "insurance-fl"

    println("Initializing Kafka consumer ...")

    // Consumer settings are used by Alpakka Kafka extension
    val config = system.settings.config.getConfig("akka.kafka.consumer")
    val consumerSettings =
      ConsumerSettings(config, new StringDeserializer, new StringDeserializer)
        .withBootstrapServers(bootstrapServers)
        .withGroupId("group1")

    // Spray JSON attribute to resume on errors
    val resumeOnParsingException = ActorAttributes.withSupervisionStrategy {
      new akka.japi.function.Function[Throwable, Supervision.Directive] {
        override def apply(t: Throwable): Supervision.Directive = t match {
          case _: spray.json.JsonParser.ParsingException => Supervision.Resume
          case _ => Supervision.stop
        }
      }
    }

    println("Reading messages ...")
    // Connect streams
    val consumer = Consumer
      .plainSource(consumerSettings, Subscriptions.topics(topic))
      .map { consumerRecord =>
        val value = consumerRecord.value()
        val sampleData = value.parseJson.convertTo[InsuranceItem]
        checkData(sampleData)
        sampleData
      }
      .withAttributes(resumeOnParsingException)
      .runForeach(checkData)

  }

  def checkData(insuranceItem: InsuranceItem): Unit = {

    // This method will check whether the item is high amount or not
    if(insuranceItem.eqSiteLimit > 50000.0) {
      println("High amount equality site limit found: " + insuranceItem)
    }

  }

}
