import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.alpakka.mongodb.scaladsl.MongoSink
import akka.stream.{ActorAttributes, ActorMaterializer, Materializer, Supervision}
import com.mongodb.reactivestreams.client.MongoClients
import json.InsuranceJsonProtocol._
import json.{InsuranceItem, Point}
import org.apache.kafka.common.serialization.StringDeserializer
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import spray.json._

object Main {

  def main(args: Array[String]): Unit = {

    println("Hi, This software will read messages from Kafka broker and then saves them to database.")

    println("Creating Akka actor system and materializer ...")
    implicit val system: ActorSystem = ActorSystem("databaseSaverSystem")
    implicit val materializer: Materializer = ActorMaterializer()

    val bootstrapServers = "localhost:9094"
    val topic = "insurance-fl"

    println("Initializing Kafka consumer ...")

    val config = system.settings.config.getConfig("akka.kafka.consumer")
    val consumerSettings =
      ConsumerSettings(config, new StringDeserializer, new StringDeserializer)
        .withBootstrapServers(bootstrapServers)
        .withGroupId("group3")

    val resumeOnParsingException = ActorAttributes.withSupervisionStrategy {
      new akka.japi.function.Function[Throwable, Supervision.Directive] {
        override def apply(t: Throwable): Supervision.Directive = t match {
          case _: spray.json.JsonParser.ParsingException => Supervision.Resume
          case _ => Supervision.stop
        }
      }
    }

    println("Connecting to MongoDB ...")
    val codecRegistry = fromRegistries(fromProviders(classOf[InsuranceItem]), fromProviders(classOf[Point]),
      DEFAULT_CODEC_REGISTRY)

    val client = MongoClients.create("mongodb://hesam:secret@127.0.0.1:27017/insurance")

    val db = client.getDatabase("insurance")

    val insuranceCollection = db.getCollection("fl", classOf[InsuranceItem])
      .withCodecRegistry(codecRegistry)

    println("Reading messages ...")
    val consumer = Consumer
      .plainSource(consumerSettings, Subscriptions.topics(topic))
      .map { consumerRecord =>
        val value = consumerRecord.value()
        // Convert JSON string value back to object
        val insuranceItem = value.parseJson.convertTo[InsuranceItem]
        println("Saving item in database: " + insuranceItem)
        insuranceItem
      }
      .runWith(MongoSink.insertOne(insuranceCollection))

  }

}
