import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.{ActorMaterializer, Materializer}
import org.apache.kafka.common.serialization.StringDeserializer

object Main {

  def main(args: Array[String]): Unit = {

    println("Hi, This software will read messages from Kafka broker and displays them.")

    // Start actor system
    println("Creating Akka actor system and materializer ...")
    implicit val system: ActorSystem = ActorSystem("messageViewerSystem")
    implicit val materializer: Materializer = ActorMaterializer()

    // Kafka consumer options
    val bootstrapServers = "localhost:9094"
    val topic = "insurance-fl"

    println("Initializing Kafka consumer ...")

    val config = system.settings.config.getConfig("akka.kafka.consumer")
    val consumerSettings =
      ConsumerSettings(config, new StringDeserializer, new StringDeserializer)
        .withBootstrapServers(bootstrapServers)
        .withGroupId("group2")

    // Print all messages from Kafka source
    println("Reading messages ...")
    val consumer = Consumer
      .plainSource(consumerSettings, Subscriptions.topics(topic))
      .runForeach(println)

  }

}
