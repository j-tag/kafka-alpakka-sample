import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.mongodb.scaladsl.MongoSource
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import com.mongodb.reactivestreams.client.MongoClients
import json.{InsuranceItem, Point}
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import scala.concurrent.{ExecutionContextExecutor, Future}

object Main {

  def main(args: Array[String]): Unit = {

    println("Hi, This software will read messages from MongoDB and saves them to a .geojson file, then it can viewed in web browser.")

    val geoJsonFilePath = "../web/eqSiteLimit-map-view/fl-insurance-locations.geojson"

    println("Creating Akka actor system and materializer ...")
    implicit val system: ActorSystem = ActorSystem("heatMapDataCalculatorSystem")
    implicit val materializer: Materializer = ActorMaterializer()

    println("Connecting to MongoDB ...")
    val codecRegistry = fromRegistries(fromProviders(classOf[InsuranceItem]), fromProviders(classOf[Point]),
      DEFAULT_CODEC_REGISTRY)

    val client = MongoClients.create("mongodb://hesam:secret@127.0.0.1:27017/insurance")

    val db = client.getDatabase("insurance")

    val insuranceCollection = db.getCollection("fl", classOf[InsuranceItem])
      .withCodecRegistry(codecRegistry)

    println("Reading data from database ...")

    val source: Source[InsuranceItem, NotUsed] =
      MongoSource(insuranceCollection.find(classOf[InsuranceItem]))

    val rows: Future[Seq[InsuranceItem]] = source.runWith(Sink.seq)

    implicit val ec: ExecutionContextExecutor = system.dispatcher

    rows.map { seqInsuranceItems =>

      println("Converting and saving data to file ...")

      val stream = new RandomAccessFile(geoJsonFilePath, "rw")
      implicit val channel: FileChannel = stream.getChannel
      channel.truncate(0)

      def writeToChannel(value: String)(implicit channel: FileChannel): Unit = {
        println("Writing item to file: " + value)
        val strBytes = value.getBytes
        val buffer = ByteBuffer.allocate(strBytes.length)
        buffer.put(strBytes)
        buffer.flip
        channel.write(buffer)
      }

      val headerString = """{
                           |"type": "FeatureCollection",
                           |"crs": { "type": "name", "properties": { "name": "urn:ogc:def:crs:OGC:1.3:CRS84" } },
                           |"features": [
                           |""".stripMargin

      writeToChannel(headerString)

      val lastItemPolicyID = seqInsuranceItems.last.policyID

      seqInsuranceItems.foreach{ insuranceItem =>

        val baseString =  s"""{ "type": "Feature", "properties": { "mag": ${"%.2f".format(insuranceItem.eqSiteLimit / 10000000) }}, "geometry": { "type": "Point", "coordinates": [ ${insuranceItem.point.longitude}, ${insuranceItem.point.latitude} ] } } """

        val value = if(insuranceItem.policyID == lastItemPolicyID) {
          baseString + "\n"
        } else {
          baseString + ",\n"
        }

        writeToChannel(value)

      }

      val footerString = """]
                           |}
                           |""".stripMargin

      writeToChannel(footerString)

      stream.close()

    }.map{_ =>
      println("Data were saved in .geojson file. Now you can view map web page in your browser.")
      system.terminate()
    }

  }

}
