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

    // This file will be contained of JSON values that represents the coordinates of
    // map points with their magnitude to be shown in a map view in web browser
    val geoJsonFilePath = "../web/eqSiteLimit-map-view/fl-insurance-locations.geojson"

    // Actor system startup
    println("Creating Akka actor system and materializer ...")
    implicit val system: ActorSystem = ActorSystem("heatMapDataCalculatorSystem")
    implicit val materializer: Materializer = ActorMaterializer()

    println("Connecting to MongoDB ...")
    // Database models
    val codecRegistry = fromRegistries(fromProviders(classOf[InsuranceItem]), fromProviders(classOf[Point]),
      DEFAULT_CODEC_REGISTRY)

    // Database credentials
    val client = MongoClients.create("mongodb://hesam:secret@127.0.0.1:27017/insurance")

    val db = client.getDatabase("insurance")

    val insuranceCollection = db.getCollection("fl", classOf[InsuranceItem])
      .withCodecRegistry(codecRegistry)

    println("Reading data from database ...")

    // Read all data from collection
    // NOTE: If the collection has a massive amount of data, you should limit is so the generated file will be
    // accessible through web browser, otherwise you maybe faced with some memory issues
    val source: Source[InsuranceItem, NotUsed] =
      MongoSource(insuranceCollection.find(classOf[InsuranceItem]).limit(100000))

    val rows: Future[Seq[InsuranceItem]] = source.runWith(Sink.seq)

    implicit val ec: ExecutionContextExecutor = system.dispatcher

    // After fetching all rows, we can start processing data
    rows.map { seqInsuranceItems =>

      println("Converting and saving data to file ...")

      // Using file channel give us a better speed in generating output file
      val stream = new RandomAccessFile(geoJsonFilePath, "rw")
      implicit val channel: FileChannel = stream.getChannel
      // Remove past data
      channel.truncate(0)

      // This inner function used to write in file in a handy dandy manner
      def writeToChannel(value: String)(implicit channel: FileChannel): Unit = {
        println("Writing item to file: " + value)
        val strBytes = value.getBytes
        val buffer = ByteBuffer.allocate(strBytes.length)
        buffer.put(strBytes)
        buffer.flip
        channel.write(buffer)
      }

      // This will be placed on top of file
      val headerString = """{
                           |"type": "FeatureCollection",
                           |"crs": { "type": "name", "properties": { "name": "urn:ogc:def:crs:OGC:1.3:CRS84" } },
                           |"features": [
                           |""".stripMargin

      writeToChannel(headerString)

      // Here we use last item policy number to detect whenever we should not place comma at the end of the string
      // Personally I hate this approach, but on the other hand I can not find any better idea here
      // So if you can make it better, please give me hand !
      val lastItemPolicyID = seqInsuranceItems.last.policyID

      // These items will be placed in the middle of file
      seqInsuranceItems.foreach{ insuranceItem =>

        val baseString =  s"""{ "type": "Feature", "properties": { "mag": ${"%.2f".format(insuranceItem.eqSiteLimit / 10000000) }}, "geometry": { "type": "Point", "coordinates": [ ${insuranceItem.point.longitude}, ${insuranceItem.point.latitude} ] } } """

        val value = if(insuranceItem.policyID == lastItemPolicyID) {
          baseString + "\n"
        } else {
          baseString + ",\n"
        }

        writeToChannel(value)

      }

      // And at the end we will place this footer
      val footerString = """]
                           |}
                           |""".stripMargin

      writeToChannel(footerString)

      // And finally don't forget to free up resources
      stream.close()

    }.map{_ =>
      println("Data were saved in .geojson file. Now you can view map web page in your browser.")
      // Bye ;)
      system.terminate()
    }

  }

}
