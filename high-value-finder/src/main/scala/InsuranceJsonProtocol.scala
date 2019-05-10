import spray.json._

case class Point(latitude: Double, longitude: Double)
case class InsuranceItem(policyID: Int, county: String, eqSiteLimit: Double, line: String, point: Point)

// This object will tell the Spray JSON how convert our data to JSON
object InsuranceJsonProtocol extends DefaultJsonProtocol {
  implicit val pointFormat: RootJsonFormat[Point] = jsonFormat(Point, "latitude", "longitude")
  implicit val insuranceFormat: RootJsonFormat[InsuranceItem] = jsonFormat(InsuranceItem, "policyID", "county",
    "eqSiteLimit", "line", "point")
}