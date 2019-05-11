package json

case class Point(latitude: Double, longitude: Double)
case class InsuranceItem(policyID: Int, county: String, eqSiteLimit: Double, line: String, point: Point)