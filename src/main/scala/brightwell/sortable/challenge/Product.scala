package brightwell.sortable.challenge

import org.joda.time.DateTime
import play.api.libs.json.{JsPath, Json, Reads, Writes}
import play.api.libs.functional.syntax._

/**
  * Created by kevin on 15/10/2016.
  */

case class Product(name: String,
                   manufacturer: String,
                   model: String,
                   family: Option[String],
                   announced: DateTime) {

  // {"product_name":"Sony_Cyber-shot_DSC-W310","manufacturer":"Sony","model":"DSC-W310","family":"Cyber-shot","announced-date":"2010-01-06T19:00:00.000-05:00"}

  def toLowerCase = {
    Product(name.toLowerCase,
      manufacturer.toLowerCase,
      model.toLowerCase,
      family.map( _.toLowerCase ),
      announced)
  }

  private def boolCheck(weight: Double, valid: Double = 1.0, invalid: Double = 0.0)(check: Listing => Boolean): PartialFunction[Listing, (Double, Double)] = {
    case (listing: Listing) =>
      val checked = if (check(listing)) valid else invalid
      (checked * weight, valid * weight)
  }

  private val title_contains_manufacturer = boolCheck(1.0, invalid = -2) {
    case (listing: Listing) =>
      listing.title.contains(this.manufacturer.toLowerCase)
  }

  private val title_contains_model = boolCheck(1.0) {
    case (listing: Listing) =>
      listing.title.contains(this.model.toLowerCase)
  }

  private val manufacturer_matches = boolCheck(1.0) {
    case (listing: Listing) =>
      listing.manufacturer.contains(this.manufacturer.toLowerCase)
  }

  def similarity(listing: Listing): Double = {
    val lListing = listing.toLowerCase

    val checks = Seq(
      title_contains_manufacturer(lListing),
      title_contains_model(lListing),
      family.map { // handle family separate because if its not specified, can't penalize
        case (f: String) =>
          if (lListing.title.contains(f.toLowerCase))
            (1.0, 1.0)
          else
            (0.0, 1.0)
      }.getOrElse((0.0, 1.0)),
      manufacturer_matches(lListing)
    )

    val result = checks.reduce((r, t) => (r._1 + t._1, r._2 + t._2))
    result._1 / result._2
  }
}

object Product {

  implicit val dateWrites = Writes.jodaDateWrites("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
  implicit val dateReads = Reads.jodaDateReads("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

  // Json conversions:

  implicit val productWrites: Writes[Product] = (
    (JsPath \ "product_name").write[String] and
    (JsPath \ "manufacturer").write[String] and
    (JsPath \ "model").write[String] and
    (JsPath \ "family").writeNullable[String] and
    (JsPath \ "announced-date").write[DateTime]
  )(unlift(Product.unapply))

  implicit val productReads: Reads[Product] = (
    (JsPath \ "product_name").read[String] and
    (JsPath \ "manufacturer").read[String] and
    (JsPath \ "model").read[String] and
    (JsPath \ "family").readNullable[String] and
    (JsPath \ "announced-date").read[DateTime]
  )(Product.apply _)
}