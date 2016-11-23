package brightwell.sortable.challenge

import com.google.common.base.{CharMatcher, Splitter}
import org.joda.time.DateTime
import play.api.libs.json.{JsPath, Json, Reads, Writes}
import play.api.libs.functional.syntax._
import collection.JavaConverters._

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
      NGram.filterChars(manufacturer),
      NGram.filterChars(model),
      family.map{ f => NGram.filterChars(f) },
      announced)
  }

  private def boolCheck(weight: Double, valid: Double = 1.0, invalid: Double = 0.0)(check: Listing => Boolean): PartialFunction[Listing, Double] = {
    case (listing: Listing) =>
      val checked = if (check(listing)) valid else invalid
      checked * weight
  }

  private def title_contains(listing: Listing) = {
    val titleGram = NGram.from(listing.title)

    val ngMan = NGram.from(manufacturer)
    val ngFamilyOpt = family.map(NGram.from)
    val ngModel = NGram.from(model)

    val nullPair = (NGram.empty, 0.0)

    val comboChecks = Seq(
        // [Manufacturer Model]
        ngFamilyOpt.map(f => (ngMan ++ f, 2.0)).getOrElse(nullPair),
        // [Man Family Model] => 3.0
        ngFamilyOpt.map(f => (ngMan ++ f ++ ngModel, 5.0)).getOrElse(nullPair),
        // [Family Model] => [2.0]
        ngFamilyOpt.map(f => (f ++ ngModel, 3.0)).getOrElse(nullPair),
        (ngMan ++ ngModel, 3.0)
      )
      .map {
        case (ng, v) => v * (if (ng.check(titleGram) == 1.0) 1.0 else 0.0)
      }
      .sum

    val eqChecks = Seq(
        (ngMan, 1.0, 0.0),
        ngFamilyOpt.map(f => (f, 1.0, -1.0)).getOrElse((NGram.empty, 0.0, 0.0)),
        (ngModel, 0.5, 0.0) // weight model information strongly
      )
      .map {
        case (ng, good, bad) =>
          if (ng.nonEmpty && ng.check(titleGram) < 0.8) bad else good
      }
      .sum

    comboChecks + eqChecks
  }

  def similarity(listing: Listing): Double = {
    val lListing = listing.toLowerCase

    val checks = Seq(
//      title_contains_manufacturer(lListing),
      title_contains(lListing),
      if (lListing.manufacturer.contains(this.manufacturer.toLowerCase)) 0.0 else -1.0
    )

    val result = checks.sum
    result
  }

  def fastSimilarity(listing: Listing): Double = {
    val thatL = this.toLowerCase

    val lListing = listing.toLowerCase
    val checks = Seq(
      if (lListing.manufacturer.contains(thatL.manufacturer)) 2.0 else 0.0,
      if (lListing.title.contains(thatL.manufacturer)) 0.5 else 0.0,
      if (lListing.title.contains(thatL.model)) 0.5 else 0.0,
      if (thatL.family.exists( f => lListing.title.contains(f) )) 0.5 else 0.0
    )

    checks.sum
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