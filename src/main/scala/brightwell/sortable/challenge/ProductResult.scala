package brightwell.sortable.challenge

import play.api.libs.json.{Json, Writes}

/**
  * Created by kevin on 16/10/2016.
  */
case class ProductResult(name: String, listings: Seq[Listing]) {

}

object ProductResult {

  // Json conversions:

  implicit val resultWrites = new Writes[ProductResult] {
    override def writes(obj: ProductResult) = Json.obj(
      "product_name" -> obj.name,
      "listings" -> obj.listings
    )
  }

}
