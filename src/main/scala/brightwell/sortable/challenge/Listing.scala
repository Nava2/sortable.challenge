package brightwell.sortable.challenge

import com.github.nscala_money.money.StaticCurrencyUnit
import org.joda.money.{CurrencyUnit, Money}
import play.api.libs.json._

/**
  * Created by kevin on 15/10/2016.
  */

case class Listing(title: String,
                   manufacturer: String,
                   price: Money) {

  // {"title":"LED Flash Macro Ring Light (48 X LED) with 6 Adapter Rings for For Canon/Sony/Nikon/Sigma Lenses",
  // "manufacturer":"Neewer Electronics Accessories",
  // "currency":"CAD",
  // "price":"35.99"}

  def toLowerCase = {
    Listing(title.toLowerCase, manufacturer.toLowerCase, price)
  }

}

object Listing {

  case class Aggregate(others: Iterable[Listing]) {

    val allTitles: Set[String] = others.map(_.title.toLowerCase).toSet
    val title = reduceToCommon(allTitles)

    val allManufacturers = others.map(_.manufacturer.toLowerCase).toSet
    val manufacturer = reduceToCommon(allManufacturers)

    val prices = others.map(_.price).toSet

    def filterTo(product: Product) = {
      val sims = others.map {
        l => (l, product.similarity(l))
      }

      val newRes = sims.filter { _._2 > 4.0 } map { _._1 }

      if (newRes.nonEmpty) {
        Some(Aggregate(newRes))
      } else {
        None
      }
    }

    private def reduceToCommon(set: Set[String]) = {
      if (set.isEmpty) {
        None
      } else if (set.size == 1) {
        Some(set.head)
      } else {
        val r = set.takeRight(set.size - 1).fold(set.head) { (lcs, title) =>
          Distance.calcLongestCommon(lcs, title).toString
        }

        Some(r)
      }
    }

  }

  // Json conversions:

  implicit val listingWrites = new Writes[Listing] {

    override def writes(obj: Listing) = Json.obj(
        "title" -> obj.title,
        "manufacturer" -> obj.manufacturer,
        "currency" -> obj.price.getCurrencyUnit.toString,
        "price" -> obj.price.getAmount.toString
      )
  }

  implicit val listingReads = new Reads[Listing] {
    override def reads(json: JsValue): JsResult[Listing] = {
      val title = (json \ "title").validate[String] match {
        case s: JsSuccess[String] => s.get
        case e: JsError => return e
      }

      val manufacturer = (json \ "manufacturer").validate[String] match {
        case s: JsSuccess[String] => s.get
        case e: JsError => return e
      }

      val currency = (json \ "currency").validate[String] match {
        case s: JsSuccess[String] => StaticCurrencyUnit.of(s.get)
        case e: JsError => return e
      }

      val price = (json \ "price").validate[String] match {
        case s: JsSuccess[String] => Money.of(currency, s.get.toDouble)
        case e: JsError => return e
      }

      JsSuccess(Listing(title, manufacturer, price))
    }
  }
}