package brightwell.sortable.challenge

import com.github.nscala_money.money.StaticCurrencyUnit
import org.joda.money.{CurrencyUnit, Money}
import play.api.libs.json._

/**
  * Created by kevin on 15/10/2016.
  */

case class Listing(title: String,
                   manufacturer: String,
                   price: Money) extends Ordered[Listing] {

  // {"title":"LED Flash Macro Ring Light (48 X LED) with 6 Adapter Rings for For Canon/Sony/Nikon/Sigma Lenses",
  // "manufacturer":"Neewer Electronics Accessories",
  // "currency":"CAD",
  // "price":"35.99"}

  def toLowerCase = {
    Listing(NGram.filterChars(title), NGram.filterChars(manufacturer), price)
  }

  import scala.math.Ordered.orderingToOrdered

  override def compare(that: Listing): Int =
    (manufacturer, title) compare (that.manufacturer, that.title)
}

object Listing {

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