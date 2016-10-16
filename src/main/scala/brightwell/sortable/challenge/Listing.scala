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

  case class Aggregate(fuzzy: FuzzyEq, others: Iterable[Listing]) {

    val titles = Set(others.map(_.title.toLowerCase))
    val manufacturers = Set(others.map(_.manufacturer.toLowerCase))
    val prices = Set(others.map(_.price))

  }

  private val TITLE_THRESHOLD = 0.10

  case class FuzzyEq(l: Listing) {
    private val ll = l.toLowerCase
    private val dist = Distance(ll.title, TITLE_THRESHOLD)

    override def equals(o: scala.Any): Boolean = o match {
      case that: FuzzyEq =>
        val lthat = that.ll

//        val (sm, lg) = if (ll.manufacturer.length < lthat.manufacturer.length) {
//          (ll.manufacturer, lthat.manufacturer)
//        } else {
//          (lthat.manufacturer, ll.manufacturer)
//        }
//
//        if (!lg.contains(sm)) {
//          false
//        } else {
          dist.check(lthat.title) <= TITLE_THRESHOLD
//        }

      case _ => false
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