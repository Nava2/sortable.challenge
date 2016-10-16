package brightwell.sortable.challenge

import org.joda.money.{CurrencyUnit, Money}
import org.joda.time.DateTime
import org.scalatest._

class DataTest extends FlatSpec with Matchers {

  "Listing.toLowerCase" should "return a version with all fields in lowercase" in {
    val list = Listing(title = "Panasonic Lumix DMC-FH20 14.1 MP Digital Camera with " +
      "8x Optical Image Stabilized Zoom and 2.7-Inch LCD (Black)",
      manufacturer = "Panasonic",
      price = Money.of(CurrencyUnit.CAD, 244.55))

    list.toLowerCase should be(Listing(list.title.toLowerCase, list.manufacturer.toLowerCase, list.price))
  }

  "Given a matching product and item" should "have a higher result than wrong." in {

    val product = Product(name="Sony_Cyber-shot_DSC-W310",
      manufacturer="Sony",
      model = "DSC-W310",
      family = Some("Cyber-shot"),
      announced = DateTime.parse("2010-01-06T19:00:00.000-05:00"))

    val trueMatch = Listing(title = "Sony DSC-W310 12.1MP Digital Camera with 4x " +
        "Wide Angle Zoom with Digital Steady Shot Image Stabilization " +
        "and 2.7 inch LCD (Silver)",
        manufacturer = "Sony",
        price = Money.of(CurrencyUnit.CAD, 139.99))
    val goodManOnly = Listing(title = "Sony DSC-W570B Black 16MP, HD Video, " +
        "5x Zoom Digital Camera with 2.7\" Screen",
        manufacturer = "Sony",
        price = Money.of(CurrencyUnit.CAD, 199.99))

    val allBad = Listing(title = "Panasonic Lumix DMC-FH20 14.1 MP Digital Camera with " +
        "8x Optical Image Stabilized Zoom and 2.7-Inch LCD (Black)",
        manufacturer = "Panasonic",
        price = Money.of(CurrencyUnit.CAD, 244.55))

    product.similarity(trueMatch) should be(1.0)
    product.similarity(goodManOnly) should be(0.5)
    product.similarity(allBad) should be(-2.0)
  }
}