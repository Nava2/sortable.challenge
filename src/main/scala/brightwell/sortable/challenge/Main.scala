package brightwell.sortable.challenge

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import play.api.libs.json.Json
import resource._

import scala.io.Source

object Main {

  import Listing._
  import Product._

  private def PRODUCTS_PATH = "/data/products.txt"
  private def LISTING_PATH = "/data/listings.txt"

  def read(sc: SparkContext, path: String) = {
    val rdd = {
      val url = Main.getClass.getResource(path)
      val dataLines = Source.fromURL(url)("UTF-8").getLines
      val linesSeq = dataLines.toSeq
      sc.parallelize(linesSeq)
    }

    rdd.map(Json.parse)
  }



  def main(args: Array[String]) {
    val conf = new SparkConf().
      setAppName("Sortable Challenge").
      setMaster("local[8]")

    val sc = new SparkContext(conf)

    val products = read(sc, PRODUCTS_PATH).map(_.as[Product]).persist

    val productsLength = products.count

    val listings = read(sc, LISTING_PATH).map(_.as[Listing]).persist
    val listingsLength = listings.count

    println(s"Products: $productsLength, listings: ${listings.count}")

    val productsByLMan = products.groupBy(p => p.manufacturer.toLowerCase).persist
    println(s"Valid Manufacturers: ${productsByLMan.count}")

    val lcaseListings = listings.map {
      case (listing: Listing) =>
        (listing.toLowerCase, listing)
    }

    val SIM_CUT_OFF = 0.70

    // Get the mappings between of manufacturers, this is done to reduce the
    // amount of possible options for the ultimate (unfortunate) cartesian with
    // the listings data
    val prodManMapping = products.map( p => (p.manufacturer.toLowerCase, p))
      .groupByKey
      .persist // persist it so that after using the keys, can use the full set later

    val plMapping = prodManMapping.keys
      .cartesian(lcaseListings) // (manufacturer, ((list.man, list.title), list))
      .filter { // filter out based on if the manufacturer is found in the listing
        case (pman, (llisting, listing)) =>

          val (sm, lg) = if (llisting.manufacturer.length < pman.length) {
            (llisting.manufacturer, pman)
          } else {
            (pman, llisting.manufacturer)
          }

          lg.contains(sm) || llisting.title.contains(pman)
      }
      .map {
        case (pman, (_, listing)) => (pman, listing)
      }
      .join(prodManMapping) // ((manufacturer, Listing), Product)
      .flatMap {
        case (_, (listing, prods)) =>
          prods.map(product => (listing, (product, product.similarity(listing))))
            .filter {
              case (_, (_, sim)) => sim > SIM_CUT_OFF
            }
      }
      .reduceByKey((accum, that) => {
        if (accum._2 > that._2) {
          accum
        } else {
          that
        }
      })
      .map { case (listing, (p, sim)) => (p, (listing, sim)) }
      .groupByKey
      .persist

    val result = plMapping.map {
        case (p, pairs) => ProductResult(p.name, pairs.map(_._1).toSeq)
      }
      .persist

    val plMappingCount = plMapping.count()
    result.take(50)
      .foreach( p => println(s"${p.name}\t=>\t${p.listings}"))

    val liMappingCount = result.map( _.listings.length ).sum

    println(s"Mapped: ${1.0 * plMappingCount / productsLength}")
    println(s"\t${1.0 * liMappingCount / listingsLength}")

    val badListings = listings.subtract(plMapping.flatMap { case (_, vs) => vs.map( _._1 ) })
    badListings//.sortBy(_.title.toLowerCase)
        .take(100)
        .foreach(println)

    // TODO write json for `result`
//
//    listings.cartesian(productsByLMan)
//        .filter { case (listing, (lower, _)) =>
//          listing.manufacturer.toLowerCase.contains(lower) ||
//            listing.title.toLowerCase.contains(lower)
//        }
////        .map { case (listing, (lower, normal)) => (listing, products) }
//
//        .groupByKey // In theory, all products for a manufacturer are now contained
//        .foreach { case (m, arr) =>
//          println(s"$m: ${arr.size}")
//        }
  }
}