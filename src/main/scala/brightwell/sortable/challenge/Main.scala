package brightwell.sortable.challenge

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import play.api.libs.json.Json

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

    val SIM_CUT_OFF = 5.0

    val prodManNGrams = products.groupBy( p => NGram.from(p.manufacturer) )

    val resultWithSims = listings.groupBy( l => NGram.from(l.manufacturer) )
      .cartesian(prodManNGrams.keys)
      .filter {
        case ((lman, lists), pman) =>
          NGram.check(pman, lman) > 0.8
      }
      .map {
        case ((_, lists), manufacturer) =>
          (manufacturer, lists)
      }
      .mapValues(Listing.Aggregate)
      .join(prodManNGrams)
      .flatMap {
        case (_, (agg, prods)) =>
          prods.map { p => agg.others.map { l => (l, (p, p.similarity(l))) } }
      }
      .flatMap { it => it }
      .reduceByKey {
        case ((p1, s1), (p2, s2)) => if (s1 > s2) (p1, s1) else (p2, s2)
      }
      .filter { case (l, (p, s)) => s > SIM_CUT_OFF }
      .map { case (l, (p, s)) => (p, (l, s)) }
      .sortBy { case (_, (_, s)) => -s }
      .groupByKey
      .cache

    val result = resultWithSims.map {
        case (p, listSims) => ProductResult(p.name, listSims.map { _._1 })
      }
      .cache

    val rproductCount = result.count
    val rlistingCount = result.map{ case (p1) => p1.listings.size }.sum

    resultWithSims.take(20)
      .foreach {
        case (p, lists) =>
          println(s"$p ->")
          lists.foreach { case (l, s) => println(s"\t[$s]\t$l") }
      }

    println(s"Products: $rproductCount/$productsLength = ${(1.0 * rproductCount)/productsLength}")
    println(s"Products: $rlistingCount/$listingsLength = ${(1.0 * rlistingCount)/listingsLength}")

    sc.stop()

    System.exit(0)
//      .map {
//        case (p, lists) =>
//          val model = NGram.filterChars(p.model)
//          (p, lists.filter( l => NGram.filterChars(l.title).contains(model)))
//      }   // now only have listings with the products model
//      .cache

//    aggListings.flatMap {
//      case (p, lists) =>
//        lists.map( l1 => (l1, lists.filter( _ != l1 )))
//            .map {
//              case (l1, l2Set) =>
//                l2Set.filter( l2 => Distance.calc(l1.title.toLowerCase
//            }
//    }

    // Get the mappings between of manufacturers, this is done to reduce the
    // amount of possible options for the ultimate (unfortunate) cartesian with
    // the listings data
//    val prodManMapping = products.map( p => (p.manufacturer.toLowerCase, p))
//      .groupByKey
//      .persist // persist it so that after using the keys, can use the full set later

//    val plMapping = products.groupBy(p => NGram.from(p.manufacturer))
//      .cartesian(listings.groupBy(l => NGram.from(l.manufacturer)))
//      .filter {
//        case ((pMan, prods), (lMan, list)) =>
//          NGram.check(pMan, lMan) > 0.75
//      }
//      .flatMap {
//        case ((_, prods), (_, list)) =>
//          list.map( (_, prods) )
//      }
//      .flatMap {
//        case (list, prods) =>
//          prods.map( p => (list, p))
//      }
//      .map { case (l, p) => (l, (p, p.similarity(l))) }
//      .filter { // Given that these "connected" bits can be large, it is useful to filter early
//        case (_, (_, sim)) => sim > SIM_CUT_OFF
//      }
//
////    val plMapping = products
////      .cartesian(listings)
////      .map {
////        case (product, listing) =>
////          (listing, (product, product.similarity(listing)))
////      }
//      .reduceByKey((accum, that) => {
//        // Since it is given that a Product-Listing relationship is 1-M, then a listing can only have one Product
//        // We use the most likely candidate rather than intentionally passing a false-positive (as per the spec)
//        if (accum._2 > that._2) {
//          accum
//        } else {
//          that
//        }
//      })
//      .map { case (listing, (p, sim)) => (p, (listing, sim)) } // rearrange and group on the product
//      .groupByKey
//      .persist
//
//    val result = plMapping.map {
//        case (p, pairs) => ProductResult(p.name, pairs.map(_._1).toSeq)
//      }
//      .persist
//
//    val plMappingCount = plMapping.count()
//    plMapping.take(50)
//      .foreach{ case (p, lists) =>
//        println(s"$p\t=>\t[${lists.size}]")
//        lists.foreach {
//          case (l, sim) => println(s"\t$sim:\t$l")
//        }
//      }
//
//    val liMappingCount = result.map( _.listings.length ).sum
//
//    println(s"Mapped: ${1.0 * plMappingCount / productsLength}")
//    println(s"\t${1.0 * liMappingCount / listingsLength}")
//
//    sc.stop()

//    val badListings = listings.subtract(plMapping.flatMap { case (_, vs) => vs.map( _._1 ) })
//    badListings//.sortBy(_.title.toLowerCase)
//        .take(100)
//        .foreach(println)

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