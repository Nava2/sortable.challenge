package brightwell.sortable.challenge

import java.io.{File, FileOutputStream, OutputStreamWriter}
import java.net.URL

import com.google.common.base.Charsets
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import play.api.libs.json.Json

import scala.io.Source

object Main {

  private val DEFAULT_FAST_SIM_CUT_OFF = 2.5
  private val DEFAULT_SIM_CUT_OFF = 5.0

  private val DEFAULT_PRODUCTS_PATH = "/data/products.txt"
  private val DEFAULT_LISTING_PATH = "/data/listings.txt"

  private val DEFAULT_OUTPUT_PATH = new File("result.jsonlines")

  case class Config(productPath: Option[File] = None,
                    listingPath: Option[File] = None,
                    outputPath: File = DEFAULT_OUTPUT_PATH,
                    masterHost: Option[String] = None, localThreads: Int = 2,
                    fastSimilarityCutOff: Double = DEFAULT_FAST_SIM_CUT_OFF,
                    similarityCutOff: Double = DEFAULT_SIM_CUT_OFF)

  private def parseArgs(args: Array[String]): Config = {
    val parser = new scopt.OptionParser[Config]("sortable-challenge") {
      head("Sortable Challenge - Kevin Brightwell (@Nava2)")

      opt[File]('p', "products").valueName("<file>").
        action( (x, c) => c.copy(productPath = Some(x)) ).
        text("Path to products.txt file")

      opt[File]('l', "listings").valueName("<file>").
        action( (x, c) => c.copy(listingPath = Some(x)) ).
        text("Path to listings.txt")

      opt[File]('o', "out").valueName("<file>").
        action( (x, c) => c.copy(outputPath = x) ).
        text("Location to write output to.")

      opt[String]("master").valueName("<url>").
        action( (x, c) => c.copy(masterHost = Some(x)) ).
        text("Specify the master URL if a spark cluster is already known.")

      opt[Int]("localThreads").
        action( (x, c) => c.copy(localThreads = x) ).
        text("Specify the number of threads to run locally, does not work if --master is specified")

      opt[Double]("fastSim").
        action( (x, c) => c.copy(fastSimilarityCutOff = x) ).
        text(s"Fast Similarity cut off, default: $DEFAULT_FAST_SIM_CUT_OFF")

      opt[Double]("sim").
        action( (x, c) => c.copy(similarityCutOff = x) ).
        text(s"Similarity cut off, default: $DEFAULT_SIM_CUT_OFF")

      help("help").text("prints this usage text")
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        config
      case None =>
        throw new Error("Invalid configuration.")
    }
  }

  def main(args: Array[String]) {

    val cfg = parseArgs(args)

    val conf = new SparkConf().
      setAppName("Sortable Challenge")

    cfg.masterHost match {
      case Some(host) =>
        conf.setMaster(host)
      case None =>
        conf.setMaster(s"local[${cfg.localThreads}]")
    }

    val sc = new SparkContext(conf)

    /**
      * Reads a file path and creates an RDD from the data.
      * @param sc Spark Context in use
      * @param path Path of the file, reads the lines and puts each line
      *             in individually
      * @return
      */
    def read(sc: SparkContext, path: Either[File, String]) = {
      val dataLines = path match {
        case Left(f) =>
          Source.fromFile(f).getLines

        case Right(p) =>
          val url = Main.getClass.getResource(p)
          Source.fromURL(url)("UTF-8").getLines
      }

      val linesSeq = dataLines.toSeq
      val rdd = sc.parallelize(linesSeq)

      rdd.map(Json.parse)
    }

    val productsRDD = {
      import Product._

      val path = cfg.productPath match {
        case Some(file) => Left(file)
        case None => Right(DEFAULT_PRODUCTS_PATH)
      }
      read(sc, path).map(_.as[Product]).persist
    }

    val productsLength = productsRDD.count

    val listingsRDD = {
      import Listing._

      val path = cfg.listingPath match {
        case Some(file) => Left(file)
        case None => Right(DEFAULT_LISTING_PATH)
      }
      read(sc, path).map(_.as[Listing]).persist
    }

    val listingsLength = listingsRDD.count

    println(s"Products: $productsLength, listings: $listingsLength")

    val prodManNGrams = productsRDD.groupBy(p => NGram.from(p.manufacturer))

    val listingsByManufacturer = listingsRDD.groupBy(l => NGram.from(l.manufacturer))
      .cartesian(prodManNGrams.keys)
      .filter {
        case ((lman, _), pman) =>
          pman.check(lman) > 0.8
      }
      .map {
        case ((_, lists), manufacturer) =>
          (manufacturer, lists)
      }

    val listingsToProductAndFastSim =
      listingsByManufacturer.join(prodManNGrams)
        .flatMap {
          case (_, (lists, prods)) =>
            prods.map { p => (p, lists) }
        }
        .flatMap {
          case (p, lists) =>
            lists.map { l =>
              (l, (p, p.fastSimilarity(l)))
            }
        }
        .repartition((0.5 * listingsLength / productsLength).asInstanceOf[Int])(
          Ordering.by[(Listing, (Product, Double)), Listing](_._1))

    val listingsToProductAndSim =
      listingsToProductAndFastSim.filter {
        case (_, (_, s)) => s > cfg.fastSimilarityCutOff
      }
        .map {
          case (l, (p, _)) => (l, (p, p.similarity(l)))
        }
        .filter {
          case (_, (_, s)) => s > cfg.similarityCutOff
        }

    val resultWithSims = listingsToProductAndSim
      .reduceByKey {
        // group by the listing, keeping only the one with higher similarity
        case ((p1, s1), (p2, s2)) => if (s1 > s2) (p1, s1) else (p2, s2)
      }
      .map { case (l, (p, s)) => (p, (l, s)) }
      .sortBy { case (_, (_, s)) => -s }
      .groupByKey
      .cache

    resultWithSims.take(20)
      .foreach {
        case (p, lists) =>
          println(s"$p ->")
          lists.foreach { case (l, s) => println(s"\t[$s]\t$l") }
      }

    val result = resultWithSims.map {
      case (p, listSims) => ProductResult(p.name, listSims.map {
        _._1
      })
    }
      .cache

    val rproductCount = result.count
    val rlistingCount = result.map{ case (p1) => p1.listings.size }.sum

    println(s"Products: $rproductCount/$productsLength = ${(1.0 * rproductCount)/productsLength}")
    println(s"Listings: $rlistingCount/$listingsLength = ${(1.0 * rlistingCount)/listingsLength}")

    val jsonResult = result.map {
        r => Json.toJson(r)
      }
      .map { _.toString }
      .collect

    for {
      fstream <- resource.managed(new FileOutputStream(DEFAULT_OUTPUT_PATH))
      writer <- resource.managed(new OutputStreamWriter(fstream, Charsets.UTF_8))
    } {
      jsonResult.foreach {
          line =>
            writer.append(line)
            writer.append(System.getProperty("line.separator"))
        }
    }
  }
}