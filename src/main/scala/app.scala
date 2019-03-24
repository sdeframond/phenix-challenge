package PhenixChallenge

import com.typesafe.scalalogging.LazyLogging

import java.io.{FileWriter, PrintWriter}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

import model._

object Main extends App with LazyLogging {

  if(args.length < 2) {
    logger.error("USAGE : phenix-challenge <path_to_data> <output_dir> [<YYYY-MM-DD>]")
    System.exit(1)
  }
  val dataDirectory = args(0)
  val outputDirectory = args(1)
  val day = if(args.length == 3) LocalDate.parse(args(2)) else LocalDate.now()
  val dayString = day.format(DateTimeFormatter.BASIC_ISO_DATE)

  val productQtiesByStore = dataSource("transactions")
    .getLines()
    .toIterable
    .map(Transaction.parse(_).get)
    .groupBy(_.storeId)
    .mapValues(
      _.groupBy(_.productId)
      .mapValues(_.map(_.quantity).sum)
    )

  val productRevenuesByStore = productQtiesByStore
    .map({ case (id, productQties) => {
      val prices = dataSource(s"reference_prod-${id.id}")
        .getLines()
        .map(Reference.parse(_).get)
        .map(r => (r.productId -> r.price))
        .toMap
      (id -> productQties
        .flatMap({case (pid, qty) => for { price <- prices.get(pid) } yield (pid, price * qty)})
      )
    }}).toMap

  val overallProductQties = productQtiesByStore.values.reduce(combine[Int])
  val overallProductRevenues = productRevenuesByStore.values.reduce(combine[BigDecimal])

  val top100QtyByStore = productQtiesByStore.mapValues(getTop100(_))
  val top100RevenueByStore = productRevenuesByStore.mapValues(getTop100(_))
  val top100QtyOverall = getTop100(overallProductQties)
  val top100RevenueOverall = getTop100(overallProductRevenues)

  top100QtyByStore.foreach {
    case (sid, top100) => serialize(s"ventes_${sid.id}", top100)
  }
  top100RevenueByStore.foreach {
    case (sid, top100) => serialize(s"ca_${sid.id}", top100)
  }

  serialize("ventes_GLOBAL", top100QtyOverall)
  serialize("ca_GLOBAL", top100RevenueOverall)

  /************************
   ** Method definitions **
   ************************/

  def dataSource(name: String) = {
    // FIXME: Make path interoperable --SDF 2019-03-06
    scala.io.Source.fromFile(s"$dataDirectory/${name}_${dayString}.data")
  }

  def combine[V](left: Map[ProductId, V], right: Map[ProductId, V])(implicit num: Numeric[V]) = {
    import num._
    left.foldLeft(right)({case (sumsByProduct, (pid, value)) =>
      sumsByProduct + (pid -> plus(value, sumsByProduct.getOrElse(pid, zero)))
    })
  }

  def getTop100[V <% Ordered[V]](valueByProduct: Map[ProductId, V]) =
    valueByProduct
      .toList
      .sortBy({case (pid, value) => value})
      .reverse
      .take(100)

  def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B =
    try { f(param) } finally { param.close() }

  def serialize[V](name: String, top100: List[(ProductId, V)]) =
    using(new FileWriter(s"$outputDirectory/top_100_${name}_${dayString}.data")) {
      writer => using(new PrintWriter(writer)) {
        printer => top100.foreach({case (ProductId(pid), qty) => printer.println(s"$pid|$qty")})
      }
    }
}
