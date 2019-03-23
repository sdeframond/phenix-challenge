import com.typesafe.scalalogging.LazyLogging

import java.nio.file.{Files, Paths}
import java.io.{FileWriter, PrintWriter}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

import scala.collection.JavaConverters._
import scala.collection.immutable.Map
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

case class Transaction
  ( id: TransactionId
  , time: LocalDateTime
  , storeId: StoreId
  , productId: ProductId
  , quantity: Int
  )

case class StoreId(id: String)
case class TransactionId(id: Int)
case class ProductId(id: Int)

object Transaction {
  def parse(string: String): Try[Transaction] = {
    try {
      string match {
        case regex(id, time, sid, pid, qty) =>
          Success(Transaction
            ( TransactionId(id.toInt)
            , LocalDateTime.parse(time, formatter)
            , StoreId(sid)
            , ProductId(pid.toInt)
            , qty.toInt
            )
          )
        case _ => Failure(new Exception(s"Failed to parse: $string"))
      }
    } catch {
      case e: Throwable => Failure(e)
    }
  }

  private val formatter = DateTimeFormatter.ofPattern("uuuuLLdd'T'HHmmssxxxx")
  private val regex = raw"(\d+)\|([^\|]+)\|([^\|]+)\|(\d+)\|(\d+)".r
}

case class Reference(productId: ProductId, price: BigDecimal)

object Reference {
  def parse(string: String): Try[Reference] = {
    try {
      string match {
        case regex(pid, price) =>
          Success(Reference
            ( ProductId(pid.toInt)
            , BigDecimal(price)
            )
          )
        case _ => Failure(new Exception(s"Failed to parse: $string"))
      }
    } catch {
      case e: Throwable => Failure(e)
    }
  }

  private val regex = raw"(\d+)\|(\d+\.\d{1,2})".r
}

object Main extends App with LazyLogging {

  if(args.length < 1) {
    logger.error("USAGE : phenix-challenge <path_to_root> [<YYYY-MM-DD>]")
    System.exit(1)
  }
  val rootDirectory = args(0)
  val day = if(args.length == 2) LocalDate.parse(args(1)) else LocalDate.now()
  val dayString = day.format(DateTimeFormatter.BASIC_ISO_DATE)

  def dataSource(name: String) = {
    // FIXME: Make path interoperable --SDF 2019-03-06
    scala.io.Source.fromFile(s"$rootDirectory/data/${name}_${dayString}.data")
  }

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

  def combine[V](left: Map[ProductId, V], right: Map[ProductId, V])(implicit num: Numeric[V]) = {
    import num._
    left.foldLeft(right)({case (sumsByProduct, (pid, value)) =>
      sumsByProduct + (pid -> plus(value, sumsByProduct.getOrElse(pid, zero)))
    })
  }

  val overallProductQties = productQtiesByStore.values.reduce(combine[Int])
  val overallProductRevenues = productRevenuesByStore.values.reduce(combine[BigDecimal])

  def getTop100[V <% Ordered[V]](valueByProduct: Map[ProductId, V]) =
    valueByProduct
      .toList
      .sortBy({case (pid, value) => value})
      .reverse
      .take(100)

  val top100QtyByStore = productQtiesByStore.mapValues(getTop100(_))
  val top100RevenueByStore = productRevenuesByStore.mapValues(getTop100(_))
  val top100QtyOverall = getTop100(overallProductQties)
  val top100RevenueOverall = getTop100(overallProductRevenues)

  def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B =
    try { f(param) } finally { param.close() }

  def serialize[V](name: String, top100: List[(ProductId, V)]) =
    using(new FileWriter(s"$rootDirectory/results/top_100_${name}_${dayString}.data")) {
      writer => using(new PrintWriter(writer)) {
        printer => top100.foreach({case (ProductId(pid), qty) => printer.println(s"$pid|$qty")})
      }
    }

  top100QtyByStore.foreach {
    case (sid, top100) => serialize(s"ventes_${sid.id}", top100)
  }
  top100RevenueByStore.foreach {
    case (sid, top100) => serialize(s"ca_${sid.id}", top100)
  }

  serialize("ventes_GLOBAL", top100QtyOverall)
  serialize("ca_GLOBAL", top100RevenueOverall)
}
