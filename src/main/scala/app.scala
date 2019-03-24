package PhenixChallenge

import com.typesafe.scalalogging.LazyLogging

import java.io.{FileWriter, PrintWriter}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

import scala.util.{Failure, Success, Try}
import scala.io.Source

import model._

case class Aggregate
  ( productQtiesByStore: Map[StoreId, Map[ProductId, Int]]
  , productRevenuesByStore: Map[StoreId, Map[ProductId, BigDecimal]]
  , overallProductQties: Map[ProductId, Int]
  , overallProductRevenues: Map[ProductId, BigDecimal]
  )

object Aggregate {

  def merge(x: Aggregate, y: Aggregate): Aggregate = {
    Aggregate(
      combine(x.productQtiesByStore, y.productQtiesByStore),
      combine(x.productRevenuesByStore, y.productRevenuesByStore),
      combine(x.overallProductQties, y.overallProductQties),
      combine(x.overallProductRevenues, y.overallProductRevenues)
    )
  }

  def fromDataSource(dataSource: String => Try[Source]): Try[Aggregate] = {
    dataSource("transactions")
      .map(getProductQtiesByStore)
      .map(
        productQtiesByStore => {
          val productRevenuesByStore = productQtiesByStore
            .map({ case (storeId, productQties) =>
              getProductRevenues(dataSource, storeId, productQties)
            })
            .filter(_.isSuccess)
            .map(_.get)
            .toMap

          val overallProductQties = productQtiesByStore.values.reduce(combine[Map[ProductId, Int]])
          val overallProductRevenues = productRevenuesByStore.values.reduce(combine[Map[ProductId, BigDecimal]])
          Aggregate(productQtiesByStore, productRevenuesByStore, overallProductQties, overallProductRevenues)
        }
      )
  }

  private def getProductQtiesByStore(transactionsSource: Source): Map[StoreId, Map[ProductId, Int]] = {
    transactionsSource.getLines()
      .toIterable
      .map(Transaction.parse(_).get)
      .groupBy(_.storeId)
      .mapValues(
        _.groupBy(_.productId)
        .mapValues(_.map(_.quantity).sum)
      )
  }

  private def getProductRevenues(dataSource: String => Try[Source], storeId: StoreId, productQties: Map[ProductId, Int]): Try[(StoreId, Map[ProductId, BigDecimal])] = {
    dataSource(s"reference_prod-${storeId.id}").map(
      referenceSource => {
        val prices = referenceSource.getLines()
          .map(Reference.parse(_).get)
          .map(r => (r.productId -> r.price))
          .toMap
        (storeId -> productQties.flatMap({
            case (pid, qty) =>
              for { price <- prices.get(pid) } yield (pid, price * qty)
          })
        )
      }
    )
  }

  private trait Combinable[T] {
    def combine(x: T, y: T): T
    def empty: T
  }

  private implicit object IntIsCombinable extends Combinable[Int] {
    def combine(x: Int, y: Int) = x + y
    def empty = 0
  }

  private implicit object BigDecimalIsCombinable extends Combinable[BigDecimal] {
    def combine(x: BigDecimal, y: BigDecimal) = x + y
    def empty = 0
  }

  private class MapIsCombinable[K, V](implicit comb: Combinable[V]) extends Combinable[Map[K, V]] {
    def combine(x: Map[K, V], y: Map[K, V]) = {
      x.foldLeft(y)({case (acc, (k, value)) =>
        acc + (k -> comb.combine(value, acc.getOrElse(k, comb.empty)))
      })
    }
    def empty = Map()
  }
  private implicit val ProductQtiesMapIsCombinable: Combinable[Map[ProductId, Int]]
    = new MapIsCombinable[ProductId, Int]
  private implicit val ProductRevenueMapIsCombinable: Combinable[Map[ProductId, BigDecimal]]
    = new MapIsCombinable[ProductId, BigDecimal]
  private implicit val QtiesByStoreMapIsCombinable: Combinable[Map[StoreId, Map[ProductId, Int]]]
    = new MapIsCombinable[StoreId, Map[ProductId, Int]]
  private implicit val RevenueByStoreMapIsCombinable: Combinable[Map[StoreId, Map[ProductId, BigDecimal]]]
    = new MapIsCombinable[StoreId, Map[ProductId, BigDecimal]]

  private def combine[T](x: T, y: T)(implicit comb: Combinable[T]) = {
    comb.combine(x, y)
  }
}

object Main extends App with LazyLogging {
  if(args.length < 2) {
    logger.error("USAGE : phenix-challenge <path_to_data> <output_dir> [<YYYY-MM-DD>]")
    System.exit(1)
  }
  val dataDirectory = args(0)
  val outputDirectory = args(1)
  val day = if(args.length == 3) LocalDate.parse(args(2)) else LocalDate.now()

  val dayString = formatDate(day)
  val tryDailyAggregate = Aggregate.fromDataSource(dataSource(dayString))
  tryDailyAggregate.foreach(
    dailyAggregate => {
      serializeTop100s(dailyAggregate, dayString)
      val weeklyAggregate = (1 to 6)
        .map(i => {
          Aggregate.fromDataSource(dataSource(formatDate(day.minusDays(i))))
        })
        .filter(_.isSuccess)
        .map(_.get)
        .foldLeft(dailyAggregate)(Aggregate.merge(_, _))
      serializeTop100s(weeklyAggregate, s"$dayString-J7")
    }
  )


  def dataSource(dayString: String)(name: String) = {
    // FIXME: Make path interoperable --SDF 2019-03-06
    Try(Source.fromFile(s"$dataDirectory/${name}_${dayString}.data"))
  }

  def formatDate(date: LocalDate) = {
    date.format(DateTimeFormatter.BASIC_ISO_DATE)
  }

  def serializeTop100s(aggregate: Aggregate, postfix: String) = {
    val top100QtyByStore = aggregate.productQtiesByStore.mapValues(getTop100(_))
    val top100RevenueByStore = aggregate.productRevenuesByStore.mapValues(getTop100(_))
    val top100QtyOverall = getTop100(aggregate.overallProductQties)
    val top100RevenueOverall = getTop100(aggregate.overallProductRevenues)

    top100QtyByStore.foreach {
      case (sid, top100) => serialize(s"ventes_${sid.id}_${postfix}", top100)
    }
    top100RevenueByStore.foreach {
      case (sid, top100) => serialize(s"ca_${sid.id}_${postfix}", top100)
    }

    serialize(s"ventes_GLOBAL_${postfix}", top100QtyOverall)
    serialize(s"ca_GLOBAL_${postfix}", top100RevenueOverall)
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
    using(new FileWriter(s"$outputDirectory/top_100_${name}.data")) {
      writer => using(new PrintWriter(writer)) {
        printer => top100.foreach({case (ProductId(pid), qty) => printer.println(s"$pid|$qty")})
      }
    }
}
