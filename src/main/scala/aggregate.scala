package PhenixChallenge.aggregate

import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success, Try}
import scala.io.Source

import PhenixChallenge.model._

object Types {
  type ProductMap[V] = Map[ProductId, V]
  type ProductQties = ProductMap[Int]
  type ProductRevenues = ProductMap[BigDecimal]
  type ProductQtiesByStore = Map[StoreId, ProductQties]
  type ProductRevenuesByStore = Map[StoreId, ProductRevenues]
}

object Helpers {
  implicit class TakeTop[V <% Ordered[V]](x: Types.ProductMap[V]) {
    def takeTop(n: Int) =
      x
        .toList
        .sortBy({case (pid, value) => value})
        .reverse
        .take(n)
  }
}

case class Aggregate
  ( productQtiesByStore: Types.ProductQtiesByStore
  , productRevenuesByStore: Types.ProductRevenuesByStore
  , overallProductQties: Types.ProductQties
  , overallProductRevenues: Types.ProductRevenues
  )

object Aggregate extends LazyLogging {
  import Types._

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

          val overallProductQties = productQtiesByStore.values.reduce(combine[ProductQties])
          val overallProductRevenues = productRevenuesByStore.values.reduce(combine[ProductRevenues])
          Aggregate(productQtiesByStore, productRevenuesByStore, overallProductQties, overallProductRevenues)
        }
      )
  }

  private def getProductQtiesByStore(transactionsSource: Source): ProductQtiesByStore = {
    transactionsSource.getLines()
      .toIterable
      .map(Transaction.parse(_).get)
      .groupBy(_.storeId)
      .mapValues(
        _.groupBy(_.productId)
        .mapValues(_.map(_.quantity).sum)
      )
  }

  private def getProductRevenues(dataSource: String => Try[Source], storeId: StoreId, productQties: ProductQties): Try[(StoreId, ProductRevenues)] = {
    dataSource(s"reference_prod-${storeId.id}") match {
      case Success(referenceSource) => {
        val prices = referenceSource.getLines()
          .map(Reference.parse(_).get)
          .map(r => (r.productId -> r.price))
          .toMap
        Success(storeId -> productQties.flatMap({
            case (pid, qty) =>
              for { price <- prices.get(pid) } yield (pid, price * qty)
          })
        )
      }

      case Failure(e) => Failure(e)
    }
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
      x.foldLeft(y)({case (acc, (k, value)) => {
          acc + (k -> comb.combine(value, acc.getOrElse(k, comb.empty)))
        }
      })
    }
    def empty = Map()
  }
  private implicit val productQtiesMapIsCombinable: Combinable[ProductQties]
    = new MapIsCombinable[ProductId, Int]
  private implicit val productRevenuesMapIsCombinable: Combinable[ProductRevenues]
    = new MapIsCombinable[ProductId, BigDecimal]
  private implicit val qtiesByStoreMapIsCombinable: Combinable[ProductQtiesByStore]
    = new MapIsCombinable[StoreId, ProductQties]
  private implicit val revenuesByStoreMapIsCombinable: Combinable[ProductRevenuesByStore]
    = new MapIsCombinable[StoreId, ProductRevenues]

  private def combine[T](x: T, y: T)(implicit comb: Combinable[T]) = {
    comb.combine(x, y)
  }
}
