package PhenixChallenge.aggregate

import com.typesafe.scalalogging.LazyLogging

import java.io.{File, FileWriter, PrintWriter}

import scala.util.{Failure, Success, Try}
import scala.io.Source

import PhenixChallenge.model._
import PhenixChallenge.combine._

class TempFileManager extends LazyLogging {
  private var files: List[File] = List()
  private var closed = false

  def getNewFile: File = {
    if (closed) { throw new Exception("Cannot get a new file after closing manager") }
    val f = File.createTempFile("phenixAggregate", ".tmp")
    files = f :: files
    f
  }

  def close(): Unit = {
    files.foreach(f => {
      logger.debug(s"Deleting tmp file: $f")
      f.delete
    })
    closed = true
    files = List()
  }
}

class Aggregate
  ( private val tfm: TempFileManager
  , productData: Iterator[ProductDatum]
  ) extends LazyLogging {

  import Aggregate._

  private val source = {
    val file = tfm.getNewFile
    logger.debug(s"Serializing Aggregate to: $file")
    withPrinter(file, printer => {
      productData.foreach(x => {
        printer.println(x.serialize)
      })
    })
    logger.debug(s"Done serializing Aggregate to: $file")
    Source.fromFile(file)
  }

  def merge(other: Aggregate): Aggregate = {
    new Aggregate(tfm, IteratorCombinator.combine(data, other.data))
  }

  def getTopQties(n: Int): Iterator[ProductQty] = {
    val top = data |> combineByProduct |> externalSortBy(tfm, - _.qty) |> toQties
    top.take(n)
  }

  def getTopRevenues(n: Int): Iterator[ProductRevenue] = {
    val top = data |> combineByProduct |> externalSortBy(tfm, - _.revenueOption.getOrElse(BigDecimal(0))) |> toRevenues
    top.take(n)
  }

  def getTopQtiesByStore(n: Int): Map[StoreId, Iterator[ProductQty]] = {
    getTopByStore(n, x => (x.storeId, - x.qty))(data)
      .mapValues(toQties)
  }

  def getTopRevenuesByStore(n: Int): Map[StoreId, Iterator[ProductRevenue]] = {
    getTopByStore(n, x => (x.storeId, - x.revenueOption.getOrElse(BigDecimal(0))))(data)
      .mapValues(toRevenues)
  }

  private def data: Iterator[ProductDatum] = source.reset.getLines.map(ProductDatum.parse)
  private def toQties(xs: Iterator[ProductDatum]): Iterator[ProductQty] = xs.map(_.toQty)
  private def toRevenues(xs: Iterator[ProductDatum]): Iterator[ProductRevenue] = xs.flatMap(_.toRevenue)

  private def combineByProduct(values: Iterator[ProductDatum]): Iterator[ProductDatum] = {
    val it = (values |> externalSortBy(tfm, _.productId)).buffered

    new Iterator[ProductDatum] {
      def hasNext = it.hasNext
      def next() = {
        var x = it.next()

        while (it.hasNext && it.head.productId == x.productId) {
          x = x.combine(it.next())
        }

        x
      }
    }
  }

  private def getTopByStore[A <% Ordered[A]](n: Int, f: ProductDatum => A)(values: Iterator[ProductDatum]): Map[StoreId, Iterator[ProductDatum]] = {
    val sorted = externalSortBy(tfm, f)(values).buffered
    var map = Map[StoreId, Iterator[ProductDatum]]()
    while (sorted.hasNext) {
      val key = sorted.head.storeId
      var count = 0
      var top = Vector[ProductDatum]() // Using a vector because we assume n is small.

      // Take the first n values for the current store.
      do {
        val n = sorted.next
        top = top :+ n
        count += 1
      } while (sorted.hasNext && sorted.head.storeId == key && count < n)

      // Add those values to the map
      map = map + (key -> top.iterator)

      // Drop remaining values until we reach the next store.
      while (sorted.hasNext && sorted.head.storeId == key) {sorted.next}
    }
    map
  }

}

object Aggregate extends LazyLogging {
  import Ordering.Tuple2

  def apply(tfm: TempFileManager, dataSource: String => Try[Source]): Try[Aggregate] = {
    dataSource("transactions")
      .map(_.getLines()
        .map(Transaction.parse(_))
        |> externalSortBy(tfm, x => (x.storeId, x.productId))
        |> sumQtiesByStoreAndProduct
        |> joinWithReferences(dataSource)
        |> (productData => new Aggregate(tfm, productData))
      )
  }

  private type Qties = Iterator[(StoreId, ProductId, Int)]

  private def sumQtiesByStoreAndProduct(_txs: Iterator[Transaction]): Qties = new Qties {
    private val txs = _txs.buffered
    def hasNext = txs.hasNext
    def next = {
      val tx = txs.next
      var sum = tx.quantity
      while (txs.hasNext
             && txs.head.storeId == tx.storeId
             && txs.head.productId == tx.productId) {
        sum += txs.next.quantity
      }
      (tx.storeId, tx.productId, sum)
    }
  }

  private def joinWithReferences(dataSource: String => Try[Source])
                                (qties: Qties): Iterator[ProductDatum]
                                = new Iterator[ProductDatum] {
    def hasNext = qties.hasNext
    def next = {
      val (sid, pid, qty) = qties.next
      val revenueOption = reference(sid, pid).map(_.price * qty)
      new ProductDatum(sid, pid, qty, revenueOption)
    }

    private var referenceSource: Option[(StoreId, Option[BufferedIterator[Reference]])] = None
    private def reference(sid: StoreId, pid: ProductId): Option[Reference] = {
      // val refsOption = updateReferenceSource(sid)
      val refsOption = referenceSource match {

        // We failed to open this source last time we tried.
        case Some((previousStoreId, None)) => {
          // Update the source only if we are looking for a different one.
          if (previousStoreId == sid) None else updateReferenceSource(sid)
        }

        // We opened a reference source previously, with success.
        case Some((previousStoreId, cached)) => {
          // Update the source only if we are looking for a different one.
          // updateReferenceSource(sid)
          if (previousStoreId == sid) cached else updateReferenceSource(sid)
        }

        // It is the first time that we are looking for a reference.
        case None => updateReferenceSource(sid)
      }

      refsOption.flatMap(refs => {
        while(refs.hasNext && refs.head.productId < pid) {
          refs.next()
        }
        if (refs.hasNext && refs.head.productId == pid) {
          Some(refs.head)
        } else {
          logger.error(s"Reference not found: ${sid}, ${pid} ")
          None
        }
      })
    }

    private def updateReferenceSource(sid: StoreId): Option[BufferedIterator[Reference]] = {
      val refsOption = dataSource(s"reference_prod-${sid.id}")
                       .toOption
                       .map(_.getLines.map(Reference.parse(_)).buffered)
      referenceSource = Some((sid, refsOption))
      refsOption
    }
  }

  private def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B =
      try { f(param) } finally { param.close() }

  private def withPrinter[B](file: File, f: PrintWriter => B): B = {
    using(new PrintWriter(new FileWriter(file)))(f)
  }

  implicit private class ThrushOperator[A](x: A) {
    def |>[B](g: A => B): B = {
      g(x)
    }
  }

  private def externalSortBy[T <: Serializable, A](tfm: TempFileManager, f: T => A)
                                          (it: Iterator[T])
                                          (implicit ordA: Ordering[A],
                                           parse: String => T) : Iterator[T] = {
    implicit val ordT = Ordering.by(f)
    val parts = it.grouped(1000 * 1000) // TODO soft-code this constant
      .map(slice => {
        val file = tfm.getNewFile
        withPrinter(file, printer =>
          slice.sorted.map(_.serialize).foreach(printer.println(_))
        )
        Source.fromFile(file).getLines.map(parse(_))
      })

    parts.reduce((x, y) => new IteratorKMerger(x, y))
  }
}

case class ProductDatum(storeId: StoreId,
                        productId: ProductId,
                        qty: Int,
                        revenueOption: Option[BigDecimal]) extends Combinable[ProductDatum]
                                                         with Ordered[ProductDatum]
                                                         with Serializable {
  def toQty: ProductQty = ProductQty(storeId, productId, qty)

  def toRevenue: Option[ProductRevenue] = {
    revenueOption.map(revenue => ProductRevenue(storeId, productId, revenue))
  }

  def combine(other: ProductDatum): ProductDatum = {
    val newPriceOption = revenueOption.flatMap(a => other.revenueOption.map(b => a + b))
    ProductDatum(storeId, productId, qty + other.qty, newPriceOption)
  }

  def compare(other: ProductDatum): Int = {
    (storeId, productId).compare(other.storeId, other.productId)
  }

  def serialize: String = {
    val priceStr = revenueOption match {
      case Some(price) => price.toString
      case None => ProductDatum.noneString
    }
    s"${storeId.id}|${productId.id}|$qty|$priceStr"
  }
}

object ProductDatum {
  implicit def parse(str: String): ProductDatum = {
    val parts = str.split("""\|""")
    val price = if (parts(3) == noneString) {
      None
    } else {
      Some(BigDecimal(parts(3)))
    }
    ProductDatum(StoreId(parts(0)), ProductId(parts(1).toInt), parts(2).toInt, price)
  }

  private val noneString = "None"
}

trait ProductValue[T] {
  val productId: ProductId
  val value: T
}

case class ProductQty(storeId: StoreId, productId: ProductId, value: Int)
extends ProductValue[Int]

case class ProductRevenue(storeId: StoreId, productId: ProductId, value: BigDecimal)
extends ProductValue[BigDecimal]
