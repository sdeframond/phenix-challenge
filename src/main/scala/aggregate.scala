package PhenixChallenge.aggregate

import com.typesafe.scalalogging.LazyLogging

import java.io.{File, FileWriter, PrintWriter}

import scala.util.{Failure, Success, Try}
import scala.io.Source

import PhenixChallenge.model._

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

private object Utils {
  def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B =
      try { f(param) } finally { param.close() }

  def withPrinter[B](file: File, f: PrintWriter => B): B = {
    using(new PrintWriter(new FileWriter(file)))(f)
  }

  implicit class ThrushOperator[A](x: A) {
    def |>[B](g: A => B): B = {
      g(x)
    }
  }

  def sortedExternally[T <: Serializable](tfm: TempFileManager)
                                         (it: Iterator[T])
                                         (implicit ord: Ordering[T],
                                          parse: String => T) : Iterator[T] = {
    it.grouped(1000 * 1000) // TODO soft-code this constant
      .map(slice => {
        val file = tfm.getNewFile
        withPrinter(file, printer =>
          slice.sorted.map(_.serialize).foreach(printer.println(_))
        )
        Source.fromFile(file).getLines.map(parse(_))
      }) |> kWayMerge[T]
  }

  def externalSortBy[T <: Serializable, A](tfm: TempFileManager, f: T => A)
                                          (it: Iterator[T])
                                          (implicit ordA: Ordering[A],
                                           parse: String => T) : Iterator[T] = {
    implicit val ordT = Ordering.by(f)
    sortedExternally(tfm)(it)
  }

  private def kWayMerge[T](parts: Iterator[Iterator[T]])
                          (implicit ord: Ordering[T]): Iterator[T] = {
    parts.reduce((x, y) => new IteratorKMerger(x, y))
  }
}

class Aggregate
  ( private val tfm: TempFileManager
  , private val productQties: ProductQties
  , private val productRevenues: ProductRevenues
  ) extends LazyLogging {

  def merge(other: Aggregate): Aggregate = {
    new Aggregate(
      new TempFileManager,
      this.productQties.combine(other.productQties),
      this.productRevenues.combine(other.productRevenues)
    )
  }

  def getTopQties(n: Int): Iterator[ProductQty] = {
    productQties.combineByProduct.getTop(n)
  }

  def getTopRevenues(n: Int): Iterator[ProductRevenue] = {
    productRevenues.combineByProduct.getTop(n)
  }

  def getTopQtiesByStore(n: Int): Map[StoreId, Iterator[ProductQty]] = {
    productQties.groupByStore.mapValues(_.getTop(n))
  }

  def getTopRevenuesByStore(n: Int): Map[StoreId, Iterator[ProductRevenue]] = {
    productRevenues.groupByStore.mapValues(_.getTop(n))
  }
}

object Aggregate extends LazyLogging {
  import Utils._

  def apply(tfm: TempFileManager, dataSource: String => Try[Source]): Try[Aggregate] = {
    dataSource("transactions")
      .map(_.getLines()
        .map(Transaction.parse(_))
        |> externalSortBy(tfm, x => (x.storeId, x.productId))
        |> ProductQties(tfm)
        |> (qties => new Aggregate(tfm, qties, ProductRevenues(tfm, dataSource, qties)))
      )
  }
}

trait Combinable[T] {
  def combine(other: T): T;
}

class IteratorCombinator[T <: Combinable[T]](_xs: Iterator[T], _ys: Iterator[T])(implicit ord: Ordering[T])
extends Iterator[T] {
  private val xs = _xs.buffered
  private val ys = _ys.buffered

  def hasNext = xs.hasNext || ys.hasNext

  def next() = (xs.hasNext, ys.hasNext) match {
    case (true, true) => {
      val x = xs.head
      val y = ys.head
      val comp = ord.compare(x, y)
      if (comp < 0) {
        xs.next()
      } else if (comp > 0) {
        ys.next()
      } else {
        xs.next()
        ys.next()
        x.combine(y)
      }
    }
    case (true, false) => xs.next
    case (false, true) => ys.next
    case (false, false) => ys.next // This should throw an error
  }
}

class IteratorKMerger[T](_xs: Iterator[T], _ys: Iterator[T])(implicit ord: Ordering[T])
extends Iterator[T] {
  private val xs = _xs.buffered
  private val ys = _ys.buffered

  def hasNext = xs.hasNext || ys.hasNext

  def next() = (xs.hasNext, ys.hasNext) match {
    case (true, true) => {
      val x = xs.head
      val y = ys.head
      val comp = ord.compare(x, y)
      if (comp <= 0) {
        xs.next()
      } else {
        ys.next()
      }
    }
    case (true, false) => xs.next
    case (false, true) => ys.next
    case (false, false) => ys.next // This should throw an error
  }
}

trait ProductValue[T] {
  val productId: ProductId
  val value: T
}

case class ProductQty(storeId: StoreId, productId: ProductId, value: Int)
extends Combinable[ProductQty] with Ordered[ProductQty] with ProductValue[Int] with Serializable {

  def combine(other: ProductQty) = ProductQty(storeId, productId, value + other.value)

  def compare(other: ProductQty) = {
    (this.storeId -> this.productId) compare (other.storeId -> other.productId)
  }

  def serialize = s"${storeId.id}|${productId.id}|${value}"
}

private object ProductQty {
  implicit def parse(string: String): ProductQty = {
    string match {
      case regex(sid, pid, v) =>
        ProductQty( StoreId(sid), ProductId(pid.toInt), v.toInt)
      case _ => throw new Exception(s"Failed to parse: $string")
    }
  }

  private val regex = raw"([^\|]+)\|(\d+)\|(\d+)".r
}

case class ProductRevenue(storeId: StoreId, productId: ProductId, value: BigDecimal)
extends Combinable[ProductRevenue] with Ordered[ProductRevenue] with ProductValue[BigDecimal]  {
  def combine(other: ProductRevenue) = ProductRevenue(storeId, productId, value + other.value)
  def compare(other: ProductRevenue) = {
    (this.storeId -> this.productId) compare (other.storeId -> other.productId)
  }
}

class ProductQties(tfm: TempFileManager, qties: Iterator[ProductQty])
extends Combinable[ProductQties] with LazyLogging {
  import Utils._

  private val source = {
    val file = tfm.getNewFile
    logger.debug(s"Serializing ProductQties to: $file")
    Utils.withPrinter(file, printer => {
      qties.foreach(x => {
        printer.println(x.serialize)
      })
      printer.flush
    })
    logger.debug(s"Done serializing ProductQties to: $file")
    Source.fromFile(file)
  }

  def iterator(): Iterator[ProductQty] = {
    logger.debug("Creating new ProductQties iterator")
    source.reset.getLines.map(ProductQty.parse(_))
  }

  def combine(other: ProductQties) : ProductQties = {
    val combined = new IteratorCombinator(iterator, other.iterator)
    new ProductQties(tfm, combined)
  }

  def groupByStore: Map[StoreId, ProductQties] = {
    iterator.toArray.groupBy(_.storeId).mapValues(x => new ProductQties(tfm, x.iterator))
  }

  def combineByProduct: ProductQties = {
    implicit val ord: Ordering[ProductQty] = Ordering.by(_.productId)
    val it = new GroupAndSumQties(
      iterator |> sortedExternally[ProductQty](tfm)
    )
    new ProductQties(tfm, it)
  }

  def getTop(n: Int) = {
    (iterator |> externalSortBy(tfm, - _.value)).take(n)
  }

}

object ProductQties {
  def apply(tfm: TempFileManager)(sortedTxs: Iterator[Transaction]): ProductQties = {
    val qties = sortedTxs.map(tx => ProductQty(tx.storeId, tx.productId, tx.quantity))
    val grouped = new GroupAndSumQties(qties)
    new ProductQties(tfm, grouped)
  }
}

private class GroupAndSumQties(_it: Iterator[ProductQty])(implicit ord: Ordering[ProductQty])
extends Iterator[ProductQty] {
  private val it = _it.buffered

  def hasNext = it.hasNext
  def next() = {
    var x = it.next()

    while (it.hasNext && (ord.compare(it.head, x) == 0)) {
      x = x.combine(it.next())
    }
    x
    // val (matching, tail) = it.span(ord.compare(_, x) == 0)
    // it = tail
    // matching.fold(x)(_.combine(_))
  }
}

class ProductRevenues(val aggregate: Array[ProductRevenue])
extends Combinable[ProductRevenues] with LazyLogging {

  def iterator = {
    logger.debug("Creating new ProductRevenues iterator")
    aggregate.iterator
  }

  def combine(other: ProductRevenues) : ProductRevenues = {
    val combined = new IteratorCombinator(iterator, other.iterator)
    new ProductRevenues(combined.toArray)
  }

  def groupByStore: Map[StoreId, ProductRevenues] = {
    aggregate.groupBy(_.storeId).mapValues(new ProductRevenues(_))
  }

  def combineByProduct: ProductRevenues = {
    groupByStore
      .values
      .map(_.iterator.map(_.copy(storeId = StoreId(""))))
      .map(q => new ProductRevenues(q.toArray))
      .reduce(_.combine(_))
  }

  def getTop(n: Int) = {
    aggregate.sortBy(- _.value).take(n).iterator
  }
}

object ProductRevenues extends LazyLogging {
  def apply(tfm: TempFileManager, dataSource: String => Try[Source], productQties: ProductQties)
  : ProductRevenues = {
    val joined = new Iterator[Option[ProductRevenue]] {
      private val it = productQties.iterator
      private var refSource: Option[(StoreId, Iterator[Reference])] = None
      def hasNext = it.hasNext
      def next() = {
        val pqty = it.next()
        val references: BufferedIterator[Reference] =
          (refSource match {
            case None => setNewReferenceSource(pqty.storeId)
            case Some((storeId, references)) => if (storeId == pqty.storeId) {
              references
            } else {
              setNewReferenceSource(pqty.storeId)
            }
          })
          .buffered

        // We can do this because references are sorted.
        while(references.hasNext && references.head.productId < pqty.productId) {
          references.next()
        }

        refSource = Some(pqty.storeId, references)

        if (references.hasNext && references.head.productId == pqty.productId) {
          val ref = references.next()
          Some(ProductRevenue(pqty.storeId, pqty.productId, pqty.value * ref.price))
        } else {
          logger.error(s"Reference not found: ${pqty.storeId}, ${pqty.productId} ")
          None
        }
      }

      private def setNewReferenceSource(storeId: StoreId): Iterator[Reference] = {
        val references = dataSource(s"reference_prod-${storeId.id}").map(
          _.getLines().map(Reference.parse(_).get)
        ).get
        refSource = Some(storeId, references)
        references
      }
    }

    logger.info("Starting serializing ProductRevenues")
    val a = joined.filter(!_.isEmpty).map(_.get).toArray
    logger.info("Done serializing ProductRevenues")
    new ProductRevenues(a) // TODO serialize this iterator into a file instead of an array.
  }
}
