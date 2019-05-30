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

  def externalSortBy[T <: Serializable, A](tfm: TempFileManager, f: T => A)
                                          (it: Iterator[T])
                                          (implicit ordA: Ordering[A],
                                           parse: String => T) : Iterator[T] = {
    implicit val ordT = Ordering.by(f)
    sortedExternally(tfm)(it)
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
    productQties.getTopByStore(n)
  }

  def getTopRevenuesByStore(n: Int): Map[StoreId, Iterator[ProductRevenue]] = {
    productRevenues.getTopByStore(n)
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

trait ProductValue[T] extends Serializable {
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

  def serialize = s"${storeId.id}|${productId.id}|${value}"
}

private object ProductRevenue {
  implicit def parse(string: String): ProductRevenue = {
    string match {
      case regex(sid, pid, v) =>
        ProductRevenue(StoreId(sid), ProductId(pid.toInt), BigDecimal(v))
      case _ => throw new Exception(s"Failed to parse: $string")
    }
  }

  private val regex = raw"([^\|]+)\|(\d+)\|(\d+\.\d+)".r
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

  def combineByProduct: ProductQties = {
    implicit val ord: Ordering[ProductQty] = Ordering.by(_.productId)
    val it = iterator |> sortedExternally[ProductQty](tfm) |> ProductQties.groupAndSum[ProductQty]
    new ProductQties(tfm, it)
  }

  def getTop(n: Int) = {
    (iterator |> externalSortBy(tfm, - _.value)).take(n)
  }

  def getTopByStore(n: Int): Map[StoreId, Iterator[ProductQty]] = {

    // For some reason I get a 'diverging implicit expansion for Ordering[(StoreId, Int)]'
    // so I manually define an ordering here.
    // val ord3 = implicitly[Ordering[(StoreId, Int)]]
    implicit object Ord extends Ordering[ProductQty] {
      def compare(x: ProductQty, y: ProductQty): Int = {
        val ord1 = implicitly[Ordering[StoreId]]
        val ord2 = implicitly[Ordering[Int]]
        val c1 = ord1.compare(x.storeId, y.storeId)
        if (c1 == 0) {
          ord2.compare(y.value, x.value)
        } else c1
      }
    }

    val sorted = (iterator |> sortedExternally(tfm)).buffered
    var map = Map[StoreId, Iterator[ProductQty]]()
    while (sorted.hasNext) {
      val key = sorted.head.storeId
      var count = 0
      var top = Vector[ProductQty]() // Using a vector because we assume n is small.

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

object ProductQties {
  def apply(tfm: TempFileManager)(sortedTxs: Iterator[Transaction]): ProductQties = {
    val qties = sortedTxs.map(tx => ProductQty(tx.storeId, tx.productId, tx.quantity))
    val grouped = groupAndSum(qties)
    new ProductQties(tfm, grouped)
  }

  def groupAndSum[T <% Combinable[T]](_it: Iterator[T])(implicit ord: Ordering[T]) = {
    new Iterator[T] {
      private val it = _it.buffered

      def hasNext = it.hasNext
      def next() = {
        var x = it.next()

        while (it.hasNext && (ord.compare(it.head, x) == 0)) {
          x = x.combine(it.next())
        }

        x
      }
    }
  }
}

class ProductRevenues(tfm: TempFileManager, it: Iterator[ProductRevenue])
extends Combinable[ProductRevenues] with LazyLogging {
  import Utils._

  private val source = {
    val file = tfm.getNewFile
    logger.debug(s"Serializing ProductRevenues to: $file")
    Utils.withPrinter(file, printer => {
      it.foreach(x => {
        printer.println(x.serialize)
      })
    })
    logger.debug(s"Done serializing ProductRevenues to: $file")
    Source.fromFile(file)
  }

  def iterator(): Iterator[ProductRevenue] = {
    logger.debug("Creating new ProductRevenues iterator")
    source.reset.getLines.map(ProductRevenue.parse(_))
  }

  def combine(other: ProductRevenues) : ProductRevenues = {
    val combined = new IteratorCombinator(iterator, other.iterator)
    new ProductRevenues(tfm, combined)
  }

  // def groupByStore: Map[StoreId, ProductRevenues] = {
  //   aggregate.groupBy(_.storeId).mapValues(x => new ProductRevenues(x.iterator))
  // }


  def combineByProduct: ProductRevenues = {
    implicit val ord: Ordering[ProductRevenue] = Ordering.by(_.productId)
    val it = iterator |> sortedExternally[ProductRevenue](tfm) |> ProductQties.groupAndSum[ProductRevenue]
    new ProductRevenues(tfm, it)
  }

  // def combineByProduct: ProductRevenues = {
  //   groupByStore
  //     .values
  //     .map(_.iterator.map(_.copy(storeId = StoreId(""))))
  //     .map(q => new ProductRevenues(q))
  //     .reduce(_.combine(_))
  // }

  def getTop(n: Int) = {
    (iterator |> externalSortBy(tfm, - _.value)).take(n)
  }

  def getTopByStore(n: Int): Map[StoreId, Iterator[ProductRevenue]] = {

    // For some reason I get a 'diverging implicit expansion for Ordering[(StoreId, Int)]'
    // so I manually define an ordering here.
    // val ord3 = implicitly[Ordering[(StoreId, Int)]]
    implicit object Ord extends Ordering[ProductRevenue] {
      def compare(x: ProductRevenue, y: ProductRevenue): Int = {
        val ord1 = implicitly[Ordering[StoreId]]
        val ord2 = implicitly[Ordering[BigDecimal]]
        val c1 = ord1.compare(x.storeId, y.storeId)
        if (c1 == 0) {
          ord2.compare(y.value, x.value)
        } else c1
      }
    }

    val sorted = (iterator |> sortedExternally(tfm)).buffered
    var map = Map[StoreId, Iterator[ProductRevenue]]()
    while (sorted.hasNext) {
      val key = sorted.head.storeId
      var count = 0
      var top = Vector[ProductRevenue]() // Using a vector because we assume n is small.

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

object ProductRevenues extends LazyLogging {

  def apply(tfm: TempFileManager,
            dataSource: String => Try[Source],
            productQties: ProductQties) : ProductRevenues = {

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

    new ProductRevenues(tfm, joined.filter(!_.isEmpty).map(_.get))
  }
}
