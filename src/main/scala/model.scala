package PhenixChallenge.model

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

import scala.util.{Failure, Success, Try}

trait Serializable {
  def serialize(): String
}

trait Combinable[T] {
  def combine(other: T): T;
}

case class TransactionId(id: Int)
case class StoreId(id: String) extends Ordered[StoreId] {
  def compare(that: StoreId) = id.compare(that.id)
}
case class ProductId(id: Int) extends Ordered[ProductId] {
  def compare(that: ProductId) = id.compare(that.id)
}

case class Reference(productId: ProductId, price: BigDecimal)

object Reference {
  def parse(string: String): Reference = {
    string match {
      case regex(pid, price) =>
        Reference(ProductId(pid.toInt), BigDecimal(price))
      case _ => throw new Exception(s"Failed to parse: $string")
    }
  }

  private val regex = raw"(\d+)\|(\d+(?:\.\d{1,2})?)".r
}

case class Transaction
  ( id: TransactionId
  , time: LocalDateTime
  , storeId: StoreId
  , productId: ProductId
  , quantity: Int
  )  extends Serializable {
    def serialize: String = {
      s"${id.id}|20170514T051919+0100|${storeId.id}|${productId.id}|$quantity"
    }
  }

object Transaction {
  implicit def parse(string: String): Transaction = {
    string match {
      case regex(id, time, sid, pid, qty) =>
        Transaction(TransactionId(id.toInt),
                    LocalDateTime.parse(time, formatter),
                    StoreId(sid),
                    ProductId(pid.toInt),
                    qty.toInt)
      case _ => throw new Exception(s"Failed to parse: $string")
    }
  }

  private val formatter = DateTimeFormatter.ofPattern("uuuuLLdd'T'HHmmssxxxx")
  private val regex = raw"(\d+)\|([^\|]+)\|([^\|]+)\|(\d+)\|(\d+)".r
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
  def serialize = s"${productId.id}|${value}"
}

case class ProductQty(storeId: StoreId, productId: ProductId, value: Int)
extends ProductValue[Int]

case class ProductRevenue(storeId: StoreId, productId: ProductId, value: BigDecimal)
extends ProductValue[BigDecimal]
