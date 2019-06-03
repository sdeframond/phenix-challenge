package PhenixChallenge.model

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

import scala.util.{Failure, Success, Try}

trait Serializable {
  def serialize(): String
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
