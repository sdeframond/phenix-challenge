package PhenixChallenge.model

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

import scala.util.{Failure, Success, Try}

case class TransactionId(id: Int)
case class StoreId(id: String)
case class ProductId(id: Int)

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

  private val regex = raw"(\d+)\|(\d+(?:\.\d{1,2})?)".r
}

case class Transaction
  ( id: TransactionId
  , time: LocalDateTime
  , storeId: StoreId
  , productId: ProductId
  , quantity: Int
  )

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
