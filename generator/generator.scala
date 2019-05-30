package Generator

import com.typesafe.scalalogging.LazyLogging

import java.io.{FileWriter, PrintWriter}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

import scala.util.Random

object Main extends App with LazyLogging {

  ////////////////////////////////
  //   CHANGE THIS AS NEEDED.   //
  ////////////////////////////////
  val startDate = LocalDate.parse("2019-03-15")
  object numberOf {
    val stores = 3000
    val txPerDays = 1000 * 1000
    // val references = 10 * 1000 * 1000
    val references = 500 * 1000
    val days = 7
  }
  ////////////////////////////////

  val random = new Random(123)

  logger.debug("START generating store IDs")
  val storeIds = (1 to numberOf.stores)
    .map(_ => random.alphanumeric.take(36).mkString)
    .toArray
  logger.debug("DONE generating store IDs")

  for {day <- (0 to (numberOf.days - 1)).toList.map(startDate.minusDays(_)) } {
    logger.debug(s"START generating data for day: $day")
    generateTransactions(day)
    storeIds.foreach(sid => {
      generateReferences(day, sid)
    })
    logger.debug(s"DONE generating data for day: $day")
  }

  def generateTransactions(day: LocalDate) = {
    logger.debug(s"START generating transactions for day: $day")
    val dayString = formatDate(day)
    withPrinter(s"transactions_$dayString") { printer =>
      for (txId <- 1 to numberOf.txPerDays) {
        val storeId = storeIds(random.nextInt(storeIds.size))
        val productId = random.nextInt(numberOf.references) + 1
        val qty = random.nextInt(10) + 1
        val txDateFormatter = DateTimeFormatter.ofPattern("uuuuLLdd'T'HHmmss")
        val dateString = day.atTime(0, 0, 0).format(txDateFormatter)
        printer.println(s"$txId|$dateString+0100|$storeId|$productId|$qty")
      }
    }
    logger.debug(s"DONE generating transactions for day: $day")
  }

  def generateReferences(day: LocalDate, storeId: String) {
    val dayString = formatDate(day)
    val priceChoices = Array("10.00", "20.00", "321.32")
    withPrinter(s"reference_prod-${storeId}_${dayString}") { printer =>
      val str = new StringBuilder()
      for (productId <- 1 to numberOf.references) {

        // Faster than generating truly random prices
        val price = priceChoices(productId % priceChoices.size)

        str.append(productId)
        str.append("|")
        str.append(price)
        str.append("\n")
      }
      printer.print(str)
    }
  }

  def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B =
    try { f(param) } finally { param.close() }

  def withPrinter(fileName: String)(f: PrintWriter => Unit) =
    using(new FileWriter(s"data/$fileName.data")) {
      writer => using(new PrintWriter(writer)) {
        printer => f(printer)
      }
    }

  def formatDate(date: LocalDate) = {
    date.format(DateTimeFormatter.BASIC_ISO_DATE)
  }
}
