package Generator

import com.typesafe.scalalogging.LazyLogging

import java.io.{FileWriter, PrintWriter}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

import scala.util.Random

object Main extends App with LazyLogging {
  object numberOf {
    val stores = 100
    val txPerDays = 10 * 1000
    val references = 1 * 100 * 1000
    val days = 7
  }
  val startDate = LocalDate.parse("2019-03-15")
  val random = new Random(123)

  val storeIds = (1 to numberOf.stores)
    .map(_ => random.alphanumeric.take(20).mkString)
    .toArray

  for {day <- (0 to (numberOf.days - 1)).toList.map(startDate.minusDays(_)) } {
    logger.info(s"Generating data for day: $day")
    generateTransactions(day)
    storeIds.foreach(generateReferences(day, _))
  }

  def generateTransactions(day: LocalDate) = {
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
  }

  def generateReferences(day: LocalDate, storeId: String) {
    val dayString = formatDate(day)
    withPrinter(s"reference_prod-${storeId}_${dayString}") { printer =>
      for (productId <- 1 to numberOf.references) {
        val price = BigDecimal
          .decimal(random.nextFloat * 100)
          .setScale(2, BigDecimal.RoundingMode.DOWN)
        printer.println(s"$productId|$price")
      }
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
