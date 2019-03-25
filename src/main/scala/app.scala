package PhenixChallenge

import com.typesafe.scalalogging.LazyLogging

import java.io.{FileWriter, PrintWriter}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

import scala.util.{Failure, Success, Try}
import scala.io.Source

import model._
import aggregate.{Aggregate, Helpers}

object Main extends App with LazyLogging {
  if(args.length < 2) {
    logger.error("USAGE : phenix-challenge <path_to_data> <output_dir> [<YYYY-MM-DD>]")
    System.exit(1)
  }
  val dataDirectory = args(0)
  val outputDirectory = args(1)
  val day = if(args.length == 3) LocalDate.parse(args(2)) else LocalDate.now()
  val dayString = formatDate(day)

  Aggregate
    .fromDataSource(dataSource(dayString))
    .foreach( // skipped if we cannot produce the main day's aggregate.
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


  def dataSource(postfix: String)(name: String) = {
    // FIXME: Make path interoperable --SDF 2019-03-06
    Try(Source.fromFile(s"$dataDirectory/${name}_${postfix}.data"))
  }

  def formatDate(date: LocalDate) = {
    date.format(DateTimeFormatter.BASIC_ISO_DATE)
  }

  def serializeTop100s(aggregate: Aggregate, postfix: String) = {
    import Helpers.TakeTop
    val n = 100
    val top100QtyByStore = aggregate.productQtiesByStore.mapValues(_.takeTop(n))
    val top100RevenueByStore = aggregate.productRevenuesByStore.mapValues(_.takeTop(n))
    val top100QtyOverall = aggregate.overallProductQties.takeTop(n)
    val top100RevenueOverall = aggregate.overallProductRevenues.takeTop(n)

    top100QtyByStore.foreach {
      case (sid, top100) => serialize(s"ventes_${sid.id}_${postfix}", top100)
    }
    top100RevenueByStore.foreach {
      case (sid, top100) => serialize(s"ca_${sid.id}_${postfix}", top100)
    }

    serialize(s"ventes_GLOBAL_${postfix}", top100QtyOverall)
    serialize(s"ca_GLOBAL_${postfix}", top100RevenueOverall)
  }

  def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B =
    try { f(param) } finally { param.close() }

  def serialize[V](name: String, top100: List[(ProductId, V)]) =
    using(new FileWriter(s"$outputDirectory/top_100_${name}.data")) {
      writer => using(new PrintWriter(writer)) {
        printer => top100.foreach({case (ProductId(pid), qty) => printer.println(s"$pid|$qty")})
      }
    }
}
