package PhenixChallenge

import com.typesafe.scalalogging.LazyLogging

import java.io.{FileWriter, PrintWriter}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

import scala.util.{Failure, Success, Try}
import scala.io.Source

import model.ProductValue
import aggregate.{Aggregate, TempFileManager}

object Main extends App with LazyLogging {
  if(args.length < 2) {
    logger.error("USAGE : phenix-challenge <path_to_data> <output_dir> [<YYYY-MM-DD>]")
    System.exit(1)
  }
  val dataDirectory = args(0)
  val outputDirectory = args(1)
  val currentDay = if(args.length == 3) LocalDate.parse(args(2)) else LocalDate.now()
  val dayString = formatDate(currentDay)

  using(new TempFileManager)(tfm => {
    val dailyAggregates = (0 to 6).toArray.map(i => {
      val day = currentDay.minusDays(i)
      logger.info(s"Starting aggregate for $day")
      Aggregate(tfm, dataSource(formatDate(day)))
    })

    logger.info("Writing daily results")
    serializeTop100s(dailyAggregates(0).get, dayString)

    val weeklyAggregate = dailyAggregates.filter(_.isSuccess).map(_.get).reduce(_.merge(_))
    logger.info(s"Writing weekly results")
    serializeTop100s(weeklyAggregate, s"$dayString-J7")
  })

  def dataSource(postfix: String)(name: String) = {
    val fileName = s"$dataDirectory/${name}_${postfix}.data"
    // FIXME: Make path interoperable --SDF 2019-03-06
    Try(Source.fromFile(fileName)) match {
      case Success(source) => {
        logger.debug(s"Opening data source: $fileName")
        Success(source)
      }
      case Failure(e) => {
        logger.error(s"Could not open data source: $fileName")
        Failure(e)
      }
    }
  }

  def formatDate(date: LocalDate) = {
    date.format(DateTimeFormatter.BASIC_ISO_DATE)
  }

  def serializeTop100s(aggregate: Aggregate, postfix: String) = {
    val n = 100

    aggregate.getTopQtiesByStore(n).foreach {
      case (sid, top100) => serialize(s"ventes_${sid.id}_${postfix}", top100)
    }
    aggregate.getTopRevenuesByStore(n).foreach {
      case (sid, top100) => serialize(s"ca_${sid.id}_${postfix}", top100)
    }

    serialize(s"ventes_GLOBAL_${postfix}", aggregate.getTopQties(n))
    serialize(s"ca_GLOBAL_${postfix}", aggregate.getTopRevenues(n))
  }

  def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B =
    try { f(param) } finally { param.close() }

  def serialize[V](name: String, top100: Iterator[ProductValue[V]]) = {
    val fileName = s"$outputDirectory/top_100_${name}.data"
    logger.debug(s"Serializing to file: $fileName")
    using(new FileWriter(fileName)) {
      writer => using(new PrintWriter(writer)) {
        printer => top100.foreach(l => printer.println(l.serialize))
      }
    }
  }
}
