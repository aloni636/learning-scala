package learningscala

import scala.io
import java.{io => jio}
import java.io.{File, FileNotFoundException}
import java.time.{LocalDate, Duration, Period}
import java.time.format.DateTimeParseException
import java.time.temporal.ChronoUnit
import com.github.tototoshi.csv._
import org.slf4j.LoggerFactory
import pprint.pprintln
import learningscala.Exercise

/*
  ---

  ### 2. Collections + functional style (this is your daily driver)

  Goal: be very comfortable with immutable collections and `map`/`flatMap` chains - this is how GeoTrellis, Spark, and all the glue code feel.

  Focus topics:

 * `List`, `Vector`, `Map`, `Seq`.
 * `map`, `flatMap`, `filter`, `groupBy`, `foldLeft`, `reduceOption`.
 * `Option`, `Either`, `Try` and pattern matching on them.

  Exercises:

 * Load a small CSV/text file with `scala.io.Source` and:

 * Split to lines, map to a case class `Record`.
 * Group by some field and compute an aggregate (mean, max, histogram).
 */

object Ex02 extends Exercise {

  override def run(): Unit = {
    val log = LoggerFactory.getLogger(getClass())

    case class Customer(
        Index: Int,
        CustomerId: Long,
        FirstName: String,
        LastName: String,
        Company: String,
        City: String,
        Country: String,
        Phone1: String,
        Phone2: String,
        Email: String,
        SubscriptionDate: LocalDate,
        Website: String
    )
    // QUESTION: Why we had to use new here but not in List()
    // ANSWER: Because List has a companion singleton List instance responsible for
    //         instantiating List instances via static constructor wrappers
    println(s"Current working directory: ${new File(".").getAbsoluteFile()}")
    val csvFile = new File("./data/customers-100.csv")
    if (!csvFile.exists())
      throw new FileNotFoundException(
        s"Couldn't find ${csvFile}\nMake sure to download it from https://github.com/datablist/sample-csv-files"
      )
    val reader = CSVReader.open(csvFile)
    val records = reader.allWithHeaders()

    val someCustomers: List[Option[Customer]] = records.map { record =>
      try
        Some(
          Customer(
            Index = record("Index").toInt,
            CustomerId = java.lang.Long.parseLong(record("Customer Id"), 16),
            FirstName = record("First Name"),
            LastName = record("Last Name"),
            Company = record("Company"),
            City = record("City"),
            Country = record("Country"),
            Phone1 = record("Phone 1"),
            Phone2 = record("Phone 2"),
            Email = record("Email"),
            SubscriptionDate = LocalDate.parse(record("Subscription Date")),
            Website = record("Website")
          )
        )
      catch {
        case e @ (_: NumberFormatException | _: DateTimeParseException |
            _: NoSuchElementException) => {
          log.warn(
            s"Failed to parse record due to ${e.getClass().getName()}: ${record}"
          )
          None
        }
        case e: Throwable => {
          log.error(
            s"Unexpected exception during customer record parsing: ${e.getClass().getName()}"
          )
          throw e
        }
      }
    }
    val customers = someCustomers.flatten
    println(
      s"Parsed ${someCustomers.length} CSV records into ${customers.length} customer entries"
    )
    val customersByCompany = customers.groupMapReduce(_.Company)(_ => 1)(_ + _)
    println("Consumers per company:")
    pprintln(customersByCompany)

    // fan-out based count rollup: we explode each item into all levels of the hierarchy, and then aggregate count
    // NOTE: Seq() is a trait which is implemented in Scala 2.13 using List
    val customersByLocation = customers
      .flatMap(x =>
        Seq(
          (x.Country, None),
          (x.Country, Some(x.City))
        )
      )
      .groupMapReduce(identity)(_ => 1)(_ + _)
    println(s"Fan-out based location based customer count:")
    customersByLocation.toList
      .sortBy(_._2)
      // NOTE: case is used for tuple unpacking, and is the preferable way to handle tuples/products
      .foreach { case ((country, cityOption), count) =>
        println(
          s"${(country +: cityOption.toList).mkString(" / ")} -> ${count}"
        )
      }

    val subscriptionTimeDifference =
      customers
        .map(_.SubscriptionDate)
        .sortBy(identity)
        .sliding(2)
        .map(x =>
          (
            Period.between(x(0), x(1)), // calender aware time difference
            ChronoUnit.DAYS.between(x(0), x(1)) // total days difference
          )
        )
        .toList
    // See: https://stackoverflow.com/a/40487511
    val regexPattern = """\d+[YMD]""".r
    val prettyDifferences = subscriptionTimeDifference
      .sortBy(x => x._2)
      .map { case (cal, total) =>
        val totalPlural = if (total == 1) "" else "s"
        val periodItems = regexPattern.findAllIn(cal.toString()).toList
        periodItems
          .map { item =>
            val numeric = item.dropRight(1).toInt
            val calPlural = if (numeric == 1) "" else "s"
            val unit = item.last
            numeric.toString + (unit match {
              case 'Y' => " Year"
              case 'M' => " Month"
              case 'D' => " Day"
            }) + calPlural
          }
          .mkString(", ") + s" (Total: ${total} Day" + totalPlural + ")"
      }
    println("Subscription time differences, ordered by max difference:")
    pprintln(prettyDifferences)
  }

}
