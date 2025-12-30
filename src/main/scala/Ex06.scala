package learningscala
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.{functions => F, types => T}
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import java.sql.Timestamp
import java.time.Duration
import java.time.Instant
import com.github.tototoshi.csv._
import java.io.{File, FileNotFoundException}

/*
  ### 6. RDD Aggregation Mechanics (Taxi Analytics)

  Goal: build correct performance intuition for RDD-based analytics by grinding through
  realistic fact-table aggregations with no shortcuts.

  Focus:
  - key/value modeling
  - shuffles and memory pressure
  - accumulator design
  - multi-stage aggregation
  - sorting and ranking
 */

/*
  ### 1. Zone-to-Zone Flow Efficiency

  Task:
  - For each pickup → drop-off zone pair:
    - trip count
    - avg duration
    - avg speed
    - avg fare per mile
  - Rank top 20 busiest weekday rush-hour flows.

  Forces:
  - composite keys `(PU, DO)`
  - early filtering
  - `aggregateByKey` / `combineByKey`
  - custom accumulators
  - post-aggregation metrics

  APIs:
  `map`, `filter`, `keyBy`, `aggregateByKey`, `mapValues`, `sortBy`, `takeOrdered`
 */

/*
  ### 2. Zone Demand Volatility

  Task:
  - For each pickup zone:
    - hourly trip counts
    - demand volatility (variance / range)
  - Rank zones by instability.

  Forces:
  - time bucketing
  - `(zone, hour)` → `zone` reshaping
  - `groupByKey` (then replacing it)
  - manual statistics

  APIs:
  `map`, `reduceByKey`, `aggregateByKey`, `groupByKey`, `mapPartitions`
 */

/*
  ### Why these two are sufficient

  Together they teach:
  - key/value modeling
  - shuffle behavior
  - memory pressure intuition
  - accumulator design
  - multi-stage aggregation

  Run both with:
  1. RDDs
  2. DataFrames

  Compare code size, mental load, Spark UI, and shuffle cost.
 */

object Ex06 extends Exercise {
  // NOTE: We use a dedicated object to run Ex05_5 Spark job because Spark library is `% "provided"`,
  //       meaning we can't load objects dependent on its existence within sbt run.
  //       This means the runner must be a dedicated object.
  override def run(): Unit = {
    SparkRunner.runSparkJob("Ex06DfJob", build = true)
    println("[Exercises] Finished 'Ex06DfJob', running 'Ex05_5_RDD_Job'...")
    SparkRunner.runSparkJob("Ex06RddJob", build = false)
  }
}

abstract class Ex6SparkJob {
  object SchemaEnforcer {
    // format: off
    val schema = T.StructType(
      Seq(
        T.StructField("VendorID", T.IntegerType, nullable = true),
        T.StructField("tpep_pickup_datetime", T.TimestampType, nullable = true),
        T.StructField("tpep_dropoff_datetime", T.TimestampType, nullable = true),
        T.StructField("passenger_count", T.DoubleType, nullable = true),
        T.StructField("trip_distance", T.DoubleType, nullable = true),
        T.StructField("RatecodeID", T.LongType, nullable = true),
        T.StructField("store_and_fwd_flag", T.StringType, nullable = true),
        T.StructField("PULocationID", T.IntegerType, nullable = true),
        T.StructField("DOLocationID", T.IntegerType, nullable = true),
        T.StructField("payment_type", T.LongType, nullable = true),
        T.StructField("fare_amount", T.DoubleType, nullable = true),
        T.StructField("extra", T.DoubleType, nullable = true),
        T.StructField("mta_tax", T.DoubleType, nullable = true),
        T.StructField("tip_amount", T.DoubleType, nullable = true),
        T.StructField("tolls_amount", T.DoubleType, nullable = true),
        T.StructField("improvement_surcharge", T.DoubleType, nullable = true),
        T.StructField("total_amount", T.DoubleType, nullable = true),
        T.StructField("congestion_surcharge", T.DoubleType, nullable = true),
        T.StructField("airport_fee", T.DoubleType, nullable = true)
      )
    )
    // format: on
    private def normName(name: String): String = {
      name.toLowerCase().replaceAll("_|-", "")
    }
    private val normNameTypePair =
      this.schema.map(f => (normName(f.name), f.name, f.dataType))

    def enforce(df: DataFrame)(implicit log: Logger): DataFrame = {
      val dfNormNameToRawPair =
        df.schema.map(f => this.normName(f.name) -> (f.name, f.dataType)).toMap

      val newCols = normNameTypePair.map { case (norm, name, dataType) =>
        val (ogName, ogDataType) = dfNormNameToRawPair(norm)
        var newCol = F.col(ogName)
        if (name != ogName) {
          log.info(s"Renaming '$ogName' to '$name'")
          newCol = newCol.alias(name)
        }
        if (dataType != ogDataType) {
          log.warn(s"Casting '$name' from $ogDataType to $dataType")
          newCol = newCol.cast(dataType)
        }
        newCol
      }
      df.select(newCols: _*)
    }
  }

  def task(taxi: DataFrame, zones: DataFrame)(implicit
      spark: SparkSession
  ): Unit

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession
      .builder()
      .appName("Ex05_5")
      .master(s"spark://spark-localhost:7077")
      .config("spark.executor.memory", "4g")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    import spark.implicits._
    implicit val log = Logger.getLogger(this.getClass())

    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    val parquetPaths = fs
      .globStatus(
        new Path("/workspaces/*/data/taxi/yellow_tripdata_2023-*.parquet")
      )
      .map(_.getPath().toString())

    val parquets = parquetPaths.map(p => spark.read.parquet(p))
    val normParquets = parquets.map(
      SchemaEnforcer
        .enforce(_)
    )
    val taxi = normParquets
      .reduce((a, b) => a.unionByName(b))
    val zones = spark.read
      .option("header", "true")
      .csv("/workspaces/*/data/taxi/taxi_zone_lookup.csv")
      .select(
        $"LocationID".cast(T.IntegerType),
        $"Borough",
        $"Zone"
      )
    this.task(taxi, zones)
  }
}

object Ex06DfJob extends Ex6SparkJob {

  def task(
      taxi: DataFrame,
      zones: DataFrame
  )(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val pickupsPerHour = taxi
      .select(
        $"tpep_pickup_datetime"
      )
      .filter(F.dayofweek($"tpep_pickup_datetime").between(2, 6))
      .withColumn("hour", F.hour($"tpep_pickup_datetime"))
      .groupBy($"hour")
      .agg(F.count($"tpep_pickup_datetime").alias("pickups_per_hour"))

    val percentile = pickupsPerHour
      .select(
        F.percentile_approx(
          $"pickups_per_hour",
          F.lit(0.9),
          F.lit(10000)
        )
      )
      .as[Long]
      .head

    val rushHours = pickupsPerHour.filter(
      $"pickups_per_hour" >= percentile
    )

    val rushHoursArr = rushHours.select($"hour").as[Int].collect
    println(
      s"[Exercises] rushHoursArr: ${rushHoursArr.mkString("[", ",", "]")}"
    )

    val duration_minutes =
      (F.unix_timestamp($"tpep_dropoff_datetime") - F.unix_timestamp(
        $"tpep_pickup_datetime"
      )) / 60
    val rushHourStatistics = taxi
      .select($"*", F.hour($"tpep_pickup_datetime").alias("hour"))
      .filter($"hour".isInCollection(rushHoursArr))
      .groupBy($"PULocationID", $"DOLocationID")
      .agg(
        F.count($"*").alias("count"),
        F.avg(duration_minutes).alias("avg_duration_minutes"),
        F.avg($"trip_distance" / (duration_minutes / 60)).alias("avg_mph"),
        F.avg($"fare_amount" / $"trip_distance").alias("avg_fare_per_mile")
      )

    val rushHourStatisticsWithZones = rushHourStatistics
      .join(
        F.broadcast(zones)
          .select(
            $"LocationID".alias("PULocationID"),
            $"Borough".alias("PU_Borough"),
            $"Zone".alias("PU_Zone")
          ),
        Seq("PULocationID"),
        "left"
      )
      .join(
        F.broadcast(zones)
          .select(
            $"LocationID".alias("DOLocationID"),
            $"Borough".alias("DO_Borough"),
            $"Zone".alias("DO_Zone")
          ),
        Seq("DOLocationID"),
        "left"
      )

    val top20BusiestFlows =
      rushHourStatisticsWithZones.orderBy($"count".desc).limit(20)
    top20BusiestFlows.write
      .option("header", "true")
      .mode("overwrite")
      .csv(
        "/workspaces/learning-scala/output/df_rush_hour_top20_busiest_flows.csv"
      )
  }

}

object Ex06RddJob extends Ex6SparkJob {
  case class TaxiRecord(
      tpep_pickup_datetime: Timestamp,
      tpep_dropoff_datetime: Timestamp,
      trip_distance: Double,
      fare_amount: Double,
      PULocationID: Int,
      DOLocationID: Int
  )

  case class ZonesRecord(
      LocationID: Int,
      Borough: String,
      Zone: String
  )

  case class Statistics(
      var count: Long,
      var avg_duration_minutes: Double,
      var avg_mph: Double,
      var avg_fare_per_mile: Double
  )

  case class StatState(
      var count: Long = 0,
      var avg_duration_minutes: AvgState = AvgState(),
      var avg_mph: AvgState = AvgState(),
      var avg_fare_per_mile: AvgState = AvgState()
  )

  case class AvgState(
      var count: Long = 0,
      var value: Double = 0
  ) {
    def consume(item: Double): Unit = {
      if (!item.isNaN() && !item.isInfinite()) {
        this.count += 1
        this.value += item
      }
    }
    def getAvg(): Double = {
      this.value / this.count
    }
    def combine(other: AvgState): Unit = {
      this.count += other.count
      this.value += other.value
    }
  }

  case class TripZones(
      PULocationID: Int,
      PU_Borough: String,
      PU_Zone: String,
      DOLocationID: Int,
      DO_Borough: String,
      DO_Zone: String
  )

  def task(taxi: DataFrame, zones: DataFrame)(implicit
      spark: SparkSession
  ): Unit = {
    import spark.implicits._
    val sc = spark.sparkContext

    val taxiRdd = taxi.rdd.map { r =>
      TaxiRecord(
        r.getAs[Timestamp]("tpep_pickup_datetime"),
        r.getAs[Timestamp]("tpep_dropoff_datetime"),
        r.getAs[Double]("trip_distance"),
        r.getAs[Double]("fare_amount"),
        r.getAs[Int]("PULocationID"),
        r.getAs[Int]("DOLocationID")
      )
    }
    val zonesRdd = zones.rdd.map(r =>
      ZonesRecord(
        r.getAs[Int]("LocationID"),
        r.getAs[String]("Borough"),
        r.getAs[String]("Zone")
      )
    )
    val pickupsPerHour = taxiRdd
      .map(r => r.tpep_pickup_datetime)
      .filter(ts =>
        2 to 6 contains ts.toLocalDateTime().getDayOfWeek().getValue
      )
      .map { ts =>
        (
          ts.toLocalDateTime().getHour(),
          1
        )
      }
      .reduceByKey(_ + _)

    val rushHoursArr = {
      val sorted = pickupsPerHour.collect.sortBy(
        { case (_, count) => count }
      )
      sorted.slice((sorted.size * 0.9).floor.toInt, sorted.size)
    }.map { case (hour, _) => hour }
    println(
      s"[Exercises] rushHoursArr: ${rushHoursArr.mkString("[", ",", "]")}"
    )

    def getDurationMinutes(pickup: Timestamp, dropoff: Timestamp): Double = {
      Duration
        .between(pickup.toInstant(), dropoff.toInstant())
        .getSeconds()
        .toDouble / 60
    }
    val rushHourStatistics = taxiRdd
      .filter(r =>
        rushHoursArr contains r.tpep_pickup_datetime.toLocalDateTime().getHour()
      )
      .map { r =>
        (
          (
            r.PULocationID,
            r.DOLocationID
          ),
          r
        )
      }
      .aggregateByKey(
        StatState()
      )(
        // reduce partition to a single value
        (stats, r) => {
          stats.count += 1
          val duration_minutes = getDurationMinutes(
            r.tpep_pickup_datetime,
            r.tpep_dropoff_datetime
          )
          stats.avg_duration_minutes.consume(duration_minutes)
          stats.avg_mph.consume(r.trip_distance / (duration_minutes / 60))
          stats.avg_fare_per_mile.consume(r.fare_amount / r.trip_distance)
          stats
        },
        // combine partition results
        (statsA, statsB) => {
          statsA.count += statsB.count
          statsA.avg_duration_minutes.combine(statsB.avg_duration_minutes)
          statsA.avg_mph.combine(statsB.avg_mph)
          statsA.avg_fare_per_mile.combine(statsB.avg_fare_per_mile)
          statsA
        }
      )
      .mapValues(stats =>
        Statistics(
          stats.count,
          stats.avg_duration_minutes.getAvg(),
          stats.avg_mph.getAvg(),
          stats.avg_fare_per_mile.getAvg()
        )
      )
    // broadcast join
    val broadcastZonesMap =
      sc.broadcast(zonesRdd.collect().map(r => r.LocationID -> r).toMap)
    val rushHourStatisticsWithZones = rushHourStatistics.map {
      case ((puLocationID, doLocationID), stats) =>
        (
          stats,
          TripZones(
            PULocationID = puLocationID,
            PU_Borough = broadcastZonesMap.value(puLocationID).Borough,
            PU_Zone = broadcastZonesMap.value(puLocationID).Zone,
            DOLocationID = doLocationID,
            DO_Borough = broadcastZonesMap.value(doLocationID).Borough,
            DO_Zone = broadcastZonesMap.value(doLocationID).Zone
          )
        )
    }
    val top20BusiestFlows =
      rushHourStatisticsWithZones.top(20)(
        Ordering.by { case (stats, _) => stats.count }
      )
    val csvFile = new File("./output/rdd_rush_hour_top20_busiest_flows.csv")
    val csvWriter = CSVWriter.open(csvFile, append = false)
    csvWriter.writeRow(
      Seq(
        "DOLocationID",
        "PULocationID",
        "count",
        "avg_duration_minutes",
        "avg_mph",
        "avg_fare_per_mile",
        "PU_Borough",
        "PU_Zone",
        "DO_Borough",
        "DO_Zone"
      )
    )
    top20BusiestFlows
      .map { case (stats, zones) =>
        Seq(
          zones.DOLocationID,
          zones.PULocationID,
          stats.count,
          stats.avg_duration_minutes,
          stats.avg_mph,
          stats.avg_fare_per_mile,
          zones.PU_Borough,
          zones.PU_Zone,
          zones.DO_Borough,
          zones.DO_Zone
        )
      }
      .foreach(csvWriter.writeRow)
  }
}
