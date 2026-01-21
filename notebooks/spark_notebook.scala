// ---
// jupyter:
//   jupytext:
//     text_representation:
//       extension: .scala
//       format_name: percent
//       format_version: '1.3'
//       jupytext_version: 1.19.0
//   kernelspec:
//     display_name: Scala
//     language: scala
//     name: scala
// ---

// %% [markdown]
// # Packages & Spark Setup

// %%
import $ivy.`org.apache.spark::spark-sql:3.5.5`

// %%
org.apache.spark.SPARK_VERSION
scala.util.Properties.versionNumberString

// %%
import org.apache.spark.sql.NotebookSparkSession
import org.apache.spark.sql.SparkSession

// %%
val spark = SparkSession.builder()
    .appName("helloJupyter")
    .master(s"spark://localhost:7077")
    .config("spark.executor.memory", "4g")
    .getOrCreate()
val sc = spark.sparkContext

sc.setLogLevel("WARN")
import spark.implicits._

// %%
kernel.silent(false)

// %% [markdown]
// # Interactive Spark

// %%
// val data = Seq(("sue", 32), ("li", 3), ("bob", 75), ("heo", 13))
// val df = data.toDF("first_name", "age")

// df.show()

// %% [markdown]
// # Task 1: Trip Flow Efficiency
//
// **Goal:** Rank the top 20 busiest **Pickup  Drop-off** pairs during weekday rush hours.
//
// * **Filter:** Weekdays + Rush Hour windows.
// * **Key:** `(PULocationID, DOLocationID)`.
// * **Metrics:** Count, Avg Duration, Avg Speed, Avg Fare/Mile.
// * **RDD Ops:** `filter`  `map` (to composite key)  `reduceByKey` (summing metrics)  `mapValues` (calculating averages)  `takeOrdered`.

// %%
import org.apache.spark.sql.{functions => F, types => T}
import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, DataFrame}

// %%
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


// %%
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

val conf = new Configuration()
val fs = FileSystem.get(conf)

val parquetPaths = fs
  .globStatus(
    new Path("/workspaces/*/data/taxi/yellow_tripdata_2023-*.parquet")
  )
  .map(_.getPath().toString())

// %%
implicit val log = Logger.getLogger(this.getClass())

// %%
import org.apache.spark.storage.StorageLevel

// %%
val parquets = parquetPaths.map(p => spark.read.parquet(p))
val normParquets = parquets.map(SchemaEnforcer.enforce)
val taxi = normParquets.reduce((a, b) => a.unionByName(b)).cache() // .persist(StorageLevel.MEMORY_ONLY)

// %%
val pickupsPerHour = taxi
  .select(
    $"tpep_pickup_datetime"
  )
  .filter(F.dayofweek($"tpep_pickup_datetime").between(2, 6))
  .withColumn("hour", F.hour($"tpep_pickup_datetime"))
  .groupBy($"hour")
  .agg(F.count($"tpep_pickup_datetime").alias("pickups_per_hour"))

// %%
// pickupsPerHour.show(50)

// %%
val percentile = pickupsPerHour.select(F.percentile_approx(
  $"pickups_per_hour",
  F.lit(0.9),
  F.lit(10000)
)).as[Long].head

// %%
val rushHours = pickupsPerHour.filter(
  $"pickups_per_hour" > percentile
)

// %%
rushHours.show()

// %%
val rushHoursArr = rushHours.select($"hour").as[Int].collect

// %%
taxi.printSchema

// %%
val duration_minutes = (F.unix_timestamp($"tpep_dropoff_datetime") - F.unix_timestamp($"tpep_pickup_datetime")) / 60
val rushHourStatistics = taxi
  .select($"*", F.hour($"tpep_pickup_datetime").alias("hour"))
  .filter($"hour".isInCollection(rushHoursArr))
  .groupBy($"PULocationID", $"DOLocationID")
  .agg(
    F.count($"*").alias("count"),
    F.avg(duration_minutes).alias("avg_duration_minutes"),
    F.avg($"trip_distance" / (duration_minutes / 60)).alias("avg_mph"),
    F.avg($"fare_amount" / $"trip_distance").alias("avg_fare_per_mile")
  ).cache()

// %%
val zones = spark.read
  .option("header", "true")
  .csv("/workspaces/*/data/taxi/taxi_zone_lookup.csv")
  .select(
    $"LocationID".cast(T.IntegerType),
    $"Borough",
    $"Zone"
  )

// %%
zones.printSchema

// %%
val rushHourStatisticsWithZones = rushHourStatistics
  .join(
    zones.select(
      $"LocationID".alias("PULocationID"),
      $"Borough".alias("PU_Borough"),
      $"Zone".alias("PU_Zone")
    ),
    Seq("PULocationID"),
    "left"
  )
  .join(
    zones.select(
      $"LocationID".alias("DOLocationID"),
      $"Borough".alias("DO_Borough"),
      $"Zone".alias("DO_Zone")
    ),
    Seq("DOLocationID"),
    "left"
  ).cache()
rushHourStatisticsWithZones.printSchema

// %%
rushHourStatisticsWithZones.orderBy($"count".desc).limit(20).show(truncate=false)

// %% [markdown] jp-MarkdownHeadingCollapsed=true
// ---
//
// # Task 2: Demand Volatility
//
// **Goal:** Rank pickup zones by how much their trip volume fluctuates hourly.
//
// * **Step 1:** Count trips per `(Zone, Hour)` using `reduceByKey`.
// * **Step 2:** Regroup by `Zone` only.
// * **Step 3:** Calculate **Variance** or **StdDev** of the hourly counts.
// * **RDD Ops:** `map` (to hour)  `reduceByKey`  `aggregateByKey` (to compute stats)  `sortBy`.

// %% [markdown]
// ---
//
// # Comparison Goal
//
// Implement both using **RDDs** (manual logic) vs. **DataFrames** (SQL/Optimization) to compare:
//
// 1. **Code Complexity:** Lines of code.
// 2. **Performance:** Execution time and shuffle size in Spark UI.
//
// **Would you like the specific mathematical formulas for the RDD variance calculation?**

// %%
val a = List(1,2,3)
