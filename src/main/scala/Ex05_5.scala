package hello
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.{functions => F, types => T}
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger

/*

## Question 1 - Zone-to-Zone Flow Efficiency

### Business question

> For each **pickup -> drop-off zone pair**, compute:
>
> * trip count
> * average trip duration
> * average speed
> * average fare per mile
>   Then rank the **top 20 busiest flows** during weekday rush hours.

---

This is a **pure fact-table grind** with:

- composite keys
- heavy shuffles
- multi-metric aggregation

DataFrames make this trivial.
RDDs force you to *understand the mechanics*.

---

### RDD concepts you’ll have to use

#### Parsing & projection

- `map` - parse rows into typed case classes
- Drop unused columns early (memory pressure)

#### Key modeling
- Composite keys: `(PULocationID, DOLocationID)`
- Time filters before shuffle

#### Aggregation
- `mapValues`
- `reduceByKey`
- Or `aggregateByKey` / `combineByKey` (better)

You’ll need to accumulate:
- count
- sum(duration)
- sum(distance)
- sum(fare)

This forces you to design:
- accumulator structs
- associative merge logic

#### Derived metrics

- Average speed = distance / duration
- Fare per mile
- Requires **post-aggregation map**

#### Sorting & ranking

- `sortBy`
- `takeOrdered`

---

### APIs you will touch
- `map`
- `filter`
- `keyBy`
- `reduceByKey` / `aggregateByKey`
- `mapValues`
- `sortBy`
- `take` / `takeOrdered`
- `persist`

This single question covers **70% of real RDD usage**.

---

## Question 2 - Time-Weighted Zone Demand Volatility

### Business question

> For each pickup zone:
>
> * Compute hourly trip counts
> * Measure how volatile demand is across the day
> * Rank zones by **demand instability**

In plain terms:

> Which neighborhoods have the most *uneven* taxi demand?

---

### Why this one is important

This introduces:

- time bucketing
- secondary grouping
- multi-stage aggregation
- nested key logic

---

### RDD mechanics it forces

#### Time normalization

- Extract hour from timestamp
- You must decide:

  - pre-bucket
  - or group then bucket

#### Hierarchical keys

You will naturally form:

- `(zone, hour)` -> count
- Then regroup by `zone`

This **forces you to reshape keys**.

#### Multi-stage aggregation

Likely pipeline:

1. `(zone, hour)` -> count
2. `zone` -> list of hourly counts
3. compute variance / stddev / range

That means:

- `map`
- `reduceByKey`
- `groupByKey` (and learning *why it hurts*)
- Or smarter `aggregateByKey`

#### Custom statistics

You’ll compute:

- mean
- variance
- max / min

No library help. You own the math.

---

### APIs you will touch

- `map`
- `filter`
- `reduceByKey`
- `aggregateByKey`
- `groupByKey (and regret it)`
- `mapPartitions`
- `persist`

---

## Why these two together are enough

Combined, they force you to learn:
- Key/value modeling
- Composite keys
- Accumulators
- Shuffle behavior
- Memory pressure
- Multi-stage aggregation
- Sorting
- Performance intuition

Without:
- streaming
- joins
- GIS
- DataFrames
- artificial toy logic

---

Do **both** questions:

1. First with **RDDs only**
2. Then redo them with **DataFrames**

Compare:
- line count
- mental load
- Spark UI
- shuffle size
- execution time

 */

object Ex05_5 extends Exercise {
  // NOTE: We use a dedicated object to run Ex05_5 Spark job because Spark library is `% "provided"`,
  //       meaning we can't load objects dependent on its existence within sbt run.
  //       This means the runner must be a dedicated object.
  override def run(): Unit = {
    SparkRunner.runSparkJob("Ex05_5_Job")
  }
}

object Ex05_5_Job {
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
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
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
        // .select(
        //   "tpep_pickup_datetime",
        //   "tpep_dropoff_datetime",
        //   "trip_distance",
        //   "fare_amount",
        //   "PULocationID",
        //   "DOLocationID"
        // )
    )
    val taxi = normParquets
      .reduce((a, b) => a.unionByName(b))
    // .persist(StorageLevel.MEMORY_ONLY)

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
      $"pickups_per_hour" > percentile
    )

    val rushHoursArr = rushHours.select($"hour").as[Int].collect

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
    // .cache()

    val zones = spark.read
      .option("header", "true")
      .csv("/workspaces/*/data/taxi/taxi_zone_lookup.csv")
      .select(
        $"LocationID".cast(T.IntegerType),
        $"Borough",
        $"Zone"
      )

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
      )
    // .cache()

    val top20BusiestFlows =
      rushHourStatisticsWithZones.orderBy($"count".desc).limit(20)
    top20BusiestFlows.write
      .option("header", "true")
      .mode("overwrite")
      .csv(
        "/workspaces/learning-scala/output/rush_hour_top20_busiest_flows.csv"
      )
  }
}
