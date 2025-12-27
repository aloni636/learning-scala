// script to be loaded at Spark shell startup
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.storage.StorageLevel

// NOTE: case class must be defined outside of object to be used in Dataset
//       because of Scala's type erasure, meaning at runtime the type information is lost
//       thus Dataset cannot infer the schema of the case class if it's defined inside an object
case class SchemaField(name: String, dataType: String, path: String)

object SparkUtils {
  // function to compare schema of multiple DataFrames
  def compareParquetSchemas(
      basePath: String,
      othersPath: String*
  )(implicit spark: SparkSession): Unit = {
    val df = spark.read.parquet(basePath)

    val dfFields = df.schema.toSet
    othersPath.foreach { otherPath =>
      val other = spark.read.parquet(otherPath)
      val otherFields = other.schema.toSet

      val missingFromOther = dfFields.diff(otherFields)
      val missingFromBase = otherFields.diff(dfFields)

      val prefix = s"[Base: ${basePath}] <-> [Other: ${otherPath}]"
      if (missingFromOther.isEmpty && missingFromBase.isEmpty) {
        println(s"$prefix Identical")
      } else {
        def printDiff(diffSet: Set[StructField], name: String): Unit = {
          println(s"  Not in $name:")
          println(
            s"${diffSet.toList.sortBy(_.name).map("    - " + _.toString()).mkString("\n")}"
          )
        }
        println(s"$prefix Different")
        if (!missingFromBase.isEmpty) {
          printDiff(missingFromBase, "Base")
        }
        if (!missingFromOther.isEmpty) {
          printDiff(missingFromOther, "Other")
        }
      }
    }
  }

  def compareFlatParquetSchemas(
      basePath: String,
      othersPath: String*
  )(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val schemaToDS = (path: String) =>
      spark.read
        .parquet(path)
        .schema
        .map(f => SchemaField(f.name, f.dataType.toString, path))
        .toDS()

    val base = schemaToDS(basePath)
    val others = othersPath.map(schemaToDS)
    others.foreach(other => {
      println(s"${base.first().path} <-> ${other.first().path}")
      if (base == other) {
        println("  Same Schema")
        return
      }
      val joined = base
        .joinWith(other, base("name") === other("name"), "outer")
        .persist(StorageLevel.MEMORY_ONLY)

      // exists only in base
      val onlyInBase = joined.filter { case (b, o) => o.name == null }.collect()
      if (!onlyInBase.isEmpty) {
        println(
          s"  Only in Base: ${onlyInBase.map { case (b, o) => s"'${b.name}'" }.mkString(", ")}"
        )
      }
      // exists only in other
      val onlyInOther =
        joined.filter { case (b, o) => b.name == null }.collect()
      if (!onlyInOther.isEmpty) {
        println(
          s"  Only in Other: ${onlyInOther.map { case (b, o) => s"'${o.name}'" }.mkString(", ")}"
        )
      }
      // same name, different datatype
      val differentTypes = joined
        .filter { case (b, o) =>
          b.name == o.name && b.dataType != o.dataType
        }
        .collect()
      if (!differentTypes.isEmpty) {
        println("  Different Data Types")
        println(
          differentTypes
            .map { case (b, o) =>
              s"  * '${b.name}': '${b.dataType}' <-> '${o.dataType}'"
            }
            .mkString("\n")
        )
      }
    })
  }
}

// implicit val implicitSpark = spark
