import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.LongType

object casa {
  val spark = {
    SparkSession.builder()
      .master("local[1]")
      .getOrCreate()
  }

  val casa_data = spark.read.option("multiline", true).json("/home/fredy/Documents/casa.json")

  val w = Window.partitionBy().orderBy(asc("fecha"))

  casa_data
    //.withColumn("aux_med", lag("medida",1).over(w))
    //.withColumn("aux_fecha", lag("fecha",1).over(w))
    .withColumn("test_med",
      col("medida") - lag("medida",1).over(w))
    .withColumn("test_fecha", to_timestamp(col("fecha")).cast(LongType)/3600
      - to_timestamp(lag(col("fecha"),1).over(w)).cast(LongType)/3600
      ).show()
}
