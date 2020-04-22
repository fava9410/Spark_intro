//Import para Jupyter-notebooks

import org.apache.spark.sql._

object Actividad1 {
  val spark = {
    SparkSession.builder()
      .master("local[1]")
      .getOrCreate()
  }

  import spark.implicits._

  val ds: Dataset[Long] = spark.range(100).as[Long]
  ds.show(10)

  val evenNumbers = ds.filter(n => n % 2 == 0)
  evenNumbers.show(10)

  val count = evenNumbers.count()

}