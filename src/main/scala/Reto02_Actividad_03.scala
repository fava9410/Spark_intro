//Import para Jupyter-notebooks

import org.apache.spark.sql._

object Actividad23 {
  val spark = {
    SparkSession.builder()
      .master("local[*]")
      .getOrCreate()
  }

  val impuestoDf = spark.read.option("header","true").csv("resources/Impuesto_deTransporte-_MinEnerg_a.csv ")


  import spark.implicits._


  import org.apache.spark.sql.functions.count

  impuestoDf.select(count("ano")).show()

  // dataframes
  import org.apache.spark.sql.functions.sum

  impuestoDf.where("ano = 2019 and trimestre = 1").select(sum("totalimpuesto").as("total")).show()
  // SQL
  impuestoDf.createOrReplaceTempView("impuestos")
  spark.sql("select sum(totalimpuesto) as total from impuestos where ano = 2019 and trimestre = 1").show()

  import org.apache.spark.sql.functions.{sum, count, avg, min, max}

  val totals = impuestoDf.where("ano = 2019 and trimestre = 1").select(
    count("totalimpuesto").alias("numImpuestos"),
    sum("totalimpuesto").as("totalPagado"),
    avg("totalimpuesto").as("promedioPagado"),
    max("totalimpuesto").as("MayorTotalPagado"),
    min("totalimpuesto").as("MenorTotalPagado")
  )

  totals.show()
  // esta función sirve para describir una columna, obtiene el count, mean, desviación estandar, min y max.
  impuestoDf.select("totalimpuesto").describe().show()

  impuestoDf.groupBy('departamento).count().orderBy('count).show
  impuestoDf.createOrReplaceTempView("impuestosTable")
  spark.sql("SELECT departamento,count(1) as count FROM impuestosTable GROUP BY departamento ORDER BY count").show()


  //SQL
  spark.sql(
    """
    SELECT departamento,sum(totalimpuesto) as total
    FROM impuestosTable GROUP BY departamento ORDER BY total desc
    """).show()
}