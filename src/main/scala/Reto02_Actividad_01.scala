//Import para Jupyter-notebooks 

import org.apache.spark.sql._

object Actividad21 {

  val spark = {
    SparkSession.builder()
      .master("local[*]")
      .getOrCreate()
  }

  import spark.implicits._
  val df = spark.read.option("header" , "true")
    .csv("resources/Impuesto_deTransporte-_MinEnerg_a.csv")

  val dff = spark.read.format("csv").option("header", true).load("resources/Impuesto_de_Transporte_-_MinEnerg_a.csv")

  val query = "select ano, departamento, municipio from impuestos"
  df.createOrReplaceTempView("impuestos")
  spark.sql(query)

  df.createOrReplaceTempView("impuestos")
  spark.sql(query).where("departamento = 'BOLIVAR'").show()
}