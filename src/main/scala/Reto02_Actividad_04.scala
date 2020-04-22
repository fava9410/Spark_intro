//Import para Jupyter-notebooks
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Actividad24 {
  val spark = {
    SparkSession.builder()
      .master("local[1]")
      .getOrCreate()
  }
  val path = "resources/Impuesto_de_Transporte_-_MinEnerg_a.csv"


  //Resultados esperados
  /*
Resultado esperado instrucción 3 
+----+------------+--------------------+
| ano|departamento|            totalano|
+----+------------+--------------------+
|2015|   ANTIOQUIA|     2.0957180299E10|
|2016|   ANTIOQUIA|     1.7395256286E10|
|2017|   ANTIOQUIA|     2.1410114208E10|
|2018|   ANTIOQUIA|      1.783454384E10|
|2019|   ANTIOQUIA|       7.734222207E9|
|2015|      ARAUCA|       4.679990028E9|
|2016|      ARAUCA| 6.251674119000001E9|
|2017|      ARAUCA| 6.187288592000003E9|
|2018|      ARAUCA| 4.374012615999996E9|
|2019|      ARAUCA|2.2934080163999996E9|
*/

  /*
Resultado esperado Instrucción 4
+----+------------+--------------------+--------------------+
| ano|departamento|            totalano|            totalacc|
+----+------------+--------------------+--------------------+
|2015|   ANTIOQUIA|     2.0957180299E10|     2.0957180299E10|
|2016|   ANTIOQUIA|     1.7395256286E10|     3.8352436585E10|
|2017|   ANTIOQUIA|     2.1410114208E10|     5.9762550793E10|
|2018|   ANTIOQUIA|      1.783454384E10|     7.7597094633E10|
|2019|   ANTIOQUIA|       7.734222207E9|      8.533131684E10|
|2015|      ARAUCA|       4.679990028E9|       4.679990028E9|
|2016|      ARAUCA| 6.251674119000001E9|     1.0931664147E10|
|2017|      ARAUCA| 6.187288592000003E9|1.711895273900000...|
|2018|      ARAUCA| 4.374012615999996E9|2.149296535500000...|
|2019|      ARAUCA|2.2934080163999996E9|2.378637337140000...|
|2015|     BOLIVAR|        9.16817554E8|        9.16817554E8|
|2016|     BOLIVAR|       2.027336645E9|       2.944154199E9|
|2017|     BOLIVAR|       4.254224718E9|       7.198378917E9|
|2018|     BOLIVAR|       3.383628179E9|     1.0582007096E10|
|2019|     BOLIVAR|       1.208467628E9|     1.1790474724E10
*/

  /*
Resultado esperado 5a.
+----+------------+--------------------+--------------------+
| ano|departamento|            totalano|            lastyear|
+----+------------+--------------------+--------------------+
|2015|   ANTIOQUIA|     2.0957180299E10|                null|
|2016|   ANTIOQUIA|     1.7395256286E10|     2.0957180299E10|
|2017|   ANTIOQUIA|     2.1410114208E10|     1.7395256286E10|
|2018|   ANTIOQUIA|      1.783454384E10|     2.1410114208E10|
|2019|   ANTIOQUIA|       7.734222207E9|      1.783454384E10|
|2015|      ARAUCA|       4.679990028E9|                null|
|2016|      ARAUCA| 6.251674119000001E9|       4.679990028E9|
|2017|      ARAUCA| 6.187288592000003E9| 6.251674119000001E9|
|2018|      ARAUCA| 4.374012615999996E9| 6.187288592000003E9|
|2019|      ARAUCA|2.2934080163999996E9| 4.374012615999996E9|
|2015|     BOLIVAR|        9.16817554E8|                null|
|2016|     BOLIVAR|       2.027336645E9|        9.16817554E8|
*/

  /*
Resultado esperado 5b
+----+------------+--------------------+--------------------+--------------------+-----+
| ano|departamento|            totalano|            lastyear|          difference|Trend|
+----+------------+--------------------+--------------------+--------------------+-----+
|2015|   ANTIOQUIA|     2.0957180299E10|                null|                null|IGUAL|
|2016|   ANTIOQUIA|     1.7395256286E10|     2.0957180299E10|      -3.561924013E9| BAJO|
|2017|   ANTIOQUIA|     2.1410114208E10|     1.7395256286E10|       4.014857922E9|SUBIO|
|2018|   ANTIOQUIA|      1.783454384E10|     2.1410114208E10|      -3.575570368E9| BAJO|
|2019|   ANTIOQUIA|       7.734222207E9|      1.783454384E10|    -1.0100321633E10| BAJO|
|2015|      ARAUCA|       4.679990028E9|                null|                null|IGUAL|
|2016|      ARAUCA| 6.251674119000001E9|       4.679990028E9| 1.571684091000001E9|SUBIO|
|2017|      ARAUCA| 6.187288592000003E9| 6.251674119000001E9|-6.438552699999809E7| BAJO|
|2018|      ARAUCA| 4.374012615999996E9| 6.187288592000003E9|-1.81327597600000...| BAJO|
|2019|      ARAUCA|2.2934080163999996E9| 4.374012615999996E9|-2.08060459959999...| BAJO|
|2015|     BOLIVAR|        9.16817554E8|                null|                null|IGUAL|
|2016|     BOLIVAR|       2.027336645E9|        9.16817554E8|       1.110519091E9|SUBIO|
|2017|     BOLIVAR|       4.254224718E9|       2.027336645E9|       2.226888073E9|SUBIO|
*/

  val impuestoDf = spark.read.format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .load("resources/Impuesto_de_Transporte_-_MinEnerg_a.csv")

  impuestoDf.cache()
  impuestoDf.createOrReplaceTempView("impuestosTable")

  impuestoDf.select(count("*"), sum("totalimpuesto")).show()
  val df3 = impuestoDf.groupBy("ano", "departamento")
    .agg(sum("totalimpuesto").alias("totalano"))
    .orderBy("departamento", "ano")

  spark.time(df3.show())

  val w4 = Window.partitionBy("departamento")
      .orderBy("departamento", "ano")
      .rangeBetween(Window.unboundedPreceding, Window.currentRow)

  spark.time(df3.withColumn("totalacc",
    sum("totalano").over(w4))
    .orderBy("departamento", "ano")
    .show())

  val w5 = Window.partitionBy("departamento")
    .orderBy("departamento", "ano")

  val df5a = df3.withColumn("lastyear",
    lag("totalano", 1).over(w5))
    .orderBy("departamento", "ano")

  df5a.show()
spark.time(
  df5a.withColumn("difference", col("totalano") - col("lastyear"))
    .withColumn("TREND",
      when(col("difference") < 0, "BAJO")
    .when(col("difference") > 0, "SUBIO")
    .otherwise("IGUAL"))
    .show())
/*
  val w = Window.partitionBy( "departamento")
      .orderBy("departamento", "ano")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  impuestoDf.select(col("ano"),
    col("departamento"),
    col("totalimpuesto"),
    sum("totalimpuesto") over (w) as "queu").show()
*/

  spark.sql(
    """
SELECT 
    distinct departamento, sum(totalimpuesto) as total 
FROM 
    impuestosTable 
GROUP BY departamento 
ORDER BY departamento
""").show()

  spark.sql(
    """
SELECT 
    distinct ano, departamento, sum(totalimpuesto) over (partition by departamento, ano order by ano) as totalano
FROM 
    impuestosTable 
ORDER BY departamento, ano
""").show()

  spark.sql(
    """
SELECT 
    distinct ano,departamento, Sum(totalimpuesto) over (partition by departamento, ano order by ano) as totalano,
    sum(totalimpuesto) over (partition by departamento order by ano) as totalacc
FROM 
    impuestosTable 
ORDER BY departamento, ano
""").show()
}