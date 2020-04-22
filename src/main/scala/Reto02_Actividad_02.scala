//Import para Jupyter-notebooks

import org.apache.spark.sql._

object Actividad22 {
  val spark = {
    SparkSession.builder()
      .master("local[*]")
      .getOrCreate()
  }

  import spark.implicits._

  val impuestoDf = spark.read.option("header", "true").csv("resources/Impuesto_deTransporte-_MinEnerg_a.csv")
  val municipios = spark.read.option("header", "true").csv("resources/Departamentos_y_municipios_de_Colombia.csv")

  //val impuestoDf = spark.read.format("csv").option("header",true).load("resources/Impuesto_de_Transporte_-_MinEnerg_a.csv")
  //val municipios = spark.read.format("csv").option("header",true).load("resources/Departamentos_y_municipios_de_Colombia.csv")

  val query =
    """
    select * from impuestos i inner join municipios m on i.municipio = m.municipio
"""
  impuestoDf.createOrReplaceTempView("impuestos")
  municipios.createOrReplaceTempView("municipios")
  spark.sql(query)

  impuestoDf.createOrReplaceTempView("impuestos")
  municipios.createOrReplaceTempView("municipios")
  spark.sql(query).show(5)

  val query2 =
    """
    select * from impuestos i inner join municipios m on i.municipio = UPPER(m.municipio)
"""

  municipios.printSchema

  val newMunicipios: DataFrame = municipios.withColumnRenamed("REGION", "region")
    .withColumnRenamed("CÓDIGO DANE DEL DEPARTAMENTO", "codDaneDep")
    .withColumnRenamed("DEPARTAMENTO", "departamento")
    .withColumnRenamed("CÓDIGO DANE DEL MUNICIPIO", "codDaneMunicipio")
    .withColumnRenamed("MUNICIPIO", "municipio")

  // Otra alternativa
  //val newColumns = Seq("region","codDaneDep","departamento", "codDaneMunicipio", "municipio")
  //val newMunicipios = municipios.toDF(newColumns:_*)

  newMunicipios.printSchema
  newMunicipios.createOrReplaceTempView("municipios")

  val query3 =
    """
    select i.ano, i.trimestre,m.codDaneDep, m.departamento, m.codDaneMunicipio, m.municipio, i.totalimpuesto 
    from impuestos i inner join municipios m on i.municipio = UPPER(m.municipio)
"""
  spark.sql(query3).show(5)
}