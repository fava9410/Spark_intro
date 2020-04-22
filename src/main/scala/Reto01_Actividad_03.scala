//Import para Jupyter-notebooks

import org.apache.spark.sql._

object Actividad3 {
  val spark = {
    SparkSession.builder()
      .master("local[1]")
      .getOrCreate()
  }

  val path = "resources/colombia.json"
  val departamentos = spark.read.json(path)
  departamentos.printSchema()
  departamentos.show()

  import spark.implicits._

  org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this) //jupyter
  case class Departamento(id: String, departamento: String, ciudades: List[String])

  val ds: Dataset[Departamento] = departamentos.as[Departamento]
  ds.show

  val ciudadesDep = ds.map(d => (d.departamento, d.ciudades.length)).collect()

  val totalCiudades = ds.map(d => d.ciudades.length).reduce(_ + _)

  val ciudadesColombia = ds.flatMap(d => d.ciudades)
    .groupBy("value").count().sort($"count".desc).collect()
}