//Import para Jupyter-notebooks
import org.apache.spark.sql._

object  Actividad2 {
  val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this) //jupyter
  case class S4ner(name: String, lastName: String, bornOn: String, gender: String)

  val employees = Seq(S4ner("Luis", "Perez", "1989-08-01", "M"), S4ner("Maria", "Rodriguez", "1988-01-10", "F"))

  val ds: Dataset[S4ner] = employees.toDS()
  ds.show

  val df: DataFrame = spark.sparkContext.parallelize(employees).toDF()
  df.show()

  df.select("name", "lastName").show()
  ds.map(x => (x.name, x.lastName)).show()

  import java.time.LocalDate
  import java.time.Period

  def calculateAge(s4ner: S4ner): Int = {
    val now = LocalDate.now()
    val birthday = LocalDate.parse(s4ner.bornOn)
    Period.between(birthday, now).getYears()
  }

  val ages: Array[Int] = ds.map(calculateAge(_)).collect()
}