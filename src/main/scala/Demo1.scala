import org.apache.spark.sql.SparkSession

object Demo1 {
  def main(args: Array[String]): Unit = {
    val sfile = "/usr/local/spark/README.md"
    val spark = SparkSession.builder.appName("Simple Application")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sdata = spark.read.textFile(sfile).cache()

    val numAs = sdata.filter(line => line.contains("a")).count()
    val numBs = sdata.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")

    spark.stop()

    Actividad24
  }
}
