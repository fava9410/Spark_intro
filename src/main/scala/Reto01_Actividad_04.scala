//Import para Jupyter-notebooks

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Actividad4 {
  val spark = {
    SparkSession.builder()
      .master("local[1]")
      .getOrCreate()
  }

  val playstore = spark.read
    .options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
    .csv("resources/playstore/googleplaystore.csv")

  val reviews = spark.read
    .options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
    .csv("resources/playstore/googleplaystore_user_reviews.csv")

  val bestApps = playstore.filter(col("Rating") > 4.7 && col("Rating") =!= "NaN")

  val result = bestApps.join(reviews, bestApps("App") === reviews("AppName"), "inner")
    .filter(col("Sentiment") === "Negative")
    .sort(col("Sentiment_Polarity").desc)
    .select("App", "Translated_Review")
    .collect()

  assert(result.contains(Row("Fuzzy Seasons: Animal Forest", "It cute I actually sad time fox graduate get attached characters. Over good game Niro's comments begging questionable borderline inappropriate though I think reword good job. Will continue play game!")))
}