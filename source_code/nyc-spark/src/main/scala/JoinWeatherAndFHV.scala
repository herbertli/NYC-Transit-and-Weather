import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object JoinWeatherAndFHV {

  def main(args: Array[String]) {

    if (args.length != 3) {
      println("Usage: JoinWeatherAndFHV <taxi path> <weather path> <output path>")
      return
    }

    val taxiDataPath = args(0)
    val weatherDataPath = args(1)
    val outputPath = args(2)

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    val taxiDF = spark.read
      .schema(DataSchema.FHVSchema)
      .csv(taxiDataPath)
      .withColumn("year", year($"pickupTime"))
      .withColumn("month", month($"pickupTime"))
      .withColumn("dayofmonth", dayofmonth($"pickupTime"))

    val weatherDF = spark.read
      .schema(DataSchema.WeatherSchema)
      .csv(weatherDataPath)
      .withColumn("yeardate", to_date($"yeardate", "MM/dd/yyyy"))
      .withColumn("year", year($"yeardate"))
      .withColumn("month", month($"yeardate"))
      .withColumn("dayofmonth", dayofmonth($"yeardate"))

    val joinedDF = taxiDF.join(weatherDF, Seq("year", "month", "dayofmonth"))
      .drop("year", "month", "dayofmonth", "yeardate")
    joinedDF.write.csv(outputPath)

    spark.stop()

  }
}