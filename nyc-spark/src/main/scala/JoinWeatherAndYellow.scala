import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object JoinWeatherAndYellow {
  def main(args: Array[String]) {

    if (args.length != 3) {
      println("Usage: JoinWeatherAndTaxi <taxi path> <weather path> <output path>")
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
      .schema(DataSchema.YellowCabSchema)
      .csv(taxiDataPath)
      .withColumn("year", year($"pickupTime"))
      .withColumn("month", month($"pickupTime"))
      .withColumn("dayofmonth", dayofmonth($"pickupTime"))

    val weatherDF = spark.read
      .option("dateFormat", "MM/dd/yyyy")
      .schema(DataSchema.WeatherSchema)
      .csv(weatherDataPath)
      .withColumn("year", year($"yeardate"))
      .withColumn("month", month($"yeardate"))
      .withColumn("dayofmonth", dayofmonth($"yeardate"))

    val joinedDF = taxiDF.join(weatherDF, taxiDF("year") === weatherDF("year") && taxiDF("month") === weatherDF("month") && taxiDF("dayofmonth") === weatherDF("day"))
    joinedDF.write.csv(outputPath)

    spark.stop()

  }
}