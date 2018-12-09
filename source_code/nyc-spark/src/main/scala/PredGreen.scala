import java.sql.Date
import java.util.Calendar

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object PredGreen {

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      println("Usage: PredGreen <model path> <input file> <output path>")
      System.exit(0)
    }

    val modelPath = args(0)
    val inputPath = args(1)
    val outputPath = args(2)

    val spark = SparkSession
      .builder()
      .appName("Spark LR NYC taxi")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val rfModel = PipelineModel.load(modelPath)

    import spark.implicits._

    val dowUDF = udf((date: Date) => {
      val cal = Calendar.getInstance()
      cal.setTime(date)
      cal.get(Calendar.DAY_OF_WEEK)
    })

    // 2018-06-10T23:52:33.000-04:00,Queens,Long Island City/Hunters Point,1,1
    val FeatureRow: StructType = StructType(Seq(
      StructField("pickupTime", TimestampType),
      StructField("pickupBoro", StringType),
      StructField("pickupHood", StringType),
      StructField("tavg", DoubleType),
      StructField("prcp_b", IntegerType),
      StructField("snow_b", IntegerType)
    ))

    val df = spark.read.schema(FeatureRow)
      .csv(inputPath)
      .withColumn("pu_month", month($"pickupTime"))
      .withColumn("pu_dayofyear", dayofyear($"pickupTime"))
      .withColumn("pu_dayofweek", dowUDF($"pickupTime"))
      .withColumn("pu_day", dayofmonth($"pickupTime"))
      .withColumn("pu_hour", hour($"pickupTime"))
      .withColumn("pu_min", minute($"pickupTime"))

    val pred = rfModel.transform(df)
      .drop("pu_month", "pu_dayofyear", "pu_dayofweek", "pu_day", "pu_hour", "pu_min")

    pred.write
      .option("header", value = true)
      .option("timestampFormat", value = "yyyy-MM-dd'T'HH:mm:ss")
      .csv(outputPath)

  }

}
