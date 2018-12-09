//import java.sql.Timestamp
//import java.time.LocalDateTime
//import java.time.format.DateTimeFormatter
//
//import org.apache.spark.ml.PipelineModel
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions._
//
//object PredGreen {
//
//  def main(args: Array[String]): Unit = {
//
//    if (args.length != 1) {
//      println("Usage: PredGreen <model path>")
//      System.exit(0)
//    }
//
//    val modelPath = args(0)
//
//    val spark = SparkSession
//      .builder()
//      .appName("Spark RF NYC taxi")
////      .config("spark.master", "local")
//      .getOrCreate()
//    spark.sparkContext.setLogLevel("ERROR")
//
//    val rfModel = PipelineModel.load(modelPath)
//
//    println("Just a couple of questions...")
//    println("What's today's date? MM/dd/yyyy:")
//    println(">")
//    val userIn = scala.io.StdIn.readLine().trim
//
//    println("What's the time? HH:mm:ss, 24-hr time please!:")
//    println(">")
//    val timeString = scala.io.StdIn.readLine().trim
//
//    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss")
//    val date: LocalDateTime = LocalDateTime.parse(userIn + " " + timeString, formatter)
//
//    println("What's the temperature outside? (in Fahrenheit):")
//    println(">")
//    val temperature: Double = scala.io.StdIn.readDouble()
//
//    println("Is it raining? ('y' or 'n'):")
//    println(">")
//    val raining: Int = if (scala.io.StdIn.readChar() == 'y') 1 else 0
//
//    println("Is it snowing? ('y' or 'n'):")
//    println(">")
//    val snowing: Int = if (scala.io.StdIn.readChar() == 'y') 1 else 0
//
//    println("What borough are you in:")
//    println(">")
//    val boro: String = scala.io.StdIn.readLine().trim
//
//    println("What neighborhood are you in:")
//    println(">")
//    val hood: String = scala.io.StdIn.readLine().trim
//
//    import spark.implicits._
//
//    val vs = Seq(FeatureVector(Timestamp.valueOf(date), boro, hood, raining, snowing, temperature))
//    val df = vs
//      .toDF()
//      .withColumn("pu_month", month($"pickupTime"))
//      .withColumn("pu_dayofyear", dayofyear($"pickupTime"))
//      .withColumn("pu_dayofweek", dayofweek($"pickupTime"))
//      .withColumn("pu_day", dayofmonth($"pickupTime"))
//      .withColumn("pu_hour", hour($"pickupTime"))
//      .withColumn("pu_min", minute($"pickupTime"))
//
//    val pred = rfModel.transform(df)
//    pred.select("passengers_prediction").show()
//
//
//  }
//
//}
