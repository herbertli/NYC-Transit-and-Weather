import org.apache.spark.sql.types._

object DataSchema {
  // 2084-11-04 11:27:28,2084-11-04 11:39:52,1.07,170,68,Manhattan,Murray Hill,Manhattan,East Chelsea,1
  val YellowCabSchema: StructType = new StructType()
    .add("pickupTime", TimestampType, nullable = true)
    .add("dropOffTime", TimestampType, nullable = true)
    .add("tripDistance", DoubleType, nullable = true)
    .add("pickupId", IntegerType, nullable = true)
    .add("dropoffId", IntegerType, nullable = true)
    .add("pickupBoro", StringType, nullable = true)
    .add("pickupHood", StringType, nullable = true)
    .add("dropoffBoro", StringType, nullable = true)
    .add("dropoffHood", StringType, nullable = true)
    .add("passengers", IntegerType, nullable = true)

  // 2018-06-30 23:59:57,2018-07-01 00:21:14,4.31,66,17,Brooklyn,DUMBO/Vinegar Hill,Brooklyn,Bedford,1
  val GreenCabSchema: StructType = new StructType()
    .add("pickupTime", TimestampType, nullable = true)
    .add("dropOffTime", TimestampType, nullable = true)
    .add("tripDistance", DoubleType, nullable = true)
    .add("pickupId", IntegerType, nullable = true)
    .add("dropoffId", IntegerType, nullable = true)
    .add("pickupBoro", StringType, nullable = true)
    .add("pickupHood", StringType, nullable = true)
    .add("dropoffBoro", StringType, nullable = true)
    .add("dropoffHood", StringType, nullable = true)
    .add("passengers", IntegerType, nullable = true)

  // 2018-06-30 23:59:57,2018-07-01 00:20:44,247,259,Bronx,West Concourse,Bronx,Woodlawn/Wakefield,1
  val FHVSchema = new StructType()
    .add("pickupTime", TimestampType, nullable = true)
    .add("dropOffTime", TimestampType, nullable = true)
    .add("pickupId", IntegerType, nullable = true)
    .add("dropoffId", IntegerType, nullable = true)
    .add("pickupBoro", StringType, nullable = true)
    .add("pickupHood", StringType, nullable = true)
    .add("dropoffBoro", StringType, nullable = true)
    .add("dropoffHood", StringType, nullable = true)
    .add("passengers", IntegerType, nullable = true)

  // 09/30/2018, 0.00, 0.0, 0.0, 65, 73, 58, 7.2, 0, 0, 0, 0
  val WeatherSchema: StructType = StructType(Seq(
    StructField("yeardate", StringType, nullable = true),
    StructField("prcp", DoubleType, nullable = true),
    StructField("snwd", DoubleType, nullable = true),
    StructField("snow", DoubleType, nullable = true),
    StructField("tavg", DoubleType, nullable = true),
    StructField("tmax", DoubleType, nullable = true),
    StructField("tmin", DoubleType, nullable = true),
    StructField("awnd", DoubleType, nullable = true),
    StructField("fog", DoubleType, nullable = true),
    StructField("thunder", DoubleType, nullable = true),
    StructField("hail", DoubleType, nullable = true),
    StructField("haze", DoubleType, nullable = true)
  ))
}