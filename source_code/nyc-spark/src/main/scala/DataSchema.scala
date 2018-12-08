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
  val FHVSchema: StructType = StructType(Seq(
    StructField("pickupTime", TimestampType, nullable = true),
    StructField("dropOffTime", TimestampType, nullable = true),
    StructField("pickupId", IntegerType, nullable = true),
    StructField("dropoffId", IntegerType, nullable = true),
    StructField("pickupBoro", StringType, nullable = true),
    StructField("pickupHood", StringType, nullable = true),
    StructField("dropoffBoro", StringType, nullable = true),
    StructField("dropoffHood", StringType, nullable = true),
    StructField("passengers", IntegerType, nullable = true)
  ))

  // 09/30/2018, 0.00, 0.0, 0.0, 65, 73, 58, 7.2, 0, 0, 0, 0
  val WeatherSchema: StructType = StructType(Seq(
    StructField("yeardate", StringType, nullable = true),
    StructField("prcp", StringType, nullable = true),
    StructField("prcp_b", StringType, nullable = true),
    StructField("snwd", StringType, nullable = true),
    StructField("snow", StringType, nullable = true),
    StructField("snow_b", StringType, nullable = true),
    StructField("tavg", StringType, nullable = true),
    StructField("tmax", StringType, nullable = true),
    StructField("tmin", StringType, nullable = true),
    StructField("awnd", StringType, nullable = true),
    StructField("fog", StringType, nullable = true),
    StructField("thunder", StringType, nullable = true),
    StructField("hail", StringType, nullable = true),
    StructField("haze", StringType, nullable = true)
  ))

  val JoinedSchema: StructType = new StructType()
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
    .add("prcp", DoubleType, nullable = true)
    .add("snwd", DoubleType, nullable = true)
    .add("snow", DoubleType, nullable = true)
    .add("tavg", DoubleType, nullable = true)
    .add("tmax", DoubleType, nullable = true)
    .add("tmin", DoubleType, nullable = true)
    .add("awnd", DoubleType, nullable = true)
    .add("fog", BooleanType, nullable = true)
    .add("thunder", BooleanType, nullable = true)
    .add("hail", BooleanType, nullable = true)
    .add("haze", BooleanType, nullable = true)
}