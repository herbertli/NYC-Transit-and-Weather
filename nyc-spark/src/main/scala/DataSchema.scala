import org.apache.spark.sql.types._

object DataSchema {
  // 2084-11-04 11:27:28,2084-11-04 11:39:52,1.07,170,68,Manhattan,Murray Hill,Manhattan,East Chelsea,1
  val YellowCabSchema = new StructType()
    .add("pickupTime", TimestampType, nullable = true)
    .add("dropOffTime", TimestampType, nullable = true)
    .add("pickupId", IntegerType, nullable = true)
    .add("dropoffId", IntegerType, nullable = true)
    .add("pickupBoro", StringType, nullable = true)
    .add("pickupHood", StringType, nullable = true)
    .add("dropoffBoro", StringType, nullable = true)
    .add("dropoffHood", StringType, nullable = true)
    .add("passengers", IntegerType, nullable = true)

  // 2018-06-30 23:59:57,2018-07-01 00:21:14,4.31,66,17,Brooklyn,DUMBO/Vinegar Hill,Brooklyn,Bedford,1
  val GreenCabSchema = new StructType()
    .add("pickupTime", TimestampType, nullable = true)
    .add("dropOffTime", TimestampType, nullable = true)
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
  val WeatherSchema = new StructType()
    .add("yeardate", DateType, nullable = true)
    .add("prcp", DoubleType, nullable = true)
    .add("snwd", DoubleType, nullable = true)
    .add("snow", DoubleType, nullable = true)
    .add("tavg", DoubleType, nullable = true)
    .add("tmax", DoubleType, nullable = true)
    .add("tmin", DoubleType, nullable = true)
    .add("awnd", DoubleType, nullable = true)
    .add("fog", IntegerType, nullable = true)
    .add("thunder", IntegerType, nullable = true)
    .add("hail", IntegerType, nullable = true)
    .add("haze", IntegerType, nullable = true)
}