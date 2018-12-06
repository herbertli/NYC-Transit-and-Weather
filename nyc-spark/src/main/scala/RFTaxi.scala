// Import the rest of the required packages
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*
Code based on:
https://github.com/combust/mleap-demo/blob/master/notebooks/airbnb-price-regression.ipynb
 */
object RFTaxi {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Usage: RFTaxi <input path> <model output path>")
      System.exit(0)
    }

    val inputFile = args(0)
    val outputDir = args(1)

    val spark = SparkSession
      .builder()
      .appName("Spark RF NYC taxi")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    // add feature columns and drop null values
    val dataset = spark.sqlContext.read.format("csv")
      .schema(DataSchema.JoinedSchema)
      .load(inputFile)
      .drop("dropOffTime", "tripDistance", "pickupId", "dropoffId", "dropoffBoro", "dropoffHood")
      .withColumn("pu_year", year($"pickupTime"))
      .withColumn("pu_month", month($"pickupTime"))
      .withColumn("pu_dayofyear", dayofyear($"pickupTime"))
      .withColumn("pu_dayofweek", dayofweek($"pickupTime"))
      .withColumn("pu_day", dayofmonth($"pickupTime"))
      .withColumn("pu_hour", hour($"pickupTime"))
      .withColumn("pu_min", min($"pickupTime"))
      .na.drop()

    val continuousFeatures = Array(
      "prcp",
      "snwd",
      "snow",
      "tavg",
      "tmax",
      "tmin",
      "awnd",
      "pu_year",
      "pu_month"
    )

    val categoricalFeatures = Array(
      "pickupBoro",
      "pickupHood",
      "fog",
      "thunder",
      "hail",
      "haze"
    )

    // Assemble feature vectors
    val continuousFeatureAssembler = new VectorAssembler(uid = "continuous_feature_assembler")
      .setInputCols(continuousFeatures)
      .setOutputCol("unscaled_continuous_features")

    // scale features
    val continuousFeatureScaler = new StandardScaler(uid = "continuous_feature_scaler")
      .setInputCol("unscaled_continuous_features")
      .setOutputCol("scaled_continuous_features")

    // index categorical features
    val categoricalFeatureIndexers = categoricalFeatures.map {
      feature => new StringIndexer(uid = s"string_indexer_$feature")
        .setInputCol(feature)
        .setOutputCol(s"${feature}_index")
    }

    // One-hot encode categorical features
    val categoricalFeatureOneHotEncoders = categoricalFeatureIndexers.map {
      indexer => new OneHotEncoderEstimator(uid = s"oh_encoder_${indexer.getOutputCol}")
        .setInputCols(Array(indexer.getOutputCol))
        .setOutputCols(Array(s"${indexer.getOutputCol}_oh"))
    }

    val featureColsRf = categoricalFeatureIndexers.map(_.getOutputCol).union(Seq("scaled_continuous_features"))

    // assemble all processes categorical and continuous features into a single feature vector
    val featureAssemblerRf = new VectorAssembler(uid = "feature_assembler_rf")
      .setInputCols(featureColsRf)
      .setOutputCol("features_rf")

    val estimators: Array[PipelineStage] = Array(continuousFeatureAssembler, continuousFeatureScaler)
      .union(categoricalFeatureIndexers)
      .union(categoricalFeatureOneHotEncoders)
      .union(Seq(featureAssemblerRf))

    val featurePipeline = new Pipeline(uid = "feature_pipeline")
      .setStages(estimators)

    val sparkFeaturePipelineModel = featurePipeline.fit(dataset)

    println("Finished constructing the pipeline")

    // Create our random forest model
    val randomForest = new RandomForestRegressor(uid = "random_forest_regression")
      .setFeaturesCol("features_rf")
      .setLabelCol("passengers")
      .setPredictionCol("passengers_prediction")

    val sparkPipelineEstimatorRf = new Pipeline().setStages(Array(sparkFeaturePipelineModel, randomForest))
    val sparkPipelineRf = sparkPipelineEstimatorRf.fit(dataset)

    println("Complete: Training Random Forest")
    val transformedDF = sparkPipelineRf.transform(dataset)
    transformedDF.show()

//    sparkPipelineRf.save(outputDir)

  }

}
