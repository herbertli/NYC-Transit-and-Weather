// Import the rest of the required packages
import java.sql.Date
import java.util.Calendar

import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*
Code based on:
https://github.com/combust/mleap-demo/blob/master/notebooks/airbnb-price-regression.ipynb
 */
object RFGreen {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Usage: RFGreen <input path> <model output path>")
      System.exit(0)
    }

    val inputFile = args(0)
    val outputDir = args(1)

    val spark = SparkSession
      .builder()
      .appName("Spark LR NYC taxi")
      .getOrCreate()
//    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val dowUDF = udf((date: Date) => {
      val cal = Calendar.getInstance()
      cal.setTime(date)
      cal.get(Calendar.DAY_OF_WEEK)
    })

    // add feature columns and drop null values
    val dataset = spark.sqlContext.read.format("csv")
      .schema(DataSchema.JoinedSchema)
      .load(inputFile)
      .na.drop()
      .withColumn("pu_month", month($"pickupTime"))
      .withColumn("pu_dayofyear", dayofyear($"pickupTime"))
      .withColumn("pu_dayofweek", dowUDF($"pickupTime"))
      .withColumn("pu_day", dayofmonth($"pickupTime"))
      .withColumn("pu_hour", hour($"pickupTime"))
      .withColumn("pu_min", minute($"pickupTime"))
      .drop("dropOffTime", "tripDistance", "pickupId", "dropoffId", "dropoffBoro",
        "dropoffHood", "prcp", "snwd", "snow", "fog", "thunder", "hail", "haze", "tmax", "tmin",
        "awnd", "pickupTime", "")
    println("Loaded and featurized data...")

    val continuousFeatures = Array("tavg", "pu_hour", "pu_min")

    val categoricalFeatures = Array(
      "pickupBoro",
      "pickupHood",
      "prcp_b",
      "snow_b",
      "pu_month",
      "pu_dayofyear",
      "pu_dayofweek",
      "pu_day"
    )

    // Assemble feature vectors
    println("Assembling feature vectors...")
    val continuousFeatureAssembler = new VectorAssembler(uid = "continuous_feature_assembler")
      .setInputCols(continuousFeatures)
      .setOutputCol("unscaled_continuous_features")

    // scale features
    println("Scaling data...")
    val continuousFeatureScaler = new StandardScaler(uid = "continuous_feature_scaler")
      .setInputCol("unscaled_continuous_features")
      .setOutputCol("scaled_continuous_features")

    // index categorical features
    println("Indexing data...")
    val categoricalFeatureIndexers = categoricalFeatures.map {
      feature => new StringIndexer(uid = s"string_indexer_$feature")
        .setInputCol(feature)
        .setOutputCol(s"${feature}_index")
    }

    // One-hot encode categorical features
    println("Adding One-hot encodors data...")
    val categoricalFeatureOneHotEncoders = categoricalFeatureIndexers.map {
      indexer => new OneHotEncoder(uid = s"oh_encoder_${indexer.getOutputCol}")
        .setInputCol(indexer.getOutputCol)
        .setOutputCol(s"${indexer.getOutputCol}_oh")
    }

    val featureColsRf = categoricalFeatureIndexers.map(_.getOutputCol).union(Seq("scaled_continuous_features"))

    // assemble all processes categorical and continuous features into a single feature vector
    println("Assembling all features...")
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

    println("Finished constructing the pipeline!")

    // Create our random forest model
    println("Creating Random Forest...")
    val gbt = new LinearRegression(uid = "linear_regression")
      .setFeaturesCol("features_rf")
      .setLabelCol("passengers")
      .setPredictionCol("passengers_prediction")
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val sparkPipelineEstimatorRf = new Pipeline().setStages(Array(sparkFeaturePipelineModel, gbt))
    println("Training Random Forest...")
    val sparkPipelineRf = sparkPipelineEstimatorRf.fit(dataset)
    println("Completed training Random Forest")

    sparkPipelineRf.save(outputDir)
    println("Model Saved!")

  }

}
