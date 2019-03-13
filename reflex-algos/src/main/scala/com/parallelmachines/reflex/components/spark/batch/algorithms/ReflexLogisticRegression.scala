package com.parallelmachines.reflex.components.spark.batch.algorithms

import com.parallelmachines.reflex.components.{ComponentAttribute, FeaturesColComponentAttribute, LabelColComponentAttribute}
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.{LogisticRegression, _}

class ReflexLogisticRegression extends ReflexSparkMLAlgoBase {
  override val label: String = "Logistic Regression"
  override lazy val defaultModelName: String = "Logistic Regression"
  override val description: String = "Logistic Regression Training"
  override val version: String = "1.0.0"

  val lr = new LogisticRegression()

  val tempSharedPath = ComponentAttribute("tempSharedPath", "",
    "temp Shared Path", "Temporary shared path for model transfer, " +
      "paths with prefix file:// or hdfs://", optional = true)
  val elasticNetParam = ComponentAttribute("elasticNetParam", 0.0, "Elastic net param", "Set the ElasticNet mixing parameter. (Default: 0.0)", optional = true)
  val family = ComponentAttribute("family", "auto", "Family", "Parameter for the name of family which is a description of the label distribution to be used in the model. (Options: 'auto'(default), 'binomial', 'multinomial')", optional = true)
  family.setOptions(List[(String, String)](("auto", "auto"), ("binomial", "binomial"), ("multinomial", "multinomial")))
  val fitIntercept = ComponentAttribute("fitIntercept", true, "Fit Intercept", "Whether to fit an intercept term. (Default: true)", optional = true)
  val maxIter = ComponentAttribute("maxIter", 100, "Maximum Iterations", "Maximum number of iterations of gradient descent to run. (Default: 100)", optional = true)
  val predictionCol = ComponentAttribute("predictionCol", "", "Prediction column", "Prediction column name.", optional = true)
  val probabilityCol = ComponentAttribute("probabilityCol", "", "Probability column", "Column name for predicted class conditional probabilities.", optional = true)
  val rawPredictionCol = ComponentAttribute("rawPredictionCol", "", "Raw prediction column", "Raw prediction (a.k.a. confidence) column name.", optional = true)
  val regParam = ComponentAttribute("regParam", 0.0, "Regularization parameter", "Regularization parameter. (Default: 0.0)", optional = true)
  val standardization = ComponentAttribute("standardization", true, "Standardization", "Whether to standardize the training features before fitting the model (Default: true)", optional = true)
  val thresholds = ComponentAttribute("thresholds", List[String](), "Thresholds", "Set thresholds in multiclass (or binary) classification to adjust the probability of predicting each class." +
    "Array must have length equal to the number of classes, with values greater than 0, excepting that at most one value may be 0." +
    "The class with largest value p/t is predicted, where p is the original probability of that class and t is the class's threshold.", optional = true)
  val tolerance = ComponentAttribute("tolerance", 1E-6, "Convergence tolerance", "Set the convergence tolerance of iterations. Smaller value will lead to higher accuracy with the cost of more iterations. (Default: 1E-6)", optional = true)
  val weightCol = ComponentAttribute("weightCol", "", "Weight column", "Sets the value of param weightCol. If this is not set or empty, all instance weights are treated as 1.0.", optional = true)

  val labelCol = LabelColComponentAttribute()
  val featuresCol = FeaturesColComponentAttribute()

  attrPack.add(tempSharedPath, elasticNetParam, family, fitIntercept, maxIter, predictionCol, probabilityCol, rawPredictionCol, regParam, standardization, thresholds, tolerance, weightCol, labelCol, featuresCol)

  override def getLabelColumnName: Option[String] = Some(labelCol.value)

  override def configure(paramMap: Map[String, Any]): Unit = {
    super.configure(paramMap)

    if (paramMap.contains(tempSharedPath.key)) {
      this.tempSharedPathStr = tempSharedPath.value
    }
    if (paramMap.contains(elasticNetParam.key)) {
      lr.setElasticNetParam(elasticNetParam.value)
    }
    if (paramMap.contains(family.key)) {
      lr.setFamily(family.value)
    }
    if (paramMap.contains(fitIntercept.key)) {
      lr.setFitIntercept(fitIntercept.value)
    }
    if (paramMap.contains(maxIter.key)) {
      lr.setMaxIter(maxIter.value)
    }
    if (paramMap.contains(predictionCol.key)) {
      lr.setPredictionCol(predictionCol.value)
    }
    if (paramMap.contains(probabilityCol.key)) {
      lr.setProbabilityCol(probabilityCol.value)
    }
    if (paramMap.contains(rawPredictionCol.key)) {
      lr.setRawPredictionCol(rawPredictionCol.value)
    }
    if (paramMap.contains(regParam.key)) {
      lr.setRegParam(regParam.value)
    }
    if (paramMap.contains(standardization.key)) {
      lr.setStandardization(standardization.value)
    }
    if (paramMap.contains(tolerance.key)) {
      lr.setTol(tolerance.value)
    }
    if (paramMap.contains(thresholds.key)) {
      val thresholdsArray = thresholds.value.asInstanceOf[List[String]]
        .map(_.toString.trim().toDouble).toArray
      if (thresholdsArray.size > 0) {
        lr.setThresholds(thresholdsArray)
      }
    }
    if (paramMap.contains(weightCol.key)) {
      lr.setWeightCol(weightCol.value)
    }

    lr.setLabelCol(labelCol.value).setFeaturesCol(featuresCol.value)
  }

  override def getAlgoStage(): PipelineStage = {
    this.featuresColName = featuresCol.value
    lr
  }
}