package com.parallelmachines.reflex.components.spark.batch.algorithms

import com.parallelmachines.reflex.components.spark.batch.algorithms.MlMethod.MlMethodType
import com.parallelmachines.reflex.components.{ComponentAttribute, FeaturesColComponentAttribute, LabelColComponentAttribute, PredictionColComponentAttribute}
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.regression.GeneralizedLinearRegression

class ReflexGLM extends ReflexSparkMLAlgoBase {
  override val label: String = "GLM Training"
  override lazy val defaultModelName: String = "Generalized Linear Regression"
  override val description: String = "Generalized Linear Regression Training"
  override val version: String = "1.0.0"

  val glm = new GeneralizedLinearRegression()

  val tempSharedPath = ComponentAttribute("tempSharedPath", "",
    "temp Shared Path", "Temporary shared path for model transfer, " +
      "paths with prefix file:// or hdfs://", optional = true)
  val family = ComponentAttribute("family", "gaussian", "Family", "family name. Supported options:" +
    " 'gaussian', 'binomial', 'poisson' and 'gamma'. (Default: 'gaussian')", optional = true)
  family.setOptions(List[(String, String)](("gaussian", "gaussian"), ("binomial", "binomial"),
    ("poisson", "poisson"), ("gamma", "gamma")))
  val link = ComponentAttribute("link", "identity", "Link", "Link Function. Supported options:" +
    "'identity', 'log', 'inverse', 'logit', 'probit', 'cloglog' and 'sqrt'. (Default: 'identity')")
  link.setOptions(List[(String, String)](("identity", "identity"), ("log", "log"),
    ("inverse", "inverse"), ("logit", "logit"), ("probit", "probit"),
    ("cloglog", "cloglog"), ("sqrt", "sqrt")))
  val fitIntercept = ComponentAttribute("fitIntercept", true, "Fit Intercept", "Whether to fit an" +
    " intercept term. (Default: true)", optional = true)
  val maxIter = ComponentAttribute("maxIter", 25, "Maximum Itereration", "Maximum Number of " +
    "iterations to run. (applicable for solver 'irls').(>= 0). (Default: 25)",
    optional = true).setValidator(x => x >= 0)
  val predictionCol = PredictionColComponentAttribute()
  val linkPredictionCol = ComponentAttribute("linkPredictionCol", "", "linkPrediction Column Name",
    "Linear Prediction column name. Default is not set, which means no output link prediction",
    optional = true)
  val regParam = ComponentAttribute("regParam", 0.0, "Regularization parameter",
    "Regularization parameter.(>= 0.0) (Default: 0.0)", optional = true).setValidator(x => x >= 0.0)
  val tolerance = ComponentAttribute("tolerance", 1E-6, "Convergence Tolerance", "Set the convergence" +
    " tolerance of iterations. Smaller value will lead to higher accuracy with the cost of more" +
    " iterations.(>= 0.0) (Default: 1E-6)", optional = true).setValidator(x => x >= 0.0)
  val weightCol = ComponentAttribute("weightCol", "", "Weight Column Name", "Sets the name for " +
    "weightCol. If this is not set or empty, all instance weights are treated as 1.0." +
    " In the Binomial family, weights correspond to number of trials and should be integer." +
    " Non-integer weights are rounded to integer in AIC calculation.",
    optional = true)

  val labelCol = LabelColComponentAttribute()
  val featuresCol = FeaturesColComponentAttribute()

  attrPack.add(tempSharedPath, family, fitIntercept, maxIter, link, predictionCol, linkPredictionCol, regParam,
    tolerance, weightCol, labelCol, featuresCol)

  override val mlType: MlMethodType = MlMethod.Regression

  override def getLabelColumnName: Option[String] = Some(labelCol.value)

  override def configure(paramMap: Map[String, Any]): Unit = {
    super.configure(paramMap)

    if (paramMap.contains(family.key)) {
      glm.setFamily(family.value)
    }

    glm.setLink(link.value)

    if (paramMap.contains(tempSharedPath.key)) {
      this.tempSharedPathStr = tempSharedPath.value
    }
    if (paramMap.contains(fitIntercept.key)) {
      glm.setFitIntercept(fitIntercept.value)
    }
    if (paramMap.contains(maxIter.key)) {
      glm.setMaxIter(maxIter.value)
    }
    if (paramMap.contains(predictionCol.key)) {
      glm.setPredictionCol(predictionCol.value)
    }
    if (paramMap.contains(linkPredictionCol.key)) {
      glm.setLinkPredictionCol(linkPredictionCol.value)
    }
    if (paramMap.contains(regParam.key)) {
      glm.setRegParam(regParam.value)
    }
    if (paramMap.contains(tolerance.key)) {
      glm.setTol(tolerance.value)
    }
    if (paramMap.contains(weightCol.key)) {
      glm.setWeightCol(weightCol.value)
    }
    if (paramMap.contains(labelCol.key)) {
      glm.setLabelCol(labelCol.value)
    }
    if (paramMap.contains(featuresCol.key)) {
      glm.setFeaturesCol(featuresCol.value)
    }
    //TODO: Sprk 2.2.0 includes additional parameters to the algorithm. tweedie mode, variancePower, linkPower
  }

  override def getAlgoStage(): PipelineStage = {
    this.featuresColName = featuresCol.value

    glm
  }
}