package com.parallelmachines.reflex.components.spark.batch.algorithms

import com.parallelmachines.reflex.components.spark.batch.algorithms.MlMethod.MlMethodType
import com.parallelmachines.reflex.components.{ComponentAttribute, FeaturesColComponentAttribute, LabelColComponentAttribute, PredictionColComponentAttribute}
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.regression.LinearRegression

class ReflexLinearRegression extends ReflexSparkMLAlgoBase {
  override val label: String = "Linear Regression"
  override val description: String = "Linear Regression Training"
  override val version: String = "1.0.0"

  val lr = new LinearRegression()

  val tempSharedPath = ComponentAttribute("tempSharedPath", "",
    "temp Shared Path", "Temporary shared path for model transfer, " +
      "paths with prefix file:// or hdfs://", optional = true)
  val elasticNetParam = ComponentAttribute("elasticNetParam", 0.0, "ElasticNet parameter",
    "the ElasticNet mixing parameter, in range [0.0, 1.0]. For alpha = 0.0, the penalty is a L2 penalty." +
      " For alpha = 1.0, it is an L1 penalty (Default: 0.0)", optional = true)
    .setValidator(x => (x >= 0.0) & (x <= 1.0))
  val aggregationDepth = ComponentAttribute("aggregationDepth", 2, "Aggregation Depth",
    "suggested depth for treeAggregate (>= 2). (Default: 2)", optional = true).setValidator(x => x >= 2)
  val standardization = ComponentAttribute("standardization", true, "Standardization", "whether to " +
    "standardize the training features before fitting the model. (Default: true)", optional = true)
  val fitIntercept = ComponentAttribute("fitIntercept", true, "Fit Intercept", "Whether to fit an" +
    " intercept term. (Default: true)", optional = true)
  val maxIter = ComponentAttribute("maxIter", 100, "Maximum Iterations", "Number of iterations of gradient" +
    " descent to run. (Default: 100). (>= 0)", optional = true).setValidator(x => x >= 0)
  val regParam = ComponentAttribute("regParam", 0.0, "Regularization Parameter",
    "Regularization parameter. (>= 0.0) (Default: 0.0)", optional = true).setValidator(x => x >= 0.0)

  val tolerance = ComponentAttribute("tolerance", 1E-6, "Convergence Tolerance",
    "Set the convergence tolerance of iterations. Smaller value will lead to higher accuracy with" +
      " the cost of more iterations. (>= 0.0) (Default: 1E-6)", optional = true).setValidator(x => x >= 0.0)
  val solver = ComponentAttribute("solver", "auto", "Solver", "the solver algorithm for optimization." +
    " If this is not set or empty, (Default: 'auto')", optional = true)
  solver.setOptions(List[(String, String)](("auto", "auto"), ("l-bfgs", "l-bfgs"), ("normal", "normal")))

  val weightCol = ComponentAttribute("weightCol", "", "Weight Column Name", "Sets the name for " +
    "weightCol. If this is not set or empty, all instance weights are treated as 1.0.", optional = true)

  val labelCol = LabelColComponentAttribute()
  val featuresCol = FeaturesColComponentAttribute()
  val predictionCol = PredictionColComponentAttribute()


  attrPack.add(tempSharedPath, elasticNetParam, fitIntercept, maxIter, predictionCol, regParam, tolerance,
    aggregationDepth, standardization, solver, weightCol, labelCol, featuresCol)

  override val mlType: MlMethodType = MlMethod.Regression

  override def getLabelColumnName: Option[String] = Some(labelCol.value)

  override def configure(paramMap: Map[String, Any]): Unit = {
    super.configure(paramMap)


    if (paramMap.contains(tempSharedPath.key)) {
      this.tempSharedPathStr = tempSharedPath.value
    }
    if (paramMap.contains(elasticNetParam.key)) {
      lr.setElasticNetParam(elasticNetParam.value)
    }
    if (paramMap.contains(aggregationDepth.key)) {
      lr.setAggregationDepth(aggregationDepth.value)
    }

    if (paramMap.contains(solver.key)) {
      lr.setSolver(solver.value)
    }

    if (paramMap.contains(standardization.key)) {
      lr.setStandardization(standardization.value)
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

    if (paramMap.contains(regParam.key)) {
      lr.setRegParam(regParam.value)
    }
    if (paramMap.contains(tolerance.key)) {
      lr.setTol(tolerance.value)
    }

    if (paramMap.contains(weightCol.key)) {
      lr.setWeightCol(weightCol.value)
    }
    if (paramMap.contains(labelCol.key)) {
      lr.setLabelCol(labelCol.value)
    }
    if (paramMap.contains(featuresCol.key)) {
      lr.setFeaturesCol(featuresCol.value)
    }

  }

  override def getAlgoStage(): PipelineStage = {
    this.featuresColName = featuresCol.value

    lr
  }
}