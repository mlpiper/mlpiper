package com.parallelmachines.reflex.common

import org.apache.spark.ml.PipelineModel

class SparkSaveModel {
  var sparkMLModel: PipelineModel = _

  var tempSharedPathStr: String = _

  def setSparkMLModel(sparkMLModel: PipelineModel): Unit = {
    this.sparkMLModel = sparkMLModel
  }

  def setTempSharedPathStr(tempSharedPathStr1: String): Unit = {
    this.tempSharedPathStr = tempSharedPathStr1
  }
}
