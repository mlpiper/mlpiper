package com.parallelmachines.reflex.common.mlobject

import com.parallelmachines.reflex.common.enums.ModelFormat
import org.apache.spark.ml.PipelineModel

/**
  * Class for handling SparkML model objects
  * This class is required to be able to save model to file.
  */
class SparkMLModel(name: String, description: String = "", id: Option[String] = None) extends Model(name, ModelFormat.SPARKML, description, id) {

  var sparkMLModel: PipelineModel = _
  var tempSharedPathStr: String = _

  def setSparkMLModel(sparkMLModel: PipelineModel): Unit = {
    this.sparkMLModel = sparkMLModel
  }

  def setTempSharedPathStr(tempSharedPathStr: String): Unit = {
    this.tempSharedPathStr = tempSharedPathStr
  }
}
