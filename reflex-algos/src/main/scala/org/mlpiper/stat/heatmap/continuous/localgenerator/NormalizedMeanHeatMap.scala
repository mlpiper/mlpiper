package org.mlpiper.stat.heatmap.continuous.localgenerator

import breeze.linalg.DenseVector
import breeze.stats.meanAndVariance
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector
import org.mlpiper.datastructures.NamedVector
import org.mlpiper.stat.heatmap.continuous.HeatMapValues
import org.mlpiper.utils.GenericNamedMatrixUtils

import scala.collection.mutable

/**
  * Compute HeatMap by using "local-by-norm-mean" methodology on mini-batch of NamedVectors
  *
  * Class is responsible for Performing a [[RichFlatMapFunction]] on [[NamedVector]]s and outputs [[org.mlpiper.stat.heatmap.continuous.HeatMapValues]]s.
  * Primary task of the class is to provide flatMap function on datastream of iterable.
  * flatMap will scale the values of each feature in range of [0,1] by using Min-Max-Scaling functionality and then find average of it.
  */
class NormalizedMeanHeatMap
  extends RichFlatMapFunction[Iterable[NamedVector], HeatMapValues] {

  /**
    * Method will scale the values of each feature in range of [0,1] by using Min-Max-Scaling functionality and then find average of it.
    */
  override def flatMap(value: Iterable[NamedVector],
                       out: Collector[HeatMapValues]): Unit = {
    val heatMapValues = NormalizedMeanHeatMapHelper.generateHeatMap(value)
    out.collect(heatMapValues)
  }
}

object NormalizedMeanHeatMapHelper extends HeatMapHelper {
  /** Method is API to generate HeatMapValues from Iterable which can be used by any engine - spark, flink */
  override def generateHeatMap(value: Iterable[NamedVector]): HeatMapValues = {
    val namedMatrix = GenericNamedMatrixUtils.iteratorOfNamedVectorToNamedMatrix(iteratorOfNamedVector = value.toIterator)

    val heatMapValueMap: mutable.Map[String, Double] = mutable.Map[String, Double]()

    val namedVectors = namedMatrix.arrayOfVector

    namedVectors.foreach(eachNamedVector => {
      if (eachNamedVector.columnValue(0).isInstanceOf[Double]) {
        val denseVector: Array[Double] =
          eachNamedVector
            .columnValue
            .toArray
            .map(_.asInstanceOf[Double])
            .filter(!_.isNaN)

        // calculating heatmap if vector exists else heatmap will not be calculated
        if (denseVector.nonEmpty) {
          // creating min max normalized values for dense vector
          val minMaxedDenseVector = minMaxNormalization(denseVector = DenseVector(denseVector))

          // getting mean from minMaxedVectors
          val meanVar = meanAndVariance(minMaxedDenseVector)
          val mean = meanVar.mean

          heatMapValueMap.put(eachNamedVector.columnName, mean)
        }
      }
    })

    val heatMapValues = HeatMapValues(heatMapValue = heatMapValueMap.toMap)

    heatMapValues
  }

  /** Method is responsible for performing normalization over dense vectors.
    * There are two cases.
    * 1. Non constant values - normalization is as usual.
    * 2. Constant values -
    * For constant values again, we need to handle 3 below cases.
    *
    * 2.a. lets say min = max = 3. Answer should be 1
    * so min = 0 and max will remain 3.
    * so x - min/max -min = 1
    * 2.b lets say min = max = -2. Again, answer should be 1
    * so min = 0 and max will remain -2.
    * so x - min/max -min = 1
    * 2.c lets say min = max = 0. Again, answer should be 0
    * so min = 0 and max = 1.
    * so x - min/max -min = 0
    */
  private def minMaxNormalization(denseVector: DenseVector[Double]): DenseVector[Double] = {
    val minMaxArray = denseVector.toArray

    var minVal = minMaxArray.min
    var maxVal = minMaxArray.max

    // for constant values,
    // min value has to be 0
    // if max is also 0, then it needs to be changed to 1.0 to prevent 0/0
    if (minVal == maxVal) {
      if (maxVal == 0.0) {
        maxVal = 1.0
      }

      minVal = 0.0
    }

    minMaxArray.zipWithIndex.foreach(eachZippedElement => {
      val elementValue = eachZippedElement._1
      minMaxArray(eachZippedElement._2) = (elementValue - minVal) / (maxVal - minVal)
    })

    DenseVector(minMaxArray)
  }
}
