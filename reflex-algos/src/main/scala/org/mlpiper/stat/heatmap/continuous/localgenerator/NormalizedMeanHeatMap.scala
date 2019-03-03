package org.mlpiper.stat.heatmap.continuous.localgenerator

import breeze.linalg.DenseVector
import breeze.stats.meanAndVariance
import org.mlpiper.datastructures.NamedVector
import org.mlpiper.stat.heatmap.continuous.HeatMapValues
import org.mlpiper.utils.GenericNamedMatrixUtils

import scala.collection.mutable

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
