package org.apache.flink.streaming.scala.examples.clustering.stat.heatmap.globalgenerator

import breeze.linalg.DenseVector
import breeze.stats.meanAndVariance
import org.apache.flink.streaming.api.scala.function.RichAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.scala.examples.clustering.stat.heatmap.{GlobalParams, HeatMapValues}
import org.apache.flink.streaming.scala.examples.clustering.stat.utils.IDElement
import org.apache.flink.util.Collector

/**
  * Class [[StandardScaleParamsGenerator]] will create parameters - mean and standard deviation - that can be used in following functions
  *
  * globalParameters' param1 will be mean and param2 will be standard deviation.
  */
class StandardScaleParamsGenerator
  extends RichAllWindowFunction[IDElement[HeatMapValues],
    IDElement[GlobalParams],
    GlobalWindow] {

  /** Method gives access to mean and std from Array. It will return tuple of Mean and Standard Deviation */
  def generateMeanAndStdFromArray(arrayOfDouble: Array[Double]): (Double, Double) = {
    val count = arrayOfDouble.length
    val mean = arrayOfDouble.sum / count

    var sumOfVar = 0.0
    arrayOfDouble.foreach(x => sumOfVar = sumOfVar + ((x - mean) * (x - mean)))

    val std = Math.sqrt(sumOfVar / (count - 1))

    (mean, std)
  }

  override def apply(window: GlobalWindow,
                     input: Iterable[IDElement[HeatMapValues]],
                     out: Collector[IDElement[GlobalParams]]): Unit = {
    val listOfVectors = input.iterator.toArray.clone()

    val listOfHMValues = listOfVectors.map(_.element.heatMapValue)
    val nameOfCols = listOfHMValues.head.keys

    val denseVectorsOfEachFeature =
      nameOfCols
        .map(eachCol => (eachCol,
          meanAndVariance(DenseVector(listOfHMValues.map(x => x(eachCol))))))

    val meanScalar = denseVectorsOfEachFeature.map(x => (x._1, x._2.mean))
    val stdScalar = denseVectorsOfEachFeature.map(x => (x._1, x._2.stdDev))

    val maxID = listOfVectors.map(_.elementID).reduce((x, y) => if (x > y) x else y)

    val globalParams = GlobalParams(params1 = meanScalar.toMap, params2 = stdScalar.toMap)

    val idedGlobalParams = IDElement(element = globalParams, elementID = maxID)

    out.collect(idedGlobalParams)
  }
}
