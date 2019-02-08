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
  * Compute HeatMap by using "local-by-mean" methodology on mini-batch of [[NamedVector]]s
  *
  * Class is responsible for Performing a [[RichFlatMapFunction]] on [[NamedVector]]s and outputs [[HeatMapValues]]s.
  * Primary task of the class is to provide flatMap function on datastream of iterable.
  * flatMap will find average of it for each features.
  */
class MeanHeatMap extends RichFlatMapFunction[Iterable[NamedVector], HeatMapValues] {
  override def flatMap(value: Iterable[NamedVector], out: Collector[HeatMapValues]): Unit = {
    val heatMapValues = MeanHeatMapHelper.generateHeatMap(value = value)

    out.collect(heatMapValues)
  }
}

object MeanHeatMapHelper extends HeatMapHelper {
  /** Method is API to generate HeatMapValues from Iterable which can be used by any engine - spark, flink */
  override def generateHeatMap(value: Iterable[NamedVector]): HeatMapValues = {
    val namedMatrix = GenericNamedMatrixUtils.iteratorOfNamedVectorToNamedMatrix(iteratorOfNamedVector = value.toIterator)

    val namedVectors = namedMatrix.arrayOfVector

    val heatMapValueMap: mutable.Map[String, Double] = mutable.Map[String, Double]()

    namedVectors.foreach(eachNamedVector => {
      if (eachNamedVector.columnValue(0).isInstanceOf[Double]) {
        val denseVector: Array[Double] = eachNamedVector.columnValue.toArray.map(_.asInstanceOf[Double])
        val meanVar = meanAndVariance(DenseVector(denseVector))
        val mean = meanVar.mean

        heatMapValueMap.put(eachNamedVector.columnName, mean)
      }
    })

    val heatMapValues = HeatMapValues(heatMapValue = heatMapValueMap.toMap)

    heatMapValues
  }
}
