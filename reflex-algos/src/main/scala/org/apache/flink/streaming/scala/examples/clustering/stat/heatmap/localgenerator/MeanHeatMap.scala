package org.apache.flink.streaming.scala.examples.clustering.stat.heatmap.localgenerator

import breeze.linalg.DenseVector
import breeze.stats.meanAndVariance
import com.parallelmachines.reflex.common.GenericNamedMatrixUtils
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.scala.examples.clustering.math.ReflexNamedVector
import org.apache.flink.streaming.scala.examples.clustering.stat.heatmap.HeatMapValues
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * Compute HeatMap by using "local-by-mean" methodology on mini-batch of ReflexNamedVectors
  *
  * Class is responsible for Performing a [[RichFlatMapFunction]] on [[ReflexNamedVector]]s and outputs [[HeatMapValues]]s.
  * Primary task of the class is to provide flatMap function on datastream of iterable.
  * flatMap will find average of it for each features.
  */
class MeanHeatMap extends RichFlatMapFunction[Iterable[ReflexNamedVector], HeatMapValues] {
  override def flatMap(value: Iterable[ReflexNamedVector], out: Collector[HeatMapValues]): Unit = {
    val heatMapValues = MeanHeatMapHelper.generateHeatMap(value = value)

    out.collect(heatMapValues)
  }
}

object MeanHeatMapHelper extends HeatMapHelper {
  /** Method is API to generate HeatMapValues from Iterable which can be used by any engine - spark, flink */
  override def generateHeatMap(value: Iterable[ReflexNamedVector]): HeatMapValues = {
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
