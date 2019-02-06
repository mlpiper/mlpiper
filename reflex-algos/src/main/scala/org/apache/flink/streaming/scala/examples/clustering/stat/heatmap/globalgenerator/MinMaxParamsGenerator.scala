package org.apache.flink.streaming.scala.examples.clustering.stat.heatmap.globalgenerator

import org.apache.flink.streaming.api.scala.function.RichAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.scala.examples.clustering.stat.heatmap.{GlobalParams, HeatMapValues}
import org.apache.flink.streaming.scala.examples.clustering.stat.utils.IDElement
import org.apache.flink.util.Collector

class MinMaxParamsGenerator
  extends RichAllWindowFunction[IDElement[HeatMapValues], IDElement[GlobalParams], GlobalWindow] {
  override def apply(window: GlobalWindow,
                     input: Iterable[IDElement[HeatMapValues]],
                     out: Collector[IDElement[GlobalParams]]): Unit = {
    val listOfVectors = input.iterator.toArray

    val listOfHMValues = listOfVectors.map(_.element.heatMapValue)
    val nameOfCols = listOfHMValues.head.keys

    val minScalar = nameOfCols.map(eachCol => (eachCol, listOfHMValues.map(x => x(eachCol)).min)).toMap
    val maxScalar = nameOfCols.map(eachCol => (eachCol, listOfHMValues.map(x => x(eachCol)).max)).toMap

    val maxID = listOfVectors.map(_.elementID).reduce((x, y) => if (x > y) x else y)

    val globalParams = GlobalParams(params1 = minScalar, params2 = maxScalar)

    val idedGlobalParams = IDElement(element = globalParams, elementID = maxID)

    out.collect(idedGlobalParams)
  }
}
