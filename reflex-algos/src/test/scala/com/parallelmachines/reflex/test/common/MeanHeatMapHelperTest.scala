package com.parallelmachines.reflex.test.common

import com.parallelmachines.reflex.common.enums.OpType
import org.junit.runner.RunWith
import org.mlpiper.datastructures.{ColumnEntry, NamedVector}
import org.mlpiper.stat.heatmap.continuous.localgenerator.MeanHeatMapHelper
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class MeanHeatMapHelperTest extends FlatSpec with Matchers {
  val testSeqOfNamedVectorForHeatMap: Seq[NamedVector] = Seq[NamedVector](
    NamedVector(Array[ColumnEntry](ColumnEntry(columnName = "A", columnValue = 1.0, OpType.CONTINUOUS),
      ColumnEntry(columnName = "B", columnValue = 10.0, OpType.CONTINUOUS))),

    NamedVector(Array[ColumnEntry](ColumnEntry(columnName = "A", columnValue = 2.0, OpType.CONTINUOUS),
      ColumnEntry(columnName = "B", columnValue = 100.0, OpType.CONTINUOUS))),

    NamedVector(Array[ColumnEntry](ColumnEntry(columnName = "A", columnValue = 0.0, OpType.CONTINUOUS),
      ColumnEntry(columnName = "B", columnValue = 100.0, OpType.CONTINUOUS))),

    NamedVector(Array[ColumnEntry](ColumnEntry(columnName = "A", columnValue = 4.0, OpType.CONTINUOUS),
      ColumnEntry(columnName = "B", columnValue = -10.0, OpType.CONTINUOUS))),

    NamedVector(Array[ColumnEntry](ColumnEntry(columnName = "A", columnValue = 5.0, OpType.CONTINUOUS),
      ColumnEntry(columnName = "B", columnValue = 50.0, OpType.CONTINUOUS)))
  )

  it should "Create Right HeatMap Object From Iterators Of Named Vectors" in {
    val iteratorOfNV = testSeqOfNamedVectorForHeatMap

    val heatMapValues = MeanHeatMapHelper.generateHeatMap(value = iteratorOfNV).heatMapValue

    val expectedHeatMapValuesSeq: Map[String, Double] = Map("A" -> 2.40, "B" -> 50.0)

    expectedHeatMapValuesSeq.foreach(eachTuple => {
      eachTuple._2 should be(heatMapValues(eachTuple._1) +- 1e-2)
    })
  }

  val testSeqOfVariableLengthNamedVectorForHeatMap: Seq[NamedVector] = Seq[NamedVector](
    NamedVector(Array[ColumnEntry](ColumnEntry(columnName = "A", columnValue = 1.0, OpType.CONTINUOUS))),

    NamedVector(Array[ColumnEntry](ColumnEntry(columnName = "B", columnValue = 10.0, OpType.CONTINUOUS))),

    NamedVector(Array[ColumnEntry](ColumnEntry(columnName = "A", columnValue = 2.0, OpType.CONTINUOUS),
      ColumnEntry(columnName = "B", columnValue = 100.0, OpType.CONTINUOUS))),

    NamedVector(Array[ColumnEntry](ColumnEntry(columnName = "B", columnValue = 100.0, OpType.CONTINUOUS))),

    NamedVector(Array[ColumnEntry](ColumnEntry(columnName = "A", columnValue = 0.0, OpType.CONTINUOUS))),

    NamedVector(Array[ColumnEntry](ColumnEntry(columnName = "A", columnValue = 4.0, OpType.CONTINUOUS),
      ColumnEntry(columnName = "B", columnValue = -10.0, OpType.CONTINUOUS))),

    NamedVector(Array[ColumnEntry](ColumnEntry(columnName = "A", columnValue = 5.0, OpType.CONTINUOUS),
      ColumnEntry(columnName = "B", columnValue = 50.0, OpType.CONTINUOUS)))
  )
  it should "Create Right HeatMap Object From Iterators Of Variable Length Named Vectors " in {
    val iteratorOfNV = testSeqOfVariableLengthNamedVectorForHeatMap

    val heatMapValues = MeanHeatMapHelper.generateHeatMap(value = iteratorOfNV).heatMapValue

    val expectedHeatMapValuesSeq: Map[String, Double] = Map("A" -> 2.40, "B" -> 50.0)

    expectedHeatMapValuesSeq.foreach(eachTuple => {
      eachTuple._2 should be(heatMapValues(eachTuple._1) +- 1e-2)
    })
  }
}
