package org.apache.flink.streaming.test.exampleScalaPrograms.clustering

import org.apache.flink.streaming.scala.examples.clustering.math.{ReflexColumnEntry, ReflexNamedVector}
import org.apache.flink.streaming.scala.examples.clustering.stat.heatmap.localgenerator.MeanHeatMapHelper
import com.parallelmachines.reflex.common.enums.OpType
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class MeanHeatMapHelperTest extends FlatSpec with Matchers {
  val testSeqOfNamedVectorForHeatMap: Seq[ReflexNamedVector] = Seq[ReflexNamedVector](
    ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = 1.0, OpType.CONTINUOUS),
      ReflexColumnEntry(columnName = "B", columnValue = 10.0, OpType.CONTINUOUS))),

    ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = 2.0, OpType.CONTINUOUS),
      ReflexColumnEntry(columnName = "B", columnValue = 100.0, OpType.CONTINUOUS))),

    ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = 0.0, OpType.CONTINUOUS),
      ReflexColumnEntry(columnName = "B", columnValue = 100.0, OpType.CONTINUOUS))),

    ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = 4.0, OpType.CONTINUOUS),
      ReflexColumnEntry(columnName = "B", columnValue = -10.0, OpType.CONTINUOUS))),

    ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = 5.0, OpType.CONTINUOUS),
      ReflexColumnEntry(columnName = "B", columnValue = 50.0, OpType.CONTINUOUS)))
  )

  it should "Create Right HeatMap Object From Iterators Of Named Vectors" in {
    val iteratorOfNV = testSeqOfNamedVectorForHeatMap

    val heatMapValues = MeanHeatMapHelper.generateHeatMap(value = iteratorOfNV).heatMapValue

    val expectedHeatMapValuesSeq: Map[String, Double] = Map("A" -> 2.40, "B" -> 50.0)

    expectedHeatMapValuesSeq.foreach(eachTuple => {
      eachTuple._2 should be(heatMapValues(eachTuple._1) +- 1e-2)
    })
  }

  val testSeqOfVariableLengthNamedVectorForHeatMap: Seq[ReflexNamedVector] = Seq[ReflexNamedVector](
    ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = 1.0, OpType.CONTINUOUS))),

    ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "B", columnValue = 10.0, OpType.CONTINUOUS))),

    ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = 2.0, OpType.CONTINUOUS),
      ReflexColumnEntry(columnName = "B", columnValue = 100.0, OpType.CONTINUOUS))),

    ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "B", columnValue = 100.0, OpType.CONTINUOUS))),

    ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = 0.0, OpType.CONTINUOUS))),

    ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = 4.0, OpType.CONTINUOUS),
      ReflexColumnEntry(columnName = "B", columnValue = -10.0, OpType.CONTINUOUS))),

    ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = 5.0, OpType.CONTINUOUS),
      ReflexColumnEntry(columnName = "B", columnValue = 50.0, OpType.CONTINUOUS)))
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
