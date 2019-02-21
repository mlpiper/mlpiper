package com.parallelmachines.reflex.test.common

import com.parallelmachines.reflex.common.dataanalysis.{ContinuousDataAnalysisResult, ContinuousDataAnalyst}
import org.apache.flink.streaming.scala.examples.clustering.math.{ReflexColumnEntry, ReflexNamedVector}
import org.apache.spark.sql.SparkSession
import com.parallelmachines.reflex.common.enums.OpType
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class ContinuousDataAnalystTest extends FlatSpec with Matchers {
  /**
    * Generate Correct Continuous Data Analysis
    */
  it should "Generate Correct Continuous Histogram" in {
    val sparkSession = SparkSession.builder.
      master("local[*]").appName("Good Continuous DA").getOrCreate()

    // NamedVectors
    val seqOfNamedVectorForCont: Seq[ReflexNamedVector] = Seq[ReflexNamedVector](
      ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = 1.0, OpType.CONTINUOUS),
        ReflexColumnEntry(columnName = "B", columnValue = 10.0, OpType.CONTINUOUS))),

      ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = 2.0, OpType.CONTINUOUS),
        ReflexColumnEntry(columnName = "B", columnValue = 100.0, OpType.CONTINUOUS))),

      ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = 0.0, OpType.CONTINUOUS),
        ReflexColumnEntry(columnName = "B", columnValue = 100.0, OpType.CONTINUOUS))),

      ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = None, OpType.CONTINUOUS),
        ReflexColumnEntry(columnName = "B", columnValue = null, OpType.CONTINUOUS))),

      ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = 0.0, OpType.CONTINUOUS),
        ReflexColumnEntry(columnName = "B", columnValue = Double.NaN, OpType.CONTINUOUS)))
    )

    //+----+-----+
    //|   A|    B|
    //+----+-----+
    //| 1.0| 10.0|
    //| 2.0|100.0|
    //| 0.0|100.0|
    //|null| null|
    //| 0.0|  NaN|
    //+----+-----+
    val rddOfNamedVector = sparkSession.sparkContext.parallelize(seqOfNamedVectorForCont)

    val featureAndDAResult = ContinuousDataAnalyst.analyze(rddOfNamedVector)

    featureAndDAResult.size == 2 should be(true)

    val continuousDataAnalysisResultForA = ContinuousDataAnalysisResult(featureName = "A",
      count = 5,
      NAs = "20.0%",
      zeros = 2,
      min = 0.0,
      max = 2.0,
      mean = 0.75,
      median = 1.0,
      std = 0.9574
    )

    featureAndDAResult("A").equals(continuousDataAnalysisResultForA) should be(true)

    val continuousDataAnalysisResultForB = ContinuousDataAnalysisResult(featureName = "B",
      count = 5,
      NAs = "40.0%",
      zeros = 0,
      min = 10.0,
      max = 100.0,
      mean = 70,
      median = 100.0,
      std = 51.9615
    )

    featureAndDAResult("B").equals(continuousDataAnalysisResultForB) should be(true)

    val _ = ContinuousDataAnalyst.updateSparkAccumulatorAndGetResultWrapper(featureAndDAResult, sparkContext = sparkSession.sparkContext)
  }
}
