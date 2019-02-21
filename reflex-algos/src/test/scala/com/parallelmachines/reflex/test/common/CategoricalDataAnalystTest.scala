package com.parallelmachines.reflex.test.common

import com.parallelmachines.reflex.common.dataanalysis
import com.parallelmachines.reflex.common.dataanalysis.{CategoricalDataAnalysisResult, CategoricalDataAnalyst}
import org.apache.flink.streaming.scala.examples.clustering.math.{ReflexColumnEntry, ReflexNamedVector}
import org.apache.spark.sql.SparkSession
import com.parallelmachines.reflex.common.enums.OpType
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class CategoricalDataAnalystTest extends FlatSpec with Matchers {
  /**
    * Generate Correct Categorical Data Analysis
    */
  it should "Generate Correct Categorical Histogram" in {
    val sparkSession = SparkSession.builder.
      master("local[*]").appName("Good Categorical DA").getOrCreate()

    // NamedVectors
    val seqOfNamedVectorForCont: Seq[ReflexNamedVector] = Seq[ReflexNamedVector](
      ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = "XYZ", OpType.CATEGORICAL),
        ReflexColumnEntry(columnName = "B", columnValue = "ML", OpType.CATEGORICAL))),

      ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = "XYZ", OpType.CATEGORICAL),
        ReflexColumnEntry(columnName = "B", columnValue = "ParallelM", OpType.CATEGORICAL))),

      ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = "MNO", OpType.CATEGORICAL),
        ReflexColumnEntry(columnName = "B", columnValue = "ML", OpType.CATEGORICAL))),

      ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = "KLMNOP", OpType.CATEGORICAL),
        ReflexColumnEntry(columnName = "B", columnValue = "ParallelM", OpType.CATEGORICAL))),

      ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = "Z", OpType.CATEGORICAL),
        ReflexColumnEntry(columnName = "B", columnValue = "Artificial Intelligence", OpType.CATEGORICAL))),

      ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = "Z", OpType.CATEGORICAL),
        ReflexColumnEntry(columnName = "B", columnValue = "Science And Progress", OpType.CATEGORICAL))),

      ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = "Z", OpType.CATEGORICAL),
        ReflexColumnEntry(columnName = "B", columnValue = None, OpType.CATEGORICAL))),

      ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = "CDE", OpType.CATEGORICAL),
        ReflexColumnEntry(columnName = "B", columnValue = "ParallelM", OpType.CATEGORICAL)))
    )
    val rddOfNamedVector = sparkSession.sparkContext.parallelize(seqOfNamedVectorForCont)

    val featureAndDAResult = CategoricalDataAnalyst.analyze(rddOfNamedVector, sc = sparkSession.sparkContext)

    featureAndDAResult.size == 2 should be(true)

    val categoricalDataAnalysisResultForA = dataanalysis.CategoricalDataAnalysisResult(featureName = "A",
      count = 8,
      NAs = "0.0%",
      uniques = 5.0,
      topFreqOccuring = 3.0,
      topFreqOccuringCat = "Z",
      // 21/8
      averageStringLength = 2.625
    )

    featureAndDAResult("A").equals(categoricalDataAnalysisResultForA) should be(true)

    val categoricalDataAnalysisResultForB = CategoricalDataAnalysisResult(featureName = "B",
      count = 8,
      NAs = "12.5%",
      uniques = 4.0,
      topFreqOccuring = 3.0,
      topFreqOccuringCat = "ParallelM",
      averageStringLength = 10.5714
    )

    featureAndDAResult("B").equals(categoricalDataAnalysisResultForB) should be(true)

    val _ = CategoricalDataAnalyst.updateSparkAccumulatorAndGetResultWrapper(featureAndDAResult, sparkContext = sparkSession.sparkContext)
  }
}
