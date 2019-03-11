package com.parallelmachines.reflex.test.reflexpipeline

import com.parallelmachines.reflex.components.spark.batch.SparkBatchPipelineInfo
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.mlpiper.utils.DataFrameUtils
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class SparkBatchPipelineInfoTest extends FlatSpec with Matchers {
  "Successfully Add, Remove And Setting Element Functionality" should "Valid E2E" in {
    val header = false
    val sparkSession = SparkSession.builder.
      master("local[*]").appName("Good SBPI Test").getOrCreate()

    val filePath = getClass.getResource("/SBPITest.csv").getPath

    val dfReader = sparkSession.sqlContext.read
      .option("inferSchema", value = true)
      .option("header", header)

    if (sparkSession.version.contains("2.2.0")) {
        dfReader.option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
    }

    var df = dfReader.csv(filePath)

    if (!header) {
      df = DataFrameUtils.renameColumns(df)
    }

    val sparkBatchPipelineInfo = new SparkBatchPipelineInfo(dataframe = df)

    var currentDFCols = sparkBatchPipelineInfo.getCurrentDataframeColumns
    var expectedDFCols: Array[String] = Array("c0", "c1", "c2", "c3", "c4", "c5")

    currentDFCols.deep == expectedDFCols.deep should be(true)

    // adding "c4", "c7", "c6", "c1", "c7"- since, c4 and c1, they should not be added & c7 should be added once only!
    sparkBatchPipelineInfo.updateCurrentDataFrameCols(
      setSpecificCols = None,
      addCols = Some(Array("c4", "c7", "c6", "c1")),
      removeCols = None
    )
    currentDFCols = sparkBatchPipelineInfo.getCurrentDataframeColumns
    expectedDFCols = Array("c0", "c1", "c2", "c3", "c4", "c5", "c7", "c6")

    currentDFCols.deep == expectedDFCols.deep should be(true)

    // removing "c7", "c1", "c5", "c10", "c7" - should not throw any error and return as expected cols
    sparkBatchPipelineInfo.updateCurrentDataFrameCols(
      setSpecificCols = None,
      addCols = None,
      removeCols = Some(Array("c7", "c1", "c5", "c10"))
    )
    currentDFCols = sparkBatchPipelineInfo.getCurrentDataframeColumns
    expectedDFCols = Array("c0", "c2", "c3", "c4", "c6")

    currentDFCols.deep == expectedDFCols.deep should be(true)

    // select "c2", "c4", "c0", "c10" - since, c10 is removed it should not be return as current DF col list
    sparkBatchPipelineInfo.updateCurrentDataFrameCols(
      setSpecificCols = Some(Array("c2", "c4", "c0", "c10")),
      addCols = None,
      removeCols = None
    )
    currentDFCols = sparkBatchPipelineInfo.getCurrentDataframeColumns
    expectedDFCols = Array("c2", "c4", "c0")

    currentDFCols.deep == expectedDFCols.deep should be(true)

    // checking operation order - set, add and remove
    // select "c2", "c4", "c0", "c10" - Cols = c2, c4, c0
    // add "c10", "c9" - Cols = c2, c4, c0, c10, c9
    // remove "c8", "c9", "c0" - Cols = c2, c4, c10
    sparkBatchPipelineInfo.updateCurrentDataFrameCols(
      setSpecificCols = Some(Array("c2", "c4", "c0", "c10")),
      addCols = Some(Array("c10", "c9")),
      removeCols = Some(Array("c8", "c9", "c0"))
    )
    currentDFCols = sparkBatchPipelineInfo.getCurrentDataframeColumns
    expectedDFCols = Array("c2", "c4", "c10")

    currentDFCols.deep == expectedDFCols.deep should be(true)
    sparkSession.stop()

  }
}
