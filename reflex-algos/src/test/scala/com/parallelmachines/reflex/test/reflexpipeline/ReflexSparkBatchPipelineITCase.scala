package com.parallelmachines.reflex.test.reflexpipeline

import breeze.linalg.DenseVector
import com.google.protobuf.ByteString
import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent
import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent.EventType
import com.parallelmachines.reflex.pipeline.{DagGen, DataFrameUtils}
import org.apache.flink.streaming.test.exampleScalaPrograms.clustering.ComparatorUtils
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import org.junit.runner.RunWith
import org.mlpiper.stat.healthlib.{CategoricalHealthForSpark, ContinuousHistogramForSpark, HealthType}
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.io.File

@RunWith(classOf[JUnitRunner])
class ReflexSparkBatchPipelineITCase extends FlatSpec with TestEnv with Matchers {

  "Good Pipeline 1 - comma separated CSV to DataFrame" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/test_csv.csv").getPath


    val json =
      s"""
           {
            "name" : "ReflexPipeline_RF",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "/tmp/tmpFile",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "withHeaders": true
                    }
                },
                {
                    "name": "ReflexNullConnector",
                    "id": 2,
                    "type": "ReflexNullConnector",
                    "parents": [{"parent": 1, "output": 0}]
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json,
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)
  }

  "Good Pipeline 2 - space separated CSV to DataFrame" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/test_csv_space.csv").getPath


    val json =
      s"""
           {
            "name" : "ReflexPipeline_RF",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "/tmp/tmpFile",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "separator": " ",
                        "withHeaders": true
                    }
                },
                {
                    "name": "ReflexNullConnector",
                    "id": 2,
                    "type": "ReflexNullConnector",
                    "parents": [{"parent": 1, "output": 0}]
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json,
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)
  }

  "Good Pipeline 3 - CSV with headers to DataFrame" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/test_csv_headers.csv").getPath

    val json =
      s"""
           {
            "name" : "ReflexPipeline_RF",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "/tmp/tmpFile",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV to DataFrame",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "withHeaders": true
                    }
                },
                {
                    "name": "ReflexNullConnector",
                    "id": 2,
                    "type": "ReflexNullConnector",
                    "parents": [{"parent": 1, "output": 0}]
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json,
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)
  }

  "Good Pipeline 4 - Vector Assembler for all columns" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM2.txt").getPath

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 4 canceled!")
      cancel
    }

    val json =
      s"""
           {
            "name" : "ReflexPipeline_RF",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "$outTmpFilePath",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "withHeaders": true
                    }
                },
                {
                    "name": "VectorAssemblerComponent",
                    "id": 2,
                    "type": "VectorAssemblerComponent",
                    "parents": [{"parent": 1, "output": 0}],
                     "arguments" : {
                        "outputCol": "features"
                     }
                },
                {
                    "name": "ReflexNullConnector",
                    "id": 3,
                    "type": "ReflexNullConnector",
                    "parents": [{"parent": 2, "output": 0}]
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)
  }

  "Good Pipeline 5 - Vector Assembler with both include/exclude fields provided" should "throw" in {

    val rfSampleDataPath = getClass.getResource("/testSVM2.txt").getPath

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 4 canceled!")
      cancel
    }

    val json =
      s"""
           {
            "name" : "ReflexPipeline_RF",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "$outTmpFilePath",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "withHeaders": true
                    }
                },
                {
                    "name": "VectorAssemblerComponent",
                    "id": 2,
                    "type": "VectorAssemblerComponent",
                    "parents": [{"parent": 1, "output": 0}],
                     "arguments" : {
                        "includeCols": ["c0"],
                        "excludeCols": ["c0"],
                        "outputCol": "features"
                     }
                },
                {
                    "name": "ReflexNullConnector",
                    "id": 3,
                    "type": "ReflexNullConnector",
                    "parents": [{"parent": 2, "output": 0}]
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    val ex = intercept[IllegalArgumentException] {
      DagGen.main(args)
    }
    assert(ex.getMessage.contains("Only one parameter includeCols or excludeCols can be used in VectorAssemblerComponent"))
  }

  "Good Pipeline 6 - Spark RF (SPARKML) - Basic Cancelled " should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM2.txt").getPath

    if (!File(rfSampleDataPath).exists) {
      println("Test Good Pipeline 6 - Spark RF (SPARKML) - Basic Got Cancelled")
      cancel
    }

    val json =
      s"""
           {
            "name" : "ReflexPipeline_RF",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "$outTmpFilePath",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "withHeaders": false
                    }
                },
                {
                    "name": "VectorAssemblerComponent",
                    "id": 2,
                    "type": "VectorAssemblerComponent",
                    "parents": [{"parent": 1, "output": 0}],
                     "arguments" : {
                         "excludeCols": ["c0"],
                         "outputCol": "features"
                     }
                },
                {
                    "name": "RF algorithm (sparkML)",
                    "id": 4,
                    "type": "ReflexRandomForestML",
                    "arguments" : {
                        "num-classes": 5,
                        "num-trees": 4,
                        "maxDepth": 3,
                        "maxBins": 3,
                        "impurity": "gini",
                        "categorialFeatureInfo": 0,
                        "featureSubsetStrategy": "auto",
                        "featuresCol": "features",
                        "tempSharedPath": "file:///tmp",
                        "labelCol": "c0"
                    },
                    "parents": [{"parent": 2, "output": 0}]
                },
                {
                    "name": "SparkModelFileSink",
                    "id": 5,
                    "type": "SparkModelFileSink",
                    "parents": [{"parent": 4, "output": 0}],
                    "arguments" : {}
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)

    // Test that histograms are generated using JPMML model
    val file = new java.io.File(outTmpFilePath)
    if (!file.exists()) {
      assert(false)
    } else {
      val header = false
      val sparkSession = SparkSession.builder.
        master("local[*]").appName("Good pipeline 6 - Histogram").getOrCreate()

      val model = PipelineModel.read.load(outTmpFilePath)
      var df = sparkSession.sqlContext.read.option("inferSchema", value = true).option("header", header).csv(rfSampleDataPath)

      if (!header) {
        df = DataFrameUtils.renameColumns(df)
      }

      val genericHealthCreator = new ContinuousHistogramForSpark(HealthType.ContinuousHistogramHealth.toString)

      genericHealthCreator.dfOfDenseVector = df
      genericHealthCreator.sparkMLModel = Some(model)
      genericHealthCreator.binSizeForEachFeatureForRef = None
      genericHealthCreator.minBinValueForEachFeatureForRef = None
      genericHealthCreator.maxBinValueForEachFeatureForRef = None
      genericHealthCreator.enableAccumOutputOfHistograms = true
      genericHealthCreator.sparkContext = sparkSession.sparkContext

      val hist = genericHealthCreator.createHealth()

      assert(hist.nonEmpty)
      sparkSession.stop()
    }
  }

  "Good Pipeline 7 - Spark RF (SPARKML) - With MaxAbsScaler" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM2.txt").getPath

    if (!File(rfSampleDataPath).exists) {
      println("Test - Spark RF (SPARKML) - With MaxAbsScaler Is Cancelled")
      cancel
    }

    val json =
      s"""
           {
            "name" : "ReflexPipeline_RF",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "$outTmpFilePath",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "withHeaders": false
                    }
                },
                {
                    "name": "VectorAssemblerComponent",
                    "id": 2,
                    "type": "VectorAssemblerComponent",
                    "parents": [{"parent": 1, "output": 0}],
                     "arguments" : {
                         "excludeCols": ["c0"],
                         "outputCol": "featuresTmp"
                     }
                },
                {
                    "name": "MaxAbsoluteScalerPipeline",
                    "id": 3,
                    "type": "MaxAbsoluteScalerPipeline",
                    "parents": [{"parent": 2, "output": 0}],
                    "arguments" : {
                        "inputCol": "featuresTmp",
                        "outputCol": "features"
                    }
                },
                {
                    "name": "RF algorithm (sparkML)",
                    "id": 4,
                    "type": "ReflexRandomForestML",
                    "arguments" : {
                        "num-classes": 5,
                        "num-trees": 4,
                        "maxDepth": 3,
                        "maxBins": 3,
                        "impurity": "gini",
                        "categorialFeatureInfo": 0,
                        "featureSubsetStrategy": "auto",
                        "featuresCol": "features",
                        "tempSharedPath": "file:///tmp",
                        "labelCol": "c0"
                    },
                    "parents": [{"parent": 3, "output": 0}]
                },
                {
                    "name": "SparkModelFileSink",
                    "id": 5,
                    "type": "SparkModelFileSink",
                    "parents": [{"parent": 4, "output": 0}],
                    "arguments" : {}
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)

    // Test that histograms are generated using JPMML model
    val file = new java.io.File(outTmpFilePath)
    if (!file.exists()) {
      assert(false)
    } else {
      val header = false
      val sparkSession = SparkSession.builder.
        master("local[*]").appName("Good Histograms").getOrCreate()

      val model = PipelineModel.read.load(outTmpFilePath)
      var df = sparkSession.sqlContext.read.option("inferSchema", value = true).option("header", header).csv(rfSampleDataPath)

      if (!header) {
        df = DataFrameUtils.renameColumns(df)
      }

      val genericHealthCreator = new ContinuousHistogramForSpark(HealthType.ContinuousHistogramHealth.toString)

      genericHealthCreator.dfOfDenseVector = df
      genericHealthCreator.sparkMLModel = Some(model)
      genericHealthCreator.binSizeForEachFeatureForRef = None
      genericHealthCreator.minBinValueForEachFeatureForRef = None
      genericHealthCreator.maxBinValueForEachFeatureForRef = None
      genericHealthCreator.enableAccumOutputOfHistograms = true
      genericHealthCreator.sparkContext = sparkSession.sparkContext

      val hist = genericHealthCreator.createHealth().get

      val expectedBinEdgeForC4 = DenseVector[Double](Int.MinValue.toDouble +: Array(-0.1873, -0.05923, 0.06884, 0.1970, 0.3249, 0.4530, 0.5811, 0.7091, 0.8372, 0.9653, 1.0934) :+ Int.MaxValue.toDouble)
      val expectedHistForC4 = DenseVector[Double](0.0, 0.0, 283.0, 23.0, 15.0, 68.0, 128.0, 250.0, 168.0, 53.0, 12.0, 0.0)

      ComparatorUtils.compareBreezeOutputVector(a = hist("c4").hist, b = expectedHistForC4) should be(true)
      ComparatorUtils.compareBreezeOutputVector(a = hist("c4").binEdges, b = expectedBinEdgeForC4) should be(true)

      val expectedBinEdgeForC5 = DenseVector[Double](Int.MinValue.toDouble +: Array(0.0792, 0.15107, 0.22294, 0.29481, 0.36668, 0.43855, 0.51042, 0.58229, 0.65416, 0.72603, 0.7979) :+ Int.MaxValue.toDouble)
      val expectedHistForC5 = DenseVector[Double](43.0, 16.0, 51.0, 98.0, 134.0, 122.0, 150.0, 198.0, 128.0, 19.0, 13.0, 28.0)

      ComparatorUtils.compareBreezeOutputVector(a = hist("c5").hist, b = expectedHistForC5) should be(true)
      ComparatorUtils.compareBreezeOutputVector(a = hist("c5").binEdges, b = expectedBinEdgeForC5) should be(true)

      val expectedBinEdgeForC6 = DenseVector[Double](Int.MinValue.toDouble +: Array(0.0437, 0.05706, 0.0704, 0.0837, 0.09714, 0.1105, 0.1238, 0.13722, 0.15058, 0.16394, 0.1773) :+ Int.MaxValue.toDouble)
      val expectedHistForC6 = DenseVector[Double](0.0, 49.0, 61.0, 79.0, 208.0, 128.0, 164.0, 174.0, 45.0, 20.0, 10.0, 62.0)

      ComparatorUtils.compareBreezeOutputVector(a = hist("c6").hist, b = expectedHistForC6) should be(true)
      ComparatorUtils.compareBreezeOutputVector(a = hist("c6").binEdges, b = expectedBinEdgeForC6) should be(true)

      val expectedBinEdgeForC7 = DenseVector[Double](Int.MinValue.toDouble +: Array(0.0282, 0.0912, 0.1542, 0.2172, 0.2802, 0.3432, 0.4062, 0.4692, 0.5322, 0.5952, 0.6582) :+ Int.MaxValue.toDouble)
      val expectedHistForC7 = DenseVector[Double](0.0, 0.0, 0.0, 43.0, 585.0, 102.0, 38.0, 29.0, 28.0, 48.0, 46.0, 81.0)

      ComparatorUtils.compareBreezeOutputVector(a = hist("c7").hist, b = expectedHistForC7) should be(true)
      ComparatorUtils.compareBreezeOutputVector(a = hist("c7").binEdges, b = expectedBinEdgeForC7) should be(true)

      val expectedBinEdgeForC11 = DenseVector[Double](Int.MinValue.toDouble +: Array(-0.0466, 0.00902, 0.06464, 0.12026, 0.17588, 0.2315, 0.28712, 0.34274, 0.39836, 0.45398, 0.5096) :+ Int.MaxValue.toDouble)
      val expectedHistForC11 = DenseVector[Double](0.0, 3.0, 51.0, 154.0, 279.0, 160.0, 37.0, 14.0, 66.0, 201.0, 25.0, 10.0)

      ComparatorUtils.compareBreezeOutputVector(a = hist("c11").hist, b = expectedHistForC11) should be(true)
      ComparatorUtils.compareBreezeOutputVector(a = hist("c11").binEdges, b = expectedBinEdgeForC11) should be(true)

      val expectedBinEdgeForC12 = DenseVector[Double](Int.MinValue.toDouble +: Array(-0.1109, -0.0874, -0.06404, -0.04061, -0.01718, 0.00625, 0.02967, 0.0531, 0.07654, 0.09997, 0.1234) :+ Int.MaxValue.toDouble)
      val expectedHistForC12 = DenseVector[Double](0.0, 0.0, 0.0, 0.0, 0.0, 937.0, 48.0, 4.0, 3.0, 1.0, 0.0, 7.0)

      ComparatorUtils.compareBreezeOutputVector(a = hist("c12").hist, b = expectedHistForC12) should be(true)
      ComparatorUtils.compareBreezeOutputVector(a = hist("c12").binEdges, b = expectedBinEdgeForC12) should be(true)

      val expectedBinEdgeForC13 = DenseVector[Double](Int.MinValue.toDouble +: Array(-0.0424, 0.02301, 0.08842, 0.15383, 0.21924, 0.2846, 0.35006, 0.41547, 0.48088, 0.5462, 0.6116) :+ Int.MaxValue.toDouble)
      val expectedHistForC13 = DenseVector[Double](0.0, 14.0, 47.0, 158.0, 260.0, 155.0, 49.0, 7.0, 68.0, 192.0, 50.0, 0.0)

      ComparatorUtils.compareBreezeOutputVector(a = hist("c13").hist, b = expectedHistForC13) should be(true)
      ComparatorUtils.compareBreezeOutputVector(a = hist("c13").binEdges, b = expectedBinEdgeForC13) should be(true)

      val expectedBinEdgeForC23 = DenseVector[Double](Int.MinValue.toDouble +: Array(-0.1499, -0.10589, -0.06187, -0.01786, 0.02614, 0.07015, 0.11416, 0.15817, 0.20218, 0.24619, 0.2902) :+ Int.MaxValue.toDouble)
      val expectedHistForC23 = DenseVector[Double](0.0, 0.0, 0.0, 0.0, 510.0, 91.0, 207.0, 40.0, 77.0, 16.0, 20.0, 39.0)

      ComparatorUtils.compareBreezeOutputVector(a = hist("c23").hist, b = expectedHistForC23) should be(true)
      ComparatorUtils.compareBreezeOutputVector(a = hist("c23").binEdges, b = expectedBinEdgeForC23) should be(true)

      val expectedBinEdgeForC24 = DenseVector[Double](Int.MinValue.toDouble +: Array(-0.2141, -0.10778, -0.00146, 0.10486, 0.2111, 0.3175, 0.42382, 0.53014, 0.63646, 0.74278, 0.8491) :+ Int.MaxValue.toDouble)
      val expectedHistForC24 = DenseVector[Double](0.0, 0.0, 0.0, 126.0, 483.0, 83.0, 25.0, 20.0, 23.0, 111.0, 111.0, 18.0)

      ComparatorUtils.compareBreezeOutputVector(a = hist("c24").hist, b = expectedHistForC24) should be(true)
      ComparatorUtils.compareBreezeOutputVector(a = hist("c24").binEdges, b = expectedBinEdgeForC24) should be(true)

      sparkSession.stop()
    }
  }

  "Good Pipeline Histogram From DataFrame Having Categorical Features" should "be valid end to end" in {

    val dataHistPath = getClass.getResource("/categoricalHist.txt").getPath

    if (!File(dataHistPath).exists) {
      println("Pipeline Histogram From DataFrame!")
      cancel
    }

    val json =
      s"""{
          "name": "RF_Train",
          "engineType": "SparkBatch",
          "pipe": [
            {
              "id": 0,
              "name": "CsvToDF",
              "type": "CsvToDF",
              "parents": [

              ],
              "arguments": {
                "separator": ",",
                "filepath": "$dataHistPath",
                "withHeaders": false
              }
            },
            {
              "id": 1,
              "name": "VectorAssemblerComponent",
              "type": "VectorAssemblerComponent",
              "parents": [
                {
                  "parent": 0,
                  "output": 0,
                  "input": 0
                }
              ],
              "arguments": {
                "excludeCols": [
                  "c0"
                ],
                "outputCol": "featuresIn"
              }
            },
            {
              "name": "VectorIndexerComponent",
              "id": 2,
              "type": "VectorIndexerComponent",
              "parents": [{"parent": 1, "output": 0}],
              "arguments" : {
                "maxCategories": 25,
                "inputCol": "featuresIn",
                "outputCol": "features"
              }
            },
            {
              "id": 3,
              "name": "ReflexRandomForestML",
              "type": "ReflexRandomForestML",
              "parents": [
                {
                  "parent": 2,
                  "output": 0,
                  "input": 0
                }
              ],
              "arguments": {
                "num-classes": 1,
                "num-trees": 2,
                "maxDepth": 2,
                "maxBins": 10,
                "impurity": "gini",
                "featureSubsetStrategy": "auto",
                "labelCol": "c0",
                "tempSharedPath": "file:///tmp",
                "featuresCol": "features"
              }
            },
            {
                "name": "SparkModelFileSink",
                "id": 4,
                "type": "SparkModelFileSink",
                "parents": [{"parent": 3, "output": 0}],
                "arguments" : {}
            }
          ],
          "systemConfig": {
               "statsDBHost": "localhost",
               "statsDBPort": 8086,
               "modelSocketSinkPort": 7777,
               "modelSocketSourcePort": 7777,
               "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
               "statsMeasurementID":"1",
               "modelFileSinkPath": "$outTmpFilePath",
               "healthStatFilePath": "/tmp/tmpHealthFile"
         }
        }""".stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000," +
        "spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)

    // Test that histograms are generated using JPMML model
    val file = new java.io.File(outTmpFilePath)
    if (!file.exists()) {
      assert(false)
    } else {
      val header = false
      val sparkSession = SparkSession.builder.
        master("local[*]").appName("Good Histograms").getOrCreate()

      val model = PipelineModel.read.load(outTmpFilePath)
      var df = sparkSession.sqlContext.read.option("inferSchema", value = true).option("header", header).csv(dataHistPath)

      if (!header) {
        df = DataFrameUtils.renameColumns(df)
      }

      val genericHealthCreator = new CategoricalHealthForSpark(HealthType.CategoricalHistogramHealth.toString)

      genericHealthCreator.dfOfDenseVector = df
      genericHealthCreator.sparkMLModel = Some(model)
      genericHealthCreator.enableAccumOutputOfHistograms = true
      genericHealthCreator.sc = sparkSession.sparkContext

      val hist = genericHealthCreator.createHealth().get
      assert(hist.nonEmpty)

      val expectedHistForC649 = Map("1" -> 0.15, "0" -> 0.85)
      hist("c649").getCategoricalCount == expectedHistForC649 should be(true)

      val expectedHistForC282 = Map("1" -> 0.05, "0" -> 0.95)
      hist("c282").getCategoricalCount == expectedHistForC282 should be(true)

      val expectedHistForC669 = Map("1" -> 0.4, "0" -> 0.6)
      hist("c669").getCategoricalCount == expectedHistForC669 should be(true)

      val expectedHistForC411 = Map("1" -> 0.1, "0" -> 0.9)
      hist("c411").getCategoricalCount == expectedHistForC411 should be(true)

      sparkSession.stop()
    }
  }

  "Good Pipeline Histogram From DataFrame" should "be valid end to end" in {

    val dataHistPath = getClass.getResource("/dataHist.txt").getPath

    if (!File(dataHistPath).exists) {
      println("Pipeline Histogram From DataFrame!")
      cancel
    }

    val json =
      s"""{
          "name": "RF_Train",
          "engineType": "SparkBatch",
          "pipe": [
            {
              "id": 0,
              "name": "CsvToDF",
              "type": "CsvToDF",
              "parents": [

              ],
              "arguments": {
                "separator": ",",
                "filepath": "$dataHistPath",
                "withHeaders": true
              }
            },
            {
              "id": 1,
              "name": "VectorAssemblerComponent",
              "type": "VectorAssemblerComponent",
              "parents": [
                {
                  "parent": 0,
                  "output": 0,
                  "input": 0
                }
              ],
              "arguments": {
                "excludeCols": [
                  "A"
                ],
                "outputCol": "features"
              }
            },
            {
              "id": 2,
              "name": "MaxAbsoluteScalerPipeline",
              "type": "MaxAbsoluteScalerPipeline",
              "parents": [
                {
                  "parent": 1,
                  "output": 0,
                  "input": 0
                }
              ],
              "arguments": {
                "inputCol": "features",
                "outputCol": "newFeatures"
              }
            },
            {
              "id": 3,
              "name": "ReflexRandomForestML",
              "type": "ReflexRandomForestML",
              "parents": [
                {
                  "parent": 2,
                  "output": 0,
                  "input": 0
                }
              ],
              "arguments": {
                "num-classes": 2,
                "num-trees": 4,
                "maxDepth": 3,
                "maxBins": 3,
                "impurity": "gini",
                "featureSubsetStrategy": "auto",
                "labelCol": "A",
                "tempSharedPath": "file:///tmp",
                "featuresCol": "newFeatures"
              }
            },
            {
                "name": "SparkModelFileSink",
                "id": 4,
                "type": "SparkModelFileSink",
                "parents": [{"parent": 3, "output": 0}],
                "arguments" : {}
            }
          ],
          "systemConfig": {
               "statsDBHost": "localhost",
               "statsDBPort": 8086,
               "modelSocketSinkPort": 7777,
               "modelSocketSourcePort": 7777,
               "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
               "statsMeasurementID":"1",
               "modelFileSinkPath": "$outTmpFilePath",
               "healthStatFilePath": "/tmp/tmpHealthFile"
         }
        }""".stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)

    // Test that histograms are generated using JPMML model
    val file = new java.io.File(outTmpFilePath)
    if (!file.exists()) {
      assert(false)
    } else {
      val header = true
      val sparkSession = SparkSession.builder.
        master("local[*]").appName("Good Histograms").getOrCreate()

      val model = PipelineModel.read.load(outTmpFilePath)
      var df = sparkSession.sqlContext.read.option("inferSchema", value = true).option("header", header).csv(dataHistPath)

      if (!header) {
        df = DataFrameUtils.renameColumns(df)
      }

      val genericHealthCreator = new ContinuousHistogramForSpark(HealthType.ContinuousHistogramHealth.toString)

      genericHealthCreator.dfOfDenseVector = df
      genericHealthCreator.sparkMLModel = Some(model)
      genericHealthCreator.binSizeForEachFeatureForRef = None
      genericHealthCreator.minBinValueForEachFeatureForRef = None
      genericHealthCreator.maxBinValueForEachFeatureForRef = None
      genericHealthCreator.enableAccumOutputOfHistograms = true
      genericHealthCreator.sparkContext = sparkSession.sparkContext

      val hist = genericHealthCreator.createHealth().get

      val expectedBinEdgeForB = DenseVector[Double](Int.MinValue.toDouble +: Array(0.0108, 0.0186, 0.02656, 0.03444, 0.0423, 0.05019, 0.05808, 0.06596, 0.07384, 0.08172, 0.0896) :+ Int.MaxValue.toDouble)
      val expectedHistForB = DenseVector[Double](0.0, 1.0, 5.0, 4.0, 0.0, 2.0, 3.0, 8.0, 6.0, 1.0, 0.0, 0.0)

      ComparatorUtils.compareBreezeOutputVector(a = hist("B").hist, b = expectedHistForB) should be(true)
      ComparatorUtils.compareBreezeOutputVector(a = hist("B").binEdges, b = expectedBinEdgeForB) should be(true)

      val expectedBinEdgeForD = DenseVector[Double](Int.MinValue.toDouble +: Array(-0.362, -0.26397, -0.16594, -0.06791, 0.03012, 0.12815, 0.22618, 0.32421, 0.42223, 0.52026, 0.61839) :+ Int.MaxValue.toDouble)
      val expectedHistForD = DenseVector[Double](0.0, 0.0, 0.0, 0.0, 21.0, 1.0, 2.0, 1.0, 1.0, 0.0, 3.0, 1.0)

      ComparatorUtils.compareBreezeOutputVector(a = hist("D").hist, b = expectedHistForD) should be(true)
      ComparatorUtils.compareBreezeOutputVector(a = hist("D").binEdges, b = expectedBinEdgeForD) should be(true)

      val expectedBinEdgeForE = DenseVector[Double](Int.MinValue.toDouble +: Array(0.9207, 0.92902, 0.93734, 0.94566, 0.95397, 0.96229, 0.97062, 0.97894, 0.98726, 0.99558, 1.0039) :+ Int.MaxValue.toDouble)
      val expectedHistForE = DenseVector[Double](0.0, 0.0, 1.0, 6.0, 9.0, 2.0, 2.0, 0.0, 4.0, 5.0, 1.0, 0.0)

      ComparatorUtils.compareBreezeOutputVector(a = hist("E").hist, b = expectedHistForE) should be(true)
      ComparatorUtils.compareBreezeOutputVector(a = hist("E").binEdges, b = expectedBinEdgeForE) should be(true)

      val expectedBinEdgeForG = DenseVector[Double](Int.MinValue.toDouble +: Array(0.0503, 0.05074, 0.051179, 0.05162, 0.052059, 0.0525, 0.05294, 0.05338, 0.05382, 0.054259, 0.0547) :+ Int.MaxValue.toDouble)
      val expectedHistForG = DenseVector[Double](0.0, 0.0, 5.0, 4.0, 2.0, 2.0, 5.0, 3.0, 7.0, 2.0, 0.0, 0.0)

      ComparatorUtils.compareBreezeOutputVector(a = hist("G").hist, b = expectedHistForG) should be(true)
      ComparatorUtils.compareBreezeOutputVector(a = hist("G").binEdges, b = expectedBinEdgeForG) should be(true)

      val expectedBinEdgeForL = DenseVector[Double](Int.MinValue.toDouble +: Array(-0.017, -0.00822, 5.59999E-4, 0.009339, 0.018119, 0.0268999, 0.035679, 0.04446, 0.0532399, 0.062019, 0.070799) :+ Int.MaxValue.toDouble)
      val expectedHistForL = DenseVector[Double](0.0, 0.0, 0.0, 4.0, 7.0, 5.0, 8.0, 3.0, 2.0, 0.0, 0.0, 1.0)

      ComparatorUtils.compareBreezeOutputVector(a = hist("L").hist, b = expectedHistForL) should be(true)
      ComparatorUtils.compareBreezeOutputVector(a = hist("L").binEdges, b = expectedBinEdgeForL) should be(true)

      assert(hist.nonEmpty)
      sparkSession.stop()
    }
  }

  "Good Pipeline 8 - Spark Logistic Regression" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM2.txt").getPath

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 5 canceled!")
      cancel
    }
    println("RF input data sample path: " + rfSampleDataPath)


    val json =
      s"""
           {
            "name" : "ReflexPipeline_RF",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "$outTmpFilePath",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "withHeaders": false
                    }
                },
                {
                    "name": "VectorAssemblerComponent",
                    "id": 2,
                    "type": "VectorAssemblerComponent",
                    "parents": [{"parent": 1, "output": 0}],
                    "arguments" : {
                         "excludeCols": ["c0"],
                         "outputCol": "features"
                    }
                },
                {
                    "name": "Reflex Logistic Regression",
                    "id": 3,
                    "type": "ReflexLogisticRegression",
                    "arguments" : {
                         "tempSharedPath": "file:///tmp",
                         "labelCol": "c0",
                         "thresholds": ["0"," 0.5"],
                         "featuresCol": "features"
                    },
                    "parents": [{"parent": 2, "output": 0}]
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)
  }

  "Good Pipeline 8 With Valid- Spark Logistic Regression" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM2.txt").getPath

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 5 canceled!")
      cancel
    }
    println("RF input data sample path: " + rfSampleDataPath)


    val json =
      s"""
           {
            "name" : "ReflexPipeline_RF",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "$outTmpFilePath",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "withHeaders": false
                    }
                },
                {
                    "name": "VectorAssemblerComponent",
                    "id": 2,
                    "type": "VectorAssemblerComponent",
                    "parents": [{"parent": 1, "output": 0}],
                    "arguments" : {
                         "excludeCols": ["c0"],
                         "outputCol": "features"
                    }
                },
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 4,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                      "filepath": "$rfSampleDataPath",
                      "withHeaders": false
                    }
                },
                {
                    "name": "VectorAssemblerComponent",
                    "id": 5,
                    "type": "VectorAssemblerComponent",
                    "parents": [{"parent": 4, "output": 0}],
                    "arguments" : {
                      "excludeCols": ["c0"],
                      "outputCol": "features"
                    }
                },
                {
                    "name": "Reflex Logistic Regression",
                    "id": 3,
                    "type": "ReflexLogisticRegression",
                    "arguments" : {
                         "tempSharedPath": "file:///tmp",
                         "labelCol": "c0",
                         "thresholds": ["0"," 0.5"],
                         "featuresCol": "features"
                    },
                    "parents": [{"parent": 2, "output": 0}, {"parent": 5, "output": 0}]
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)
  }

  "Good Pipeline 9 - Spark KMeans Clustering (SPARKML)" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM2.txt").getPath()

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 9 canceled!")
      cancel
    }
    println("Kmeans input data sample path: " + rfSampleDataPath)


    val json =
      s"""
           {
            "name" : "ReflexPipeline_Kmeans_Clust",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "$outTmpFilePath",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "withHeaders": false
                    }
                },
                {
                    "name": "VectorAssemblerComponent",
                    "id": 2,
                    "type": "VectorAssemblerComponent",
                    "parents": [{"parent": 1, "output": 0}],
                    "arguments" : {
                         "outputCol": "features"
                    }
                },
                {
                    "name": "Kmeans Clustering algorithm (sparkML)",
                    "id": 3,
                    "type": "ReflexKmeansML",
                    "arguments" : {
                     "k": 3,
                     "featuresCol": "features",
                     "predictionCol": "predictions"
                    },
                    "parents": [{"parent": 2, "output": 0}]
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)
  }

  "Good Pipeline 10 - Spark GBT Reg (SPARKML)" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM2b.csv").getPath

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 10 canceled!")
      cancel
    }
    println("GBT Reg input data sample path: " + rfSampleDataPath)


    val json =
      s"""
           {
            "name" : "ReflexPipeline_GBT_Reg",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "$outTmpFilePath",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "withHeaders": false
                    }
                },
                {
                    "name": "VectorAssemblerComponent",
                    "id": 2,
                    "type": "VectorAssemblerComponent",
                    "parents": [{"parent": 1, "output": 0}],
                    "arguments" : {
                         "excludeCols": ["c0"],
                         "outputCol": "features"
                    }
                },
                {
                    "name": "StringIndexerComponent",
                    "id": 3,
                    "type": "StringIndexerComponent",
                    "parents": [{"parent": 2, "output": 0}],
                    "arguments" : {
                         "inputCol": "c0",
                         "outputCol": "c00"
                    }
                },
                {
                   "name": "RF algorithm (sparkML)",
                   "id": 4,
                   "type": "ReflexRandomForestML",
                   "arguments" : {
                       "num-classes": 2,
                       "savedModelType": "sparkML",
                       "tempSharedPath": "file:///tmp",
                       "num-trees": 4,
                       "maxDepth": 3,
                       "maxBins": 1000,
                       "impurity": "gini",
                       "categorialFeatureInfo": 0,
                       "significantFeaturesNumber": 5,
                       "featureSubsetStrategy": "auto",
                       "featuresCol": "features",
                       "labelCol": "c00"
                   },
                    "parents": [{"parent": 3, "output": 0}]
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)
  }


  "Good Pipeline 11 - Spark RF Class (SPARKML) with PCA" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM2.txt").getPath

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 11 canceled!")
      cancel
    }
    println("RF input data sample path: " + rfSampleDataPath)


    val json =
      s"""
         {
          "name" : "ReflexPipeline_RF_Class",
          "engineType": "SparkBatch",
          "systemConfig" : {
                  "statsDBHost": "localhost",
                  "statsDBPort": 8086,
                  "modelSocketSinkPort": 7777,
                  "modelSocketSourcePort": 7777,
                  "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                  "statsMeasurementID":"1",
                  "modelFileSinkPath": "$outTmpFilePath",
                  "healthStatFilePath": "/tmp/tmpHealthFile"
               },
          "pipe" : [
              {
                  "name": "Read data from CSV (txt format)",
                  "id": 1,
                  "type": "CsvToDF",
                  "parents": [],
                  "arguments" : {
                      "filepath": "$rfSampleDataPath",
                      "withHeaders": false
                  }
              },
              {
                  "name": "VectorAssemblerComponent",
                  "id": 2,
                  "type": "VectorAssemblerComponent",
                  "parents": [{"parent": 1, "output": 0}],
                  "arguments" : {
                       "excludeCols": ["c0"],
                       "outputCol": "featuresIn"
                  }
              },
              {
                  "name": "PCAFEComponent",
                  "id": 3,
                  "type": "PCAFE",
                  "parents": [{"parent": 2, "output": 0}],
                  "arguments" : {
                      "k": 3,
                      "inputCol": "featuresIn",
                      "outputCol": "features"
                  }
              },
              {
                  "name": "RF algorithm (sparkML)",
                  "id": 4,
                  "type": "ReflexRandomForestML",
                  "arguments" : {
                      "num-classes": 5,
                      "num-trees": 4,
                      "maxDepth": 3,
                      "maxBins": 3,
                      "impurity": "gini",
                      "categorialFeatureInfo": 0,
                      "featureSubsetStrategy": "auto",
                      "featuresCol": "features",
                      "labelCol": "c0"
                  },
                  "parents": [{"parent": 3, "output": 0}]
              }
           ]
         }
     """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)
  }


  "Good Pipeline 12 - Spark RF Class (SPARKML) with VectorIndexer" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM4.csv").getPath

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 12 canceled!")
      cancel
    }
    println("RF input data sample path: " + rfSampleDataPath)


    val json =
      s"""
           {
            "name" : "ReflexPipeline_RF_Class_VectorIndexer",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "$outTmpFilePath",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "withHeaders": false
                    }
                },
                {
                    "name": "VectorAssemblerComponent",
                    "id": 2,
                    "type": "VectorAssemblerComponent",
                    "parents": [{"parent": 1, "output": 0}],
                    "arguments" : {
                         "excludeCols": ["c0"],
                         "outputCol": "featuresIn"
                    }
                },
                {
                    "name": "VectorIndexerComponent",
                    "id": 3,
                    "type": "VectorIndexerComponent",
                    "parents": [{"parent": 2, "output": 0}],
                    "arguments" : {
                        "maxCategories": 25,
                        "inputCol": "featuresIn",
                        "outputCol": "features"
                    }
                },
                {
                    "name": "RF algorithm (sparkML)",
                    "id": 4,
                    "type": "ReflexRandomForestML",
                    "arguments" : {
                        "num-classes": 5,
                        "num-trees": 4,
                        "maxDepth": 3,
                        "maxBins": 1000,
                        "impurity": "gini",
                        "categorialFeatureInfo": 0,
                        "significantFeaturesNumber": 5,
                        "featureSubsetStrategy": "auto",
                        "featuresCol": "features",
                        "labelCol": "c0"
                    },
                    "parents": [{"parent": 3, "output": 0}]
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)
  }


  "Good Pipeline 13 - Spark RF Classification (SPARKML) with StringIndexer" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM2.txt").getPath

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 13 canceled!")
      cancel
    }


    val json =
      s"""
           {
            "name" : "ReflexPipeline_RF_Class_StringIndexer",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "$outTmpFilePath",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "withHeaders": false
                    }
                },
                {
                    "name": "StringIndexerComponent",
                    "id": 2,
                    "type": "StringIndexerComponent",
                    "parents": [{"parent": 1, "output": 0}],
                    "arguments" : {
                        "inputCol": "c1",
                        "outputCol": "c25"
                    }
                 },
                {
                    "name": "VectorAssemblerComponent",
                    "id": 3,
                    "type": "VectorAssemblerComponent",
                    "parents": [{"parent": 2, "output": 0}],
                    "arguments" : {
                         "includeCols": ["c2", "c3", "c4", "c7", "c12", "c11", "c12", "c25"],
                         "outputCol": "features"
                    }
                },
                {
                "   name": "RF algorithm (sparkML)",
                    "id": 4,
                    "type": "ReflexRandomForestML",
                    "arguments" : {
                        "num-classes": 5,
                        "num-trees": 4,
                        "maxDepth": 3,
                        "maxBins": 1000,
                        "impurity": "gini",
                        "categorialFeatureInfo": 0,
                        "featureSubsetStrategy": "auto",
                        "featuresCol": "features",
                        "labelCol": "c0"
                    },
                    "parents": [{"parent": 3, "output": 0}]
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)
  }


  "Good Pipeline 14 - Spark RF Classification (SPARKML) with Binarizer" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM2.txt").getPath

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 14 canceled!")
      cancel
    }


    val json =
      s"""
           {
            "name" : "ReflexPipeline_RF_class_Binarizer",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "$outTmpFilePath",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "withHeaders": false
                    }
                },
                {
                    "name": "BinarizerComponent",
                    "id": 2,
                    "type": "BinarizerComponent",
                    "parents": [{"parent": 1, "output": 0}],
                    "arguments" : {
                        "threshold": 0.1,
                        "inputCol": "c3",
                        "outputCol": "c25"
                    }
                },
                {
                     "name": "VectorAssemblerComponent",
                     "id": 3,
                     "type": "VectorAssemblerComponent",
                     "parents": [{"parent": 2, "output": 0}],
                     "arguments" : {
                         "includeCols": ["c1", "c2", "c4", "c7", "c12", "c11", "c12", "c25"],
                         "outputCol": "features"
                     }
                },
                {
                    "name": "RF algorithm (sparkML)",
                    "id": 4,
                    "type": "ReflexRandomForestML",
                    "arguments" : {
                        "num-classes": 5,
                        "num-trees": 4,
                        "maxDepth": 3,
                        "maxBins": 3,
                        "impurity": "gini",
                        "categorialFeatureInfo": 0,
                        "featureSubsetStrategy": "auto",
                        "featuresCol": "features",
                        "labelCol": "c0"
                    },
                    "parents": [{"parent": 3, "output": 0}]
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)
  }


  "Good Pipeline 15 - Spark RF classification (SPARKML) with Vector Slicer" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM2.txt").getPath

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 15 canceled!")
      cancel
    }


    val json =
      s"""
           {
            "name" : "ReflexPipeline_RF_Class_slice",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "$outTmpFilePath",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "withHeaders": false
                    }
                },
                {
                    "name": "VectorAssemblerComponent",
                    "id": 2,
                    "type": "VectorAssemblerComponent",
                    "parents": [{"parent": 1, "output": 0}],
                    "arguments" : {
                         "excludeCols": ["c0"],
                         "outputCol": "featuresIn"
                    }
                },
                {
                    "name": "VectorSlicerComponent",
                    "id": 3,
                    "type": "VectorSlicerComponent",
                    "parents": [{"parent": 2, "output": 0}],
                    "arguments" : {
                        "includeIndices": [" 2", " 3", "4", "5"],
                        "inputCol": "featuresIn",
                        "outputCol": "features"
                    }
                },
                {
                    "name": "RF algorithm (sparkML)",
                    "id": 4,
                    "type": "ReflexRandomForestML",
                    "arguments" : {
                        "num-classes": 5,
                        "num-trees": 4,
                        "maxDepth": 3,
                        "maxBins": 3,
                        "impurity": "gini",
                        "categorialFeatureInfo": 0,
                        "featureSubsetStrategy": "auto",
                        "featuresCol": "features",
                        "labelCol": "c0"
                    },
                    "parents": [{"parent": 3, "output": 0}]
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)
  }


  "Good Pipeline 16 - Spark RF Class (SPARKML) with QuantileDiscretizer" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM2.txt").getPath

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 16 canceled!")
      cancel
    }


    val json =
      s"""
           {
            "name" : "ReflexPipeline_RF_Class_QuantileDiscretizer",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "$outTmpFilePath",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "withHeaders": false
                    }
                },
                {
                    "name": "QuantileDiscretizerComponent",
                    "id": 2,
                    "type": "QuantileDiscretizerComponent",
                    "parents": [{"parent": 1, "output": 0}],
                    "arguments" : {
                        "numBuckets": 6,
                        "inputCol": "c3",
                        "outputCol": "c25"
                    }
                },
                {
                    "name": "VectorAssemblerComponent",
                    "id": 3,
                    "type": "VectorAssemblerComponent",
                    "parents": [{"parent": 2, "output": 0}],
                    "arguments" : {
                         "includeCols": ["c25", "c4"],
                         "outputCol": "features"
                    }
                },
                {
                    "name": "RF algorithm (sparkML)",
                    "id": 4,
                    "type": "ReflexRandomForestML",
                    "arguments" : {
                        "num-classes": 5,
                        "num-trees": 4,
                        "maxDepth": 3,
                        "maxBins": 1000,
                        "impurity": "gini",
                        "categorialFeatureInfo": 0,
                        "featureSubsetStrategy": "auto",
                        "featuresCol": "features",
                        "labelCol": "c0"
                    },
                    "parents": [{"parent": 3, "output": 0}]
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)
  }


  "Good Pipeline 17 - Spark RF Class (SPARKML) with StringIndexer and OneHotEnc" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM4.csv").getPath

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 17 canceled!")
      cancel
    }


    val json =
      s"""
           {
            "name" : "ReflexPipeline_RF_class_StringIndexer_OneHot",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "$outTmpFilePath",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "withHeaders": false
                    }
                },
                {
                    "name": "StringIndexerComponent",
                    "id": 2,
                    "type": "StringIndexerComponent",
                    "parents": [{"parent": 1, "output": 0}],
                    "arguments" : {
                        "inputCol": "c1",
                        "outputCol": "c25"
                    }
                },
                {
                    "name": "OneHotEncoderComponent",
                    "id": 3,
                    "type": "OneHotEncoderComponent",
                    "parents": [{"parent": 2, "output": 0}],
                    "arguments" : {
                        "inputCol": "c25",
                        "outputCol": "c26"
                    }
                },
                {
                    "name": "VectorAssemblerComponent",
                    "id": 4,
                    "type": "VectorAssemblerComponent",
                    "parents": [{"parent": 3, "output": 0}],
                    "arguments" : {
                         "includeCols": ["c26", "c5", "c4"],
                         "outputCol": "features"
                    }
                },
                {
                    "name": "RF algorithm (sparkML)",
                    "id": 5,
                    "type": "ReflexRandomForestML",
                    "arguments" : {
                        "num-classes": 5,
                        "num-trees": 4,
                        "maxDepth": 3,
                        "maxBins": 1000,
                        "impurity": "gini",
                        "categorialFeatureInfo": 0,
                        "featureSubsetStrategy": "auto",
                        "featuresCol": "features",
                        "labelCol": "c0"
                    },
                    "parents": [{"parent": 4, "output": 0}]
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)
  }


  "Good Pipeline 18 - Spark RF Class (SPARKML) with RFormula" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM2.txt").getPath

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 18 canceled!")
      cancel
    }


    val json =
      s"""
           {
            "name" : "ReflexPipeline_DT_Reg_Rformula",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "$outTmpFilePath",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "withHeaders": false
                    }
                },
                {
                    "name": "RFormulaComponent",
                    "id": 2,
                    "type": "RFormulaComponent",
                    "parents": [{"parent": 1, "output": 0}],
                    "arguments" : {
                        "RFormula": "c0 ~ .",
                        "featuresCol": "features",
                        "labelCol": "c0"
                    }
                },
                {
                    "name": "RF algorithm (sparkML)",
                    "id": 3,
                    "type": "ReflexRandomForestML",
                    "arguments" : {
                        "num-classes": 5,
                        "num-trees": 4,
                        "maxDepth": 3,
                        "maxBins": 1000,
                        "impurity": "gini",
                        "categorialFeatureInfo": 0,
                        "featureSubsetStrategy": "auto",
                        "featuresCol": "features",
                        "labelCol": "c0"
                    },
                    "parents": [{"parent": 2, "output": 0}]
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)
  }


  "Good Pipeline 19 - Spark GLM (SPARKML)" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM2.txt").getPath

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 19 canceled!")
      cancel
    }


    val json =
      s"""
           {
            "name" : "ReflexPipeline_GLM",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "$outTmpFilePath",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "withHeaders": false
                    }
                },
                {
                    "name": "VectorAssemblerComponent",
                    "id": 2,
                    "type": "VectorAssemblerComponent",
                    "parents": [{"parent": 1, "output": 0}],
                    "arguments" : {
                         "excludeCols": ["c0"],
                         "outputCol": "features"
                    }
                },
                {
                    "name": "GLM algorithm (sparkML)",
                    "id": 3,
                    "type": "ReflexGLM",
                    "arguments" : {
                        "tempSharedPath": "file:///tmp",
                        "featuresCol": "features",
                        "predictionCol": "predictions",
                        "linkPredictionCol": "linkPrediction",
                        "link": "identity",
                        "labelCol": "c0"
                    },
                    "parents": [{"parent": 2, "output": 0}]
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)
  }


  "Good Pipeline 20 - Spark Linear Regression" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM2.txt").getPath

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 20 canceled!")
      cancel
    }


    val json =
      s"""
           {
            "name" : "ReflexPipeline_LR",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "$outTmpFilePath",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "withHeaders": false
                    }
                },
                {
                    "name": "VectorAssemblerComponent",
                    "id": 2,
                    "type": "VectorAssemblerComponent",
                    "parents": [{"parent": 1, "output": 0}],
                    "arguments" : {
                         "excludeCols": ["c0"],
                         "outputCol": "features"
                    }
                },
                {
                    "name": "Reflex Linear Regression",
                    "id": 3,
                    "type": "ReflexLinearRegression",
                    "arguments" : {
                         "tempSharedPath": "file:///tmp",
                         "labelCol": "c0",
                         "featuresCol": "features",
                         "predictionCol": "predictions"
                    },
                    "parents": [{"parent": 2, "output": 0}]
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)
  }


  "Good Pipeline 21 - Spark GBT Class (SPARKML)" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM2.txt").getPath

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 21 canceled!")
      cancel
    }


    val json =
      s"""
           {
            "name" : "ReflexPipeline_GBT_Class",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "$outTmpFilePath",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "withHeaders": false
                    }
                },
                {
                    "name": "VectorAssemblerComponent",
                    "id": 2,
                    "type": "VectorAssemblerComponent",
                    "parents": [{"parent": 1, "output": 0}],
                    "arguments" : {
                        "excludeCols": ["c0"],
                        "outputCol": "features"
                    }
                },
                {
                    "name": "GBT Classification algorithm (sparkML)",
                    "id": 3,
                    "type": "ReflexGBTML",
                    "arguments" : {
                        "tempSharedPath": "file:///tmp",
                        "maxDepth": 3,
                        "significantFeaturesNumber": 50,
                        "maxBins": 3,
                        "maxIter": 50,
                        "featuresCol": "features",
                        "predictionCol": "predictions",
                        "labelCol": "c0"
                    },
                    "parents": [{"parent": 2, "output": 0}]
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)
  }


  "Good Pipeline 22 - Spark Random Forest Regression (SPARKML)" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM2.txt").getPath

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 22 canceled!")
      cancel
    }


    val json =
      s"""
           {
            "name" : "ReflexPipeline_RFR_Class",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "$outTmpFilePath",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "withHeaders": false
                    }
                },
                {
                    "name": "VectorAssemblerComponent",
                    "id": 2,
                    "type": "VectorAssemblerComponent",
                    "parents": [{"parent": 1, "output": 0}],
                    "arguments" : {
                        "excludeCols": ["c0"],
                        "outputCol": "features"
                    }
                },
                {
                    "name": "RF Regression algorithm (sparkML)",
                    "id": 3,
                    "type": "ReflexRandomForestRegML",
                    "arguments" : {
                        "tempSharedPath": "file:///tmp",
                        "maxDepth": 3,
                        "num-classes": 5,
                        "num-trees": 4,
                        "maxDepth": 3,
                        "featureSubsetStrategy": "auto",
                        "significantFeaturesNumber": 5,
                        "maxBins": 3,
                        "featuresCol": "features",
                        "predictionCol": "predictions",
                        "labelCol": "c0"
                    },
                    "parents": [{"parent": 2, "output": 0}]
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)
  }


  "Good Pipeline 23 - Spark RF Class (SPARKML) with ChiSqSelector" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM2.txt").getPath

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 23 canceled!")
      cancel
    }


    val json =
      s"""
           {
            "name" : "ReflexPipeline_RF_Class_ChiSqSelector",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "$outTmpFilePath",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "withHeaders": false
                    }
                },
                {
                    "name": "VectorAssemblerComponent",
                    "id": 2,
                    "type": "VectorAssemblerComponent",
                    "parents": [{"parent": 1, "output": 0}],
                    "arguments" : {
                         "excludeCols": ["c0"],
                         "outputCol": "featuresIn"
                    }
                },
                {
                    "name": "ChiSqSelectorComponent",
                    "id": 3,
                    "type": "ChiSqSelectorComponent",
                    "parents": [{"parent": 2, "output": 0}],
                    "arguments" : {
                        "featuresCol": "featuresIn",
                        "labelCol": "c0",
                        "numTopFeatures": 3,
                        "selectorType": "numTopFeatures",
                        "outputCol": "features"
                    }
                },
                {
                    "name": "RF algorithm (sparkML)",
                    "id": 4,
                    "type": "ReflexRandomForestML",
                    "arguments" : {
                        "num-classes": 5,
                        "num-trees": 4,
                        "maxDepth": 3,
                        "maxBins": 1000,
                        "impurity": "gini",
                        "categorialFeatureInfo": 0,
                        "significantFeaturesNumber": 5,
                        "featureSubsetStrategy": "auto",
                        "featuresCol": "features",
                        "labelCol": "c0"
                    },
                    "parents": [{"parent": 3, "output": 0}]
                }
              ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)
  }


  "Good Pipeline 24 - Spark RF Class (SPARKML) with Bucketizer" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM2.txt").getPath

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 24 canceled!")
      cancel
    }


    val json =
      s"""
           {
            "name" : "ReflexPipeline_RF_Class_Bucketizer",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "$outTmpFilePath",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "withHeaders": false
                    }
                },
                {
                    "name": "BucketizerComponent",
                    "id": 2,
                    "type": "BucketizerComponent",
                    "parents": [{"parent": 1, "output": 0}],
                    "arguments" : {
                        "splits": ["-1"," 0.2"," 0.3"," 1.2"],
                        "inputCol": "c3",
                        "outputCol": "c25"
                    }
                },
                {
                    "name": "VectorAssemblerComponent",
                    "id": 3,
                    "type": "VectorAssemblerComponent",
                    "parents": [{"parent": 2, "output": 0}],
                    "arguments" : {
                         "includeCols": [" c25", "c5", "c4"],
                         "outputCol": "features"
                    }
                },
                {
                    "name": "RF algorithm (sparkML)",
                    "id": 4,
                    "type": "ReflexRandomForestML",
                    "arguments" : {
                        "num-classes": 5,
                        "num-trees": 4,
                        "maxDepth": 3,
                        "maxBins": 1000,
                        "impurity": "gini",
                        "categorialFeatureInfo": 0,
                        "featureSubsetStrategy": "auto",
                        "featuresCol": "features",
                        "labelCol": "c0"
                    },
                    "parents": [{"parent": 3, "output": 0}]
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)
  }


  "Good Pipeline 25 - Spark RF Class (SPARKML) with StringIndexer and IndexToString" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM4.csv").getPath

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 25 canceled!")
      cancel
    }


    val json =
      s"""
           {
            "name" : "ReflexPipeline_RF_Class_StringIndexer_IndexToString",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "$outTmpFilePath",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "withHeaders": false
                    }
                },
                {
                    "name": "StringIndexerComponent",
                    "id": 2,
                    "type": "StringIndexerComponent",
                    "parents": [{"parent": 1, "output": 0}],
                    "arguments" : {
                        "inputCol": "c1",
                        "outputCol": "c25"
                    }
                },
                {
                    "name": "VectorAssemblerComponent",
                    "id": 3,
                    "type": "VectorAssemblerComponent",
                    "parents": [{"parent": 2, "output": 0}],
                    "arguments" : {
                         "excludeCols": ["c0","c1"],
                         "outputCol": "features"
                    }
                },
                {
                    "name": "IndexToStringComponent",
                    "id": 4,
                    "type": "IndexToStringComponent",
                    "parents": [{"parent": 3, "output": 0}],
                    "arguments" : {
                        "labels": ["0.0", " 0.1", "0.2", " 0.3", "0.4", "0.5", "0.6", "0.7", "0.8", "0.9", "1.0"],
                        "inputCol": "c25",
                        "outputCol": "c26"
                    }
                },
                {
                    "name": "StringIndexerComponent",
                    "id": 5,
                    "type": "StringIndexerComponent",
                    "parents": [{"parent": 4, "output": 0}],
                    "arguments" : {
                        "inputCol": "c26",
                        "outputCol": "c27"
                    }
                 },
                {
                    "name": "RF algorithm (sparkML)",
                    "id": 6,
                    "type": "ReflexRandomForestML",
                    "arguments" : {
                        "num-classes": 5,
                        "num-trees": 4,
                        "maxDepth": 3,
                        "maxBins": 1000,
                        "impurity": "gini",
                        "categorialFeatureInfo": 0,
                        "featureSubsetStrategy": "auto",
                        "featuresCol": "features",
                        "labelCol": "c27"
                    },
                    "parents": [{"parent": 5, "output": 0}]
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)
  }

  "Good Pipeline 26 - pipeline with health component" should "be valid end to end" in {
    val modelForRfPath = getClass.getResource("/modelForRf.gz").getPath


    val healthStr1 = Json(DefaultFormats).write(Map[String, Any]("name" -> "TestHealthForSpark", "data" -> "\"0,1,3,4\""))
    val healthStr2 = Json(DefaultFormats).write(Map[String, Any]("name" -> "TestHealthForSpark", "data" -> "\"0,1,3,4\""))

    val tmpFilePath = "/tmp/prediction"

    var modelForRf = scala.io.Source.fromFile(modelForRfPath,"ISO-8859-1").mkString

    val inputs = List(ReflexEvent(EventType.Model, None, ByteString.copyFrom(modelForRf.map(_.toByte).toArray), Some("1234")),
      ReflexEvent(EventType.MLHealthModel, None, ByteString.copyFrom(healthStr1.map(_.toByte).toArray), None),
      ReflexEvent(EventType.MLHealthModel, None, ByteString.copyFrom(healthStr2.map(_.toByte).toArray), None))
    val writeServer = new HealthEventWriteReadSocketServer(inputs)
    val readServer = new HealthEventWriteReadSocketServer(null, writeMode = false)

    val writeServerThread = new Thread {
      override def run(): Unit = {
        writeServer.run
      }
    }

    val readServerThread = new Thread {
      override def run(): Unit = {
        readServer.run
      }
    }

    val sourcePort = writeServer.port
    val sinkPort = readServer.port

    writeServerThread.start()
    readServerThread.start()

    val json =
      s"""
           {
            "name" : "ReflexPipeline_RF",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "",
                    "mlObjectSocketHost": "localhost",
                    "mlObjectSocketSourcePort": $sourcePort,
                    "mlObjectSocketSinkPort": $sinkPort,
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "TestHealthComponent",
                    "id": 1,
                    "type": "TestHealthComponent",
                    "arguments" : {
                        "enableValidation": true,
                        "enablePerformance": true,
                        "labelCol": "c0"
                    }
                },
                {
                  "name": "SaveToFile",
                  "id": 2,
                   "type": "SaveToFile",
                   "parents": [
                   {
                      "parent": 1,
                      "output": 0
                    }
                    ],
                     "arguments": {
                      "filepath" : "$tmpFilePath"
                     }
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--test-mode",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)

    // since it is now writing to directory, we need better way to check files content
    //    val lines = scala.io.Source.fromFile(tmpFilePath).mkString.split('\n')

    //    assert(lines.length == 2)
    removePathIfExists(tmpFilePath)
  }

  "Good Pipeline 27 - health turned off" should "be valid end to end" in {
    val modelForRfPath = getClass.getResource("/modelForRf.gz").getPath


    val healthStr1 = Json(DefaultFormats).write(Map[String, Any]("name" -> "TestHealthForSpark", "data" -> "\"0,1,3,4\""))
    val healthStr2 = Json(DefaultFormats).write(Map[String, Any]("name" -> "TestHealthForSpark", "data" -> "\"0,1,3,4\""))

    val tmpFilePath = "/tmp/prediction"

    var modelForRf = scala.io.Source.fromFile(modelForRfPath,"ISO-8859-1").mkString

    val inputs = List(ReflexEvent(EventType.Model, None, ByteString.copyFrom(modelForRf.map(_.toByte).toArray), Some("1234")),
      ReflexEvent(EventType.MLHealthModel, None, ByteString.copyFrom(healthStr1.map(_.toByte).toArray), None),
      ReflexEvent(EventType.MLHealthModel, None, ByteString.copyFrom(healthStr2.map(_.toByte).toArray), None))
    val writeServer = new HealthEventWriteReadSocketServer(inputs)
    val readServer = new HealthEventWriteReadSocketServer(null, writeMode = false)

    val writeServerThread = new Thread {
      override def run(): Unit = {
        writeServer.run
      }
    }

    val readServerThread = new Thread {
      override def run(): Unit = {
        readServer.run
      }
    }

    val sourcePort = writeServer.port
    val sinkPort = readServer.port

    writeServerThread.start()
    readServerThread.start()

    val json =
      s"""
           {
            "name" : "ReflexPipeline_RF",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "",
                    "mlObjectSocketHost": "localhost",
                    "mlObjectSocketSourcePort": $sourcePort,
                    "mlObjectSocketSinkPort": $sinkPort,
                    "healthStatFilePath": "/tmp/tmpHealthFile",
                    "enableHealth": false
                 },
            "pipe" : [
                {
                    "name": "TestHealthComponent",
                    "id": 1,
                    "type": "TestHealthComponent",
                    "arguments" : {
                    }
                },
                {
                  "name": "SaveToFile",
                  "id": 2,
                   "type": "SaveToFile",
                   "parents": [
                   {
                      "parent": 1,
                      "output": 0
                    }
                    ],
                     "arguments": {
                      "filepath" : "$tmpFilePath"
                     }
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--test-mode",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)

    //sink does create file path. But it contains no message in it!
    removePathIfExists(tmpFilePath)
  }


  "Good Pipeline 28 - Spark DT Reg (SPARKML)" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM2.txt").getPath

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 26 canceled!")
      cancel
    }

    val json =
      s"""
           {
            "name" : "ReflexPipeline_DT_Reg",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "$outTmpFilePath",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "withHeaders": false
                    }
                },
                {
                    "name": "VectorAssemblerComponent",
                    "id": 2,
                    "type": "VectorAssemblerComponent",
                    "parents": [{"parent": 1, "output": 0}],
                    "arguments" : {
                         "excludeCols": ["c0"],
                         "outputCol": "features"
                    }
                },
                {
                    "name": "DT Regression algorithm (sparkML)",
                    "id": 3,
                    "type": "ReflexDTRegML",
                    "arguments" : {
                        "tempSharedPath": "file:///tmp",
                        "maxDepth": 3,
                        "maxBins": 3,
                        "featuresCol": "features",
                        "predictionCol": "predictions",
                        "varianceCol": "variance",
                        "labelCol": "c0"
                    },
                    "parents": [{"parent": 2, "output": 0}]
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)
  }


  "Good Pipeline 29 - Spark DT Class (SPARKML)" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM2.txt").getPath

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 27 canceled!")
      cancel
    }


    val json =
      s"""
           {
            "name" : "ReflexPipeline_DT_Class",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "$outTmpFilePath",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "$rfSampleDataPath",
                        "withHeaders": false
                    }
                },
                {
                    "name": "VectorAssemblerComponent",
                    "id": 2,
                    "type": "VectorAssemblerComponent",
                    "parents": [{"parent": 1, "output": 0}],
                    "arguments" : {
                         "excludeCols": ["c0"],
                         "outputCol": "features"
                    }
                },
                {
                    "name": "DT Classification algorithm (sparkML)",
                    "id": 3,
                    "type": "ReflexDTML",
                    "arguments" : {
                        "tempSharedPath": "file:///tmp",
                        "maxDepth": 3,
                        "maxBins": 3,
                        "impurity": "gini",
                        "featuresCol": "features",
                        "significantFeaturesNumber": 5,
                        "predictionCol": "predictions",
                        "labelCol": "c0"
                    },
                    "parents": [{"parent": 2, "output": 0}]
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    noException should be thrownBy DagGen.main(args)
  }

  "Good Pipeline 32 - Spark DT Reg (SPARKML) with StringIndexer and oneHotEnc" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM4.csv").getPath()

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 32 canceled!")
      cancel
    }
    println("DT Reg input data sample path: " + rfSampleDataPath)


    val json =
      s"""
           {
            "name" : "ReflexPipeline_DT_Reg_StringIndexer_OneHot",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "${outTmpFilePath}",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "${rfSampleDataPath}",
                        "withHeaders": false
                    }
                },
                {
                 "name": "StringIndexerComponent",
                 "id": 2,
                 "type": "StringIndexerComponent",
                 "parents": [{"parent": 1, "output": 0}],
                 "arguments" : {
                 "inputCol": "c1",
                 "outputCol": "c25"
                 }
                 },
                 {
                 "name": "OneHotEncoderComponent",
                 "id": 3,
                 "type": "OneHotEncoderComponent",
                 "parents": [{"parent": 2, "output": 0}],
                 "arguments" : {
                 "inputCol": "c25",
                 "outputCol": "c26"
                 }
                 },
                {
                    "name": "VectorAssemblerComponent",
                    "id": 4,
                    "type": "VectorAssemblerComponent",
                    "parents": [{"parent": 3, "output": 0}],
                    "arguments" : {
                         "includeCols": ["c10", "c23", "c14", "c7"],
                         "outputCol": "features2"
                    }
                },
                {
                    "name": "VectorIndexerComponent",
                    "id": 5,
                    "type": "VectorIndexerComponent",
                    "parents": [{"parent": 4, "output": 0}],
                    "arguments" : {
                         "inputCol": "features2",
                          "maxCategories": 200,
                         "outputCol": "features22"
                    }
                },
                {
                    "name": "VectorAssemblerComponent",
                    "id": 6,
                    "type": "VectorAssemblerComponent",
                    "parents": [{"parent": 5, "output": 0}],
                    "arguments" : {
                         "excludeCols": ["c0", "c25", "c1", "c2", "c3", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16", "c17", "c18", "c19", "c20", "c21", "c22", "c23", "c24", "features2", "features22"],
                         "outputCol": "features1"
                    }
                },
                {
                    "name": "VectorAssemblerComponent",
                    "id": 7,
                    "type": "VectorAssemblerComponent",
                    "parents": [{"parent": 6, "output": 0}],
                    "arguments" : {
                         "includeCols": ["features1", "features22"],
                         "outputCol": "features"
                    }
                },
                {
                    "name": "DT Regression algorithm (sparkML)",
                    "id": 8,
                    "type": "ReflexDTRegML",
                    "arguments" : {
                     "tempSharedPath": "file:///tmp",
                     "maxDepth": 3,
                     "maxBins": 300,
                      "featuresCol": "features",
                      "significantFeaturesNumber": 8,
                       "predictionCol": "predictions",
                       "varianceCol": "variance",
                        "labelCol": "c0"
                    },
                    "parents": [{"parent": 7, "output": 0}]
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000")
    noException should be thrownBy DagGen.main(args)
  }

  "Good Pipeline 33 - Sink Dataframe to file" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM4.csv").getPath()
    val tmpFilePath = "/tmp/prediction"

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 33 canceled!")
      cancel
    }
    println("DT Reg input data sample path: " + rfSampleDataPath)


    val json =
      s"""
           {
            "name" : "ReflexPipeline_DT_Reg_StringIndexer_OneHot",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "${outTmpFilePath}",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "${rfSampleDataPath}",
                        "withHeaders": false
                    }
                },
                {
                    "name": "StringIndexerComponent",
                    "id": 2,
                    "type": "StringIndexerComponent",
                    "parents": [{"parent": 1, "output": 0}],
                    "arguments" : {
                        "inputCol": "c1",
                        "outputCol": "c25"
                    }
                },
                {
                    "name": "DFtoFile",
                    "id": 5,
                    "type": "DFtoFile",
                    "parents": [
                    {
                        "parent": 2,
                        "output": 0
                    }],
                    "arguments": {
                        "filepath" : "$tmpFilePath",
                        "withHeaders": true,
                        "separator": ","
                    }
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000")
    noException should be thrownBy DagGen.main(args)
    removePathIfExists(tmpFilePath)
  }

  "Good Pipeline 34 - Sink Dataframe with specific columns to file " should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM4.csv").getPath()
    val tmpFilePath = "/tmp/prediction"

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 34 canceled!")
      cancel
    }
    println("DT Reg input data sample path: " + rfSampleDataPath)


    val json =
      s"""
           {
            "name" : "ReflexPipeline_DT_Reg_StringIndexer_OneHot",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "${outTmpFilePath}",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "${rfSampleDataPath}",
                        "withHeaders": false
                    }
                },
                {
                    "name": "StringIndexerComponent",
                    "id": 2,
                    "type": "StringIndexerComponent",
                    "parents": [{"parent": 1, "output": 0}],
                    "arguments" : {
                        "inputCol": "c1",
                        "outputCol": "c25"
                    }
                },
                {
                    "name": "DFtoFile",
                    "id": 5,
                    "type": "DFtoFile",
                    "parents": [
                    {
                        "parent": 2,
                        "output": 0
                    }],
                    "arguments": {
                        "filepath" : "$tmpFilePath",
                        "withHeaders": true,
                        "separator": ",",
                        "includeCols":["c1","c3","c5","c7", "nonExistingColumn"]
                    }
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000")
    noException should be thrownBy DagGen.main(args)
    removePathIfExists(tmpFilePath)
  }

  "Bad Pipeline 35 - training kmeans" should "throw Exception with generic message" in {

    val rfSampleDataPath = getClass.getResource("/testSVM4.csv").getPath()
    val tmpFilePath = "/tmp/kmeans-model"

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 35 canceled!")
      cancel
    }
    println("Kmeans input data sample path: " + rfSampleDataPath)

    val unexistingComponent = "UnexistingComponent"

    val json =
      s"""
        {
           "engineType": "SparkBatch",
           "name": "training_kmeans_spark",
           "pipe": [{
             "parents": [],
             "id": 0,
             "name": "CsvToDF",
             "type": "CsvToDF",
             "arguments": {
               "filepath": "/data-lake/jenkins-data/GE_DEMO/Train_dataset_A.csv",
               "withHeaders": false,
               "separator": ","
             }
           }, {
             "parents": [{
               "input": 0,
               "output": 0,
               "parent": 0
             }],
             "id": 1,
             "name": "VectorAssemblerComponent",
             "type": "VectorAssemblerComponent",
             "arguments": {
               "outputCol": "features"
             }
           }, {
             "parents": [{
               "input": 0,
               "output": 0,
               "parent": 1
             }],
             "id": 2,
             "name": "ReflexKmeansML",
             "type": "ReflexKmeansML",
             "arguments": {
               "k": 3,
               "featuresCol": "features",
               "predictionCol": "predictions",
               "initStep": 2,
               "initMode": "k-means",
               "maxIter": 300,
               "tol": 1e-05,
               "seed": 1
             }
           }, {
             "parents": [],
             "id": 3,
             "name": "${unexistingComponent}",
             "type": "${unexistingComponent}",
             "arguments": {
               "className": "org/apache/spark/rdd/HadoopRDD.class"
             }
           }],
           "systemConfig": {
             "statsDBHost": "localhost",
             "statsDBPort": 8086,
             "statsMeasurementID": "predictions-542394da-3918-4939-abda-1a3393cadc21",
             "mlObjectSocketHost": "0.0.0.0",
             "mlObjectSocketSourcePort": 37136,
             "mlObjectSocketSinkPort": 33990,
             "modelFileSinkPath": "${outTmpFilePath}",
             "modelFileSourcePath": "/tmp/eco//542394da-3918-4939-abda-1a3393cadc21/input-model-a0ad0a5d-0c93-4669-8c8c-e34ca87d506e",
             "healthStatFilePath": "/tmp/eco//542394da-3918-4939-abda-1a3393cadc21/health-22c4bd1e-de92-47ec-a197-fa2fd8009c7c",
             "workflowInstanceId": "",
             "socketSourcePort": 0,
             "socketSinkPort": 0,
             "enableHealth": true,
             "canaryThreshold": -1.0
           }
         }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000")
    val ex = intercept[Exception] {
      DagGen.main(args)
    }
    removePathIfExists(tmpFilePath)
    assert(ex.getMessage.contains(s"Error: component '${unexistingComponent}' is not supported by 'SparkBatch' engine"))
  }

  "Good Pipeline 36 - Split dataframe" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM4.csv").getPath()
    val tmpFilePath1 = "/tmp/prediction1"
    val tmpFilePath2 = "/tmp/prediction2"

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 36 canceled!")
      cancel
    }
    println("DT Reg input data sample path: " + rfSampleDataPath)


    val json =
      s"""
           {
            "name" : "ReflexPipeline_DT_Reg_StringIndexer_OneHot",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "modelSocketSinkPort": 7777,
                    "modelSocketSourcePort": 7777,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID":"1",
                    "modelFileSinkPath": "${outTmpFilePath}",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "Read data from CSV (txt format)",
                    "id": 1,
                    "type": "CsvToDF",
                    "parents": [],
                    "arguments" : {
                        "filepath": "${rfSampleDataPath}",
                        "withHeaders": false
                    }
                },
                {
                    "name": "DFSplit",
                    "id": 2,
                    "type": "DFSplit",
                    "parents": [{"parent": 1, "output": 0}],
                    "arguments" : {
                        "splitRatio": 0.8
                    }
                },
                {
                    "name": "DFtoFile",
                    "id": 3,
                    "type": "DFtoFile",
                    "parents": [
                    {
                        "parent": 2,
                        "output": 0
                    }],
                    "arguments": {
                        "filepath" : "$tmpFilePath1",
                        "withHeaders": true,
                        "separator": ","
                    }
                },
                {
                    "name": "DFtoFile",
                    "id": 4,
                    "type": "DFtoFile",
                    "parents": [
                    {
                        "parent": 2,
                        "output": 1
                    }],
                    "arguments": {
                        "filepath" : "$tmpFilePath2",
                        "withHeaders": true,
                        "separator": ","
                    }
                }
             ]
           }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000")
    noException should be thrownBy DagGen.main(args)
    removePathIfExists(tmpFilePath1)
    removePathIfExists(tmpFilePath2)
  }

  "Good Pipeline 37 - logistic regression with split" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM2.txt").getPath()

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 37 canceled!")
      cancel
    }
    println("DT Reg input data sample path: " + rfSampleDataPath)

    val json =
      s"""
         {
             "name": "00_classification_training",
             "engineType": "SparkBatch",
             "systemConfig" : {
                                 "statsDBHost": "localhost",
                                 "statsDBPort": 8086,
                                 "modelSocketSinkPort": 7777,
                                 "modelSocketSourcePort": 7777,
                                 "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                                 "statsMeasurementID":"1",
                                 "modelFileSinkPath": "${outTmpFilePath}",
                                 "healthStatFilePath": "/tmp/tmpHealthFile"
                              },
             "pipe": [
                 {
                     "id": 0,
                     "name": "CsvToDF",
                     "type": "CsvToDF",
                     "parents": [

                ],
                     "arguments": {
                         "separator": ",",
                         "filepath": "${rfSampleDataPath}",
                         "withHeaders": false,
                         "ignoreLeadingWhiteSpace": true,
                         "ignoreTrailingWhiteSpace": true
                     }
                 },
                 {
                    "id": 1,
                     "name": "DFSplit",
                     "type": "DFSplit",
                     "parents": [
                     {
                              "parent": 0,
                              "output": 0,
                              "input": 0
                         }
                   ],
                    "arguments": {
                           "splitRatio": 0.8
                      }
                 },
                 {
                     "id": 2,
                     "name": "VectorAssemblerComponent",
                     "type": "VectorAssemblerComponent",
                     "parents": [
                         {
                             "parent": 1,
                             "output": 0,
                             "input": 0
                         }
                     ],
                     "arguments": {
                         "excludeCols": [
                             "c0"
                         ],
                         "outputCol": "features"
                     }
                 },
                 {
                     "id": 3,
                     "name": "ReflexLogisticRegression",
                     "type": "ReflexLogisticRegression",
                     "parents": [
                         {
                             "parent": 2,
                             "output": 0,
                             "input": 0
                         },
                         {
                             "parent": 1,
                             "output": 1,
                             "input": 1
                         }
                     ],
                     "arguments": {
                         "tempSharedPath": "file:///tmp",
                         "elasticNetParam": 0,
                         "family": "auto",
                         "fitIntercept": true,
                         "maxIter": 100,
                         "regParam": 0,
                         "standardization": true,
                         "tolerance": 0.000001,
                         "labelCol": "c0",
                         "featuresCol": "features"
                     }
                 }
             ]
         }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000")
    noException should be thrownBy DagGen.main(args)
  }

  "Good Pipeline 38 - logistic regression with Duplicate" should "be valid end to end" in {

    val rfSampleDataPath = getClass.getResource("/testSVM2.txt").getPath()

    if (!File(rfSampleDataPath).exists) {
      println("Good Pipeline 37 canceled!")
      cancel
    }
    println("DT Reg input data sample path: " + rfSampleDataPath)

    val json =
      s"""
         {
             "name": "00_classification_training",
             "engineType": "SparkBatch",
             "systemConfig" : {
                                 "statsDBHost": "localhost",
                                 "statsDBPort": 8086,
                                 "modelSocketSinkPort": 7777,
                                 "modelSocketSourcePort": 7777,
                                 "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                                 "statsMeasurementID":"1",
                                 "modelFileSinkPath": "${outTmpFilePath}",
                                 "healthStatFilePath": "/tmp/tmpHealthFile"
                              },
             "pipe": [
                 {
                     "id": 0,
                     "name": "CsvToDF",
                     "type": "CsvToDF",
                     "parents": [

                ],
                     "arguments": {
                         "separator": ",",
                         "filepath": "${rfSampleDataPath}",
                         "withHeaders": false,
                         "ignoreLeadingWhiteSpace": true,
                         "ignoreTrailingWhiteSpace": true
                     }
                 },
                 {
                    "id": 1,
                     "name": "TwoDup",
                     "type": "TwoDup",
                     "parents": [
                         {
                              "parent": 0,
                              "output": 0,
                              "input": 0
                         }
                      ]
                 },
                 {
                     "id": 2,
                     "name": "VectorAssemblerComponent",
                     "type": "VectorAssemblerComponent",
                     "parents": [
                         {
                             "parent": 1,
                             "output": 0,
                             "input": 0
                         }
                     ],
                     "arguments": {
                         "excludeCols": [
                             "c0"
                         ],
                         "outputCol": "featuresIn"
                     }
                 },
                 {
                     "id": 3,
                     "name": "VectorIndexerComponent",
                     "type": "VectorIndexerComponent",
                     "parents": [
                         {
                             "parent": 2,
                             "output": 0,
                             "input": 0
                         }
                     ],
                     "arguments": {
                         "inputCol": "featuresIn",
                         "outputCol": "features",
                         "maxCategories": 20
                     }
                 },
                 {
                     "id": 4,
                     "name": "ReflexLogisticRegression",
                     "type": "ReflexLogisticRegression",
                     "parents": [
                         {
                             "parent": 3,
                             "output": 0,
                             "input": 0
                         },
                         {
                             "parent": 1,
                             "output": 1,
                             "input": 1
                         }
                     ],
                     "arguments": {
                         "tempSharedPath": "file:///tmp",
                         "elasticNetParam": 0,
                         "family": "auto",
                         "fitIntercept": true,
                         "maxIter": 100,
                         "regParam": 0,
                         "standardization": true,
                         "tolerance": 0.000001,
                         "labelCol": "c0",
                         "featuresCol": "features"
                     }
                 }
             ]
         }
       """.stripMargin

    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000")
    noException should be thrownBy DagGen.main(args)
  }

  "Building continuous histogram for data" should "be valid end to end" in {

    val dataHistPath = getClass.getResource("/dataHist.txt").getPath

    if (!File(dataHistPath).exists) {
      println("Pipeline Histogram From DataFrame!")
      cancel
    }

    val sparkSession = SparkSession.builder.
      master("local[*]").appName("Good Histograms").getOrCreate()

    val df = sparkSession.sqlContext.read
      .option("inferSchema", value = true)
      .option("header", true)
      .csv(dataHistPath)

    val genericHealthCreator = new ContinuousHistogramForSpark(HealthType.ContinuousHistogramHealth.toString)

    genericHealthCreator.dfOfDenseVector = df
    // by setting spark to none, we ensure the system attempt to determine categorical and continuous
    genericHealthCreator.sparkMLModel = None
    genericHealthCreator.binSizeForEachFeatureForRef = None
    genericHealthCreator.minBinValueForEachFeatureForRef = None
    genericHealthCreator.maxBinValueForEachFeatureForRef = None
    genericHealthCreator.enableAccumOutputOfHistograms = true
    genericHealthCreator.sparkContext = sparkSession.sparkContext

    val hist = genericHealthCreator.createHealth().get

    hist.keySet.size
    assert(hist.nonEmpty && hist.size == 3 && hist.contains("B") &&
      hist.contains("F") && hist.contains("L"))

    sparkSession.stop()
  }
}
