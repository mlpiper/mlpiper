package com.parallelmachines.reflex.test.reflexpipeline

import java.io.File
import java.nio.file.{Files, Paths}

import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponentFactory
import org.junit.runner.RunWith
import org.mlpiper.infrastructure._
import org.mlpiper.infrastructure.factory.{ByClassComponentFactory, ComponentJSONSignatureParser, ReflexComponentFactory}
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

@RunWith(classOf[JUnitRunner])
class ReflexPipelineTest extends FlatSpec with Matchers {

  private val logger = LoggerFactory.getLogger(getClass)

  DagTestUtil.initComponentFactory()

  override def withFixture(test: NoArgTest) = {
    DagTestUtil.initComponentFactory()

    try super.withFixture(test)
    finally {
      // nothing for now
    }
  }

  "Empty DagGen args" should "throw an exception" in {
    val args = Array[String]()
    intercept[Exception] {
      DagGen.main(args)
    }
  }

  "Components json generation" should "be valid end to end" in {
    val componentsFile = java.io.File.createTempFile("components", ".json")
    componentsFile.deleteOnExit()

    val componentsDir = DagTestUtil.getComponentsDir

    val args = Array[String]("--comp-desc", s"${componentsFile.getAbsolutePath}", "--external-comp", s"$componentsDir")
    noException should be thrownBy DagGen.main(args)
    assert(componentsFile.length() != 0)
  }
  "Good Pipeline 1" should "be valid" in {

    val json1 =
      """
        {
        "name" : "ReflexPipeline_1",
        "engineType": "FlinkStreaming",
        "systemConfig" : {
          "statsDBHost": "localhost",
          "statsDBPort": 8086,
          "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
          "statsMeasurementID": "1",
          "modelFileSinkPath": "/tmp/tmpFile"
        },
        "pipe" : [
           {
                 "name": "data parser",
                 "id": 33,
                 "type": "ReflexStringToBreezeVectorComponent",
                 "parents": [{"parent": 22, "output" : 0}],
                 "arguments" : {
                     "separator": "colon",
                     "attributes": 1,
                     "indicesRange": "1"
                 }
             },
             {
                "name": "Turbine general input",
                "id": 22,
                "type": "ReflexSocketConnector",
                "parents": [],
                "arguments" : {
                    "socketSourcePort": 9900,
                    "socketSourceHost": "localhost"
                }
            },
            {
                "name": "printer",
                "id": 44,
                "type": "ReflexNullConnector",
                "parents": [{"parent": 33, "output" : 0}]
            }
            ]
        }
        """

    val res = DagTestUtil.parseGenerateValidate(json1)
    assert(res != null, "json1 is not valid")
    assert(res.nodeList.length == 3, "wrong number of nodes in DAG after parsing")
    assert(res.pipeInfo.getComponentById(33).isDefined, "component with id = 33 does not exist")
    assert(res.pipeInfo.getComponentById(33).get.name == "data parser", "wrong component name, 'data parser' expected")
    assert(res.pipeInfo.getComponentById(1) == None, "component with id = 1 should not exist")
  }

  "Good Pipeline 1 extended with API" should "be valid" in {

    val json1 =
      """
        {
        "name" : "ReflexPipeline_1",
        "engineType": "FlinkStreaming",
        "systemConfig" : {
          "statsDBHost": "localhost",
          "statsDBPort": 8086,
          "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
          "statsMeasurementID": "1",
          "modelFileSinkPath": "/tmp/tmpFile"
        },
        "pipe" : [
           {
                 "name": "data parser",
                 "id": 11,
                 "type": "ReflexStringToBreezeVectorComponent",
                 "parents": [{"parent": 22, "output" : 0}],
                 "arguments" : {
                     "separator": "colon",
                     "attributes": 1,
                     "indicesRange": "1"
                 }
             }
            ]
        }
        """.stripMargin

    val jsonParser = new ReflexPipelineJsonParser()
    var pipeInfo = jsonParser.parsePipelineInfo(json1)
    assert(pipeInfo.getMaxId == 11, "json1 is not valid")
    pipeInfo.addComponent(Component("Turbine general input", 22, "ReflexSocketConnector", ListBuffer[Parent](), Some(Map[String, Any]("socketSourcePort" -> 9900, "socketSourceHost" -> "localhost"))))
    pipeInfo.addComponent(Component("printer", 44, "ReflexNullConnector", ListBuffer[Parent](Parent(11, 0, None, None, None)), None))

    val res = new ReflexPipelineBuilder().buildPipelineFromInfo(pipeInfo)
    assert(res.nodeList.size == 3, "json1 is not valid")
  }

  "Good Pipeline 1 created with API" should "be valid" in {

    val json1 =
      """
        {"name":"TestPipe",
        "engineType":"FlinkStreaming",
        "language":"Python",
        "systemConfig":{
          "statsDBHost":"localhost",
          "statsDBPort":8086,
          "workflowInstanceId":"8117aced55d7427e8cb3d9b82e4e26ac",
          "statsMeasurementID":"1",
          "modelFileSinkPath":"/tmp/tmpFile",
          "mlObjectSocketHost":"localhost",
          "mlObjectSocketSinkPort":12345
        },
        "pipe":[
             {
                "name":"General input",
                "id":1,
                "type":"ReflexSocketConnector",
                "parents": [],
                "arguments":{
                    "socketSourcePort":9900,
                    "socketSourceHost":"localhost"
                }
            },
            {
                 "name":"Data parser",
                 "id":2,
                 "type":"ReflexStringToBreezeVectorComponent",
                 "parents":[{"parent": 1, "output" : 0, "input": 0}],
                 "arguments":{
                 "separator":"colon",
                 "attributes":1,
                 "indicesRange":"1"
                 }
            },
            {
                 "name":"TestVectorToString",
                 "id":3,
                 "type":"TestVectorToString",
                 "parents":[{"parent": 2, "output" : 0, "input": 0}]
            },
            {
                 "name":"Auto Generated Singleton",
                 "id":4,
                 "type":"EventSocketSink",
                 "parents":[{"parent": 3, "output": 0, "eventType":"Alert", "input" : 0, "eventLabel":"custom_label"}]
            }
        ]
        }
        """.stripMargin

    val flinkStreamingInfo = ReflexComponentFactory.getEngineFactory(ComputeEngineType.FlinkStreaming).asInstanceOf[ByClassComponentFactory]
    flinkStreamingInfo.registerComponent(classOf[com.parallelmachines.reflex.test.components.TestVectorToString])

    val res1 = new ReflexPipelineBuilder().buildPipelineFromJson(json1)
    assert(res1.nodeList.size == 4, "json1 is not valid")
    val pipeInfo1 = res1.pipeInfo


    val pipeInfo2 = ReflexPipelineInfo()
    val systemConf = ReflexSystemConfig()

    systemConf.statsDBHost = "localhost"
    systemConf.statsDBPort = 8086
    systemConf.workflowInstanceId = "8117aced55d7427e8cb3d9b82e4e26ac"
    systemConf.statsMeasurementID = "1"
    systemConf.modelFileSinkPath = "/tmp/tmpFile"
    systemConf.mlObjectSocketHost = Some("localhost")
    systemConf.mlObjectSocketSinkPort = Some(12345)

    pipeInfo2.name = "TestPipe"
    pipeInfo2.language = Some("Python")
    pipeInfo2.engineType = ComputeEngineType.FlinkStreaming
    pipeInfo2.systemConfig = systemConf

    pipeInfo2.addComponent(Component("General input", 1, "ReflexSocketConnector", ListBuffer[Parent](), Some(Map[String, Any]("socketSourceHost" -> "localhost", "socketSourcePort" -> 9900))))
    pipeInfo2.addComponent(Component("Data parser", 2, "ReflexStringToBreezeVectorComponent", ListBuffer[Parent](new Parent(1, 0, Some(0), None, None)),
      Some(Map[String, Any]("separator" -> "colon", "attributes" -> 1, "indicesRange" -> "1"))))
    pipeInfo2.addComponent(Component("TestVectorToString", 3, "TestVectorToString", ListBuffer[Parent](new Parent(2, 0, Some(0), None, None)), None))
    pipeInfo2.addComponent(Component("Test Socket Sink", 4, "EventSocketSink", ListBuffer[Parent](new Parent(3, 0, Some(0), Some("Alert"), Some("custom_label"))), None))

    val res2 = new ReflexPipelineBuilder().buildPipelineFromInfo(pipeInfo2)
    assert(res2.nodeList.size == 4, "pipeInfo2 is not valid")

    /* EventSocketSink in initial json has id == 4.
     * It will be replaced with id == 5,
     * so filter out '4's and '5's to compare strings */
    assert(json1.filter(_ > ' ').filter(_ != '4').filter(_ != '5').sorted == pipeInfo1.toJson().filter(_ > ' ').filter(_ != '4').filter(_ != '5').sorted)
    assert(pipeInfo1.toJson().sorted == pipeInfo2.toJson().sorted)
  }

  "Good Pipeline to check PipelineInfo API" should "be valid" in {
    val flinkStreamingInfo =
      ReflexComponentFactory
        .getEngineFactory(ComputeEngineType.FlinkStreaming)
        .asInstanceOf[ByClassComponentFactory]

    flinkStreamingInfo.registerComponent(classOf[com.parallelmachines.reflex.test.components.TestAlgoComponentWithModel])
    val json1 =
      """
    {
    "name": "SVM Inference with Model Update 0.9 pipeline",
    "engineType": "FlinkStreaming",
    "systemConfig": {
      "statsDBHost": "daenerys-c17",
      "statsDBPort": 8086,
      "mlObjectSocketHost": "",
      "mlObjectSocketSinkPort": 7777,
      "mlObjectSocketSourcePort": 7777,
      "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
      "statsMeasurementID": "1",
      "modelFileSinkPath": "/tmp/model_sink",
      "healthStatSocketSourcePort": 34566
    },
    "pipe": [
    {
      "name": "Model updater",
      "id": 1,
      "type": "ReflexNullSourceConnector",
      "parents": []
    },
    {
      "name": "Kafka",
      "id": 2,
      "type": "ReflexKafkaConnector",
      "parents": [],
      "arguments": {
        "port": 9092,
        "host": "daenerys-c17",
        "topic": "SVM"
      }
    },
    {
      "name": "String to Labeled Vector",
      "id": 3,
      "type": "ReflexStringToLabeledVectorComponent",
      "parents": [
      {
        "parent": 2,
        "output": 0
      }
      ],
      "arguments": {
        "separator": ",",
        "attributes": 26,
        "labelIndex": 0,
        "timestampIndex": 25,
        "indicesRange": "1-24"
      }
    },
    {
      "name": "inference",
      "id": 4,
      "type": "TestAlgoComponentWithModel",
      "parents": [
      {
        "parent": 1,
        "output": 0,
        "input": 1
      },
      {
        "parent": 3,
        "output": 0,
        "input": 0
      }
      ]
    },
    {
      "name": "null",
      "id": 5,
      "type": "ReflexNullConnector",
      "parents": [
      {
        "parent": 4,
        "output": 0
      }
      ]
    },
    {
      "name": "null",
      "id": 6,
      "type": "ReflexNullConnector",
      "parents": [
      {
        "parent": 4,
        "output": 1
      }
      ]
    }
    ]
  }
  """

    val pipeInfo = new ReflexPipelineJsonParser().parsePipelineInfo(json1)
    val res = new ReflexPipelineBuilder().buildPipelineFromInfo(pipeInfo)

    assert(pipeInfo.getComponentsByProperties(ComponentsGroups.algorithms, isSource = false).length == 1)
    assert(pipeInfo.getComponentsByProperties(ComponentsGroups.connectors, isSource = false).length == 0)
    assert(pipeInfo.getComponentsByProperties(ComponentsGroups.connectors, isSource = true).length == 2)
    assert(pipeInfo.getComponentsByProperties(ComponentsGroups.connectors, isSource = true, Some(ConnectionGroups.DATA)).length == 2)

    val compsList = pipeInfo.getComponentsByProperties(ComponentsGroups.connectors, isSource = true, Some(ConnectionGroups.DATA))
    val args = compsList(1).getArguments
    assert(args("port") == 9092)
    assert(args("host") == "daenerys-c17")
    assert(args("topic") == "SVM")

    assert(res != null, "json1 is not valid")

    // 6
    assert(res.nodeList.length == 6, "wrong number of nodes in DAG after parsing")

    val c1 = compsList(1)
    val c2 = c1.copy(arguments = Some(Map[String, Any]("port" -> 9092, "host" -> "daenerys-c17", "topic" -> "SVM")))
    assert(c1.equalArgumentsTo(c2))
  }

  "Pipeline 1" should "throw IllegalArgumentException" in {

    val json1 =
      """
        {
        "name" : "ReflexPipeline_1",
        "engineType": "FlinkStreaming",
        "systemConfig" : {
          "statsDBHost": "localhost",
          "statsDBPort": 8086,
          "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
          "statsMeasurementID": "1",
          "modelFileSinkPath": "/tmp/tmpFile"
        },
        "pipe" : [

             {
                "name": "Turbine general input",
                "id": 22,
                "type": "ReflexSocketConnector",
                "parents": [],
                "arguments" : {
                    "socketSourcePort": 9900,
                    "socketSourceHost": "localhost"
                }
            },
            {
                 "name": "data parser",
                 "id": 33,
                 "type": "ReflexStringToBreezeVectorComponent",
                 "parents": [{"parent": 23, "output" : 0}],
                 "arguments" : {
                 "separator": "colon",
                 "attributes": 1,
                 "indicesRange": "1"
                 }
            },
            {
                "name": "printer",
                "id": 44,
                "type": "ReflexNullConnector",
                "parents": [{"parent": 33, "output" : 0}]
            }
            ]
        }
        """

    val ex = intercept[IllegalArgumentException] {
      DagTestUtil.parseGenerateValidate(json1)
    }
    assert(ex.getMessage.contains("Pipeline does not contain component id: 23"))

  }

  "Good pipeline 2" should "be valid" in {
    val json1 =
      """
        {
            "name" : "ReflexSamplePipeline",
            "engineType": "FlinkStreaming",
             "systemConfig" : {
                  "statsDBHost": "localhost",
                  "statsDBPort": 8086,
                  "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                  "statsMeasurementID": "1",
                  "modelFileSinkPath": "/tmp/tmpFile"
             },
            "pipe" : [
            {
                "name": "Turbine general input",
                "id": 1,
                "type": "ReflexKafkaConnector",
                "parents": [],
                "arguments" : {
                    "port": 9900,
                    "host": "localhost",
                    "topic": "reflex"
                }
            },
            {
                "name": "Turbine general input",
                "id": 2,
                "type": "TwoDup",
                "parents": [{"parent": 1, "output" : 0}],
                "arguments" : {
                }
            },
            {
                "name": "printer",
                "id": 3,
                "type": "ReflexNullConnector",
                "parents": [{"parent": 2, "output" : 0}]
            },
            {
                "name": "printer",
                "id": 4,
                "type": "ReflexNullConnector",
                "parents": [{"parent": 2, "output" : 1}]
            }
            ]
        }
       """

    val res = DagTestUtil.parseGenerateValidate(json1)
    assert(res != null, "json1 is not valid")
    assert(res.nodeList.length == 4, "wrong number of nodes in DAG after parsing")
  }

  "Good pipeline 3" should "be valid" in {

    val goodJson =
      """
        {
        "name" : "ReflexSamplePipeline",
        "engineType": "FlinkStreaming",
             "systemConfig" : {
                  "statsDBHost": "localhost",
                  "statsDBPort": 8086,
                  "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                  "statsMeasurementID": "1",
                  "modelFileSinkPath": "/tmp/tmpFile"
             },
        "pipe" : [
        {
            "name": "Turbine 1",
            "id": 1,
            "type": "ReflexSocketConnector",
            "parents": [],
            "arguments" : {
                "socketSourcePort": 9900,
                "socketSourceHost": "localhost"
            }
        },
        {
            "name": "Turbine 2",
            "id": 2,
            "type": "ReflexSocketConnector",
            "parents": [],
            "arguments" : {
                "socketSourcePort": 9901,
                "socketSourceHost": "localhost"
            }
        },
        {
            "name": "Union of input",
            "id": 3,
            "type": "ReflexTwoUnionComponent",
            "parents": [{"parent": 1, "output" : 0}, {"parent": 2, "output" : 0}],
            "arguments" : {
            }
        },
        {
            "name": "printer",
            "id": 4,
            "type": "ReflexNullConnector",
            "parents": [{"parent": 3, "output" : 0}]
        }
        ]
    }
    """
    val res = DagTestUtil.parseGenerateValidate(goodJson)
    assert(res != null, "json1 is not valid")
    assert(res.nodeList.length == 4, "wrong number of nodes in DAG after parsing")
  }

  "Good pipeline 4 with BreezeVector components" should "be valid" in {
    val goodJson =
      """
            {
                "name" : "ReflexSamplePipeline",
                "engineType": "FlinkStreaming",
             "systemConfig" : {
                  "statsDBHost": "localhost",
                  "statsDBPort": 8086,
                  "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                  "statsMeasurementID": "1",
                  "modelFileSinkPath": "/tmp/tmpFile"
             },
                "pipe" : [
                    {
                        "name": "Turbine 1",
                        "id": 1,
                        "type": "ReflexSocketConnector",
                        "parents": [],
                        "arguments" : {
                            "socketSourcePort": 9900,
                            "socketSourceHost": "localhost"
                        }
                    },
                    {
                        "name": "String to vector",
                        "id": 2,
                        "type": "ReflexStringToBreezeVectorComponent",
                        "parents": [{"parent": 1, "output" : 0}],
                        "arguments" : {
                            "separator": "comma",
                            "attributes": 3,
                            "indicesRange": "0-2"
                        }
                    },
                    {
                        "name": "printer",
                        "id": 3,
                        "type": "ReflexNullConnector",
                        "parents": [{"parent": 2, "output" : 0}]
                    }
                ]
        }
        """
    val res = DagTestUtil.parseGenerateValidate(goodJson)
    assert(res != null, "json1 is not valid")
    assert(res.nodeList.length == 3, "wrong number of nodes in DAG after parsing")
  }

  "Good Pipeline with multi input component and input index" should "be valid" in {
    val flinkStreamingInfo =
      ReflexComponentFactory
        .getEngineFactory(ComputeEngineType.FlinkStreaming)
        .asInstanceOf[ByClassComponentFactory]

    flinkStreamingInfo.registerComponent(classOf[com.parallelmachines.reflex.test.components.TestAlgoComponentWithModel])
    val json1 =
      """
    {
    "name": "SVM Inference with Model Update 0.9 pipeline",
    "engineType": "FlinkStreaming",
    "systemConfig": {
      "statsDBHost": "daenerys-c17",
      "statsDBPort": 8086,
      "mlObjectSocketHost": "",
      "mlObjectSocketSinkPort": 7777,
      "mlObjectSocketSourcePort": 7777,
      "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
      "statsMeasurementID": "1",
      "modelFileSinkPath": "/tmp/model_sink",
      "healthStatSocketSourcePort": 34566
    },
    "pipe": [
    {
      "name": "Model updater",
      "id": 1,
      "type": "ReflexNullSourceConnector",
      "parents": []
    },
    {
      "name": "Kafka",
      "id": 2,
      "type": "ReflexKafkaConnector",
      "parents": [],
      "arguments": {
        "port": 9092,
        "host": "daenerys-c17",
        "topic": "SVM"
      }
    },
    {
      "name": "String to Labeled Vector",
      "id": 3,
      "type": "ReflexStringToLabeledVectorComponent",
      "parents": [
      {
        "parent": 2,
        "output": 0
      }
      ],
      "arguments": {
        "separator": ",",
        "attributes": 26,
        "labelIndex": 0,
        "timestampIndex": 25,
        "indicesRange": "1-24"
      }
    },
    {
      "name": "SVM inference",
      "id": 4,
      "type": "TestAlgoComponentWithModel",
      "parents": [
      {
        "parent": 1,
        "output": 0,
        "input": 1
      },
      {
        "parent": 3,
        "output": 0,
        "input": 0
      }
      ]
    },
    {
      "name": "null",
      "id": 5,
      "type": "ReflexNullConnector",
      "parents": [
      {
        "parent": 4,
        "output": 0
      }
      ]
    },
    {
      "name": "null",
      "id": 6,
      "type": "ReflexNullConnector",
      "parents": [
      {
        "parent": 4,
        "output": 1
      }
      ]
    }
    ]
  }
  """
    val res = DagTestUtil.parseGenerateValidate(json1)
    assert(res != null, "json1 is not valid")

    // 6
    assert(res.nodeList.length == 6, "wrong number of nodes in DAG after parsing")
  }

  "Good Pipeline with input index in parent definition" should "be valid" in {

    val json1 =
      """
        {
        "name" : "ReflexPipeline_1",
        "engineType": "FlinkStreaming",
        "systemConfig" : {
          "statsDBHost": "localhost",
          "statsDBPort": 8086,
          "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
          "statsMeasurementID": "1",
          "modelFileSinkPath": "/tmp/tmpFile"
        },
        "pipe" : [
           {
                 "name": "data parser",
                 "id": 33,
                 "type": "ReflexStringToBreezeVectorComponent",
                 "parents": [{"parent": 22, "output" : 0, "input": 0}],
                 "arguments" : {
                     "separator": "colon",
                     "attributes": 1,
                     "indicesRange": "1"
                 }
             },
             {
                "name": "Turbine general input",
                "id": 22,
                "type": "ReflexSocketConnector",
                "parents": [],
                "arguments" : {
                    "socketSourcePort": 9900,
                    "socketSourceHost": "localhost"
                }
            },
            {
                "name": "printer",
                "id": 44,
                "type": "ReflexNullConnector",
                "parents": [{"parent": 33, "output" : 0, "input": 0}]
            }
            ]
        }
        """

    val res = DagTestUtil.parseGenerateValidate(json1)
    assert(res != null, "json1 is not valid")
    assert(res.nodeList.length == 3, "wrong number of nodes in DAG after parsing")
  }

  "Good Pipeline with input index in parent definition 2, where Parents list" should "be sorted by input field" in {

    val goodJson =
      """
        {
        "name" : "ReflexSamplePipeline",
        "engineType": "FlinkStreaming",
             "systemConfig" : {
                  "statsDBHost": "localhost",
                  "statsDBPort": 8086,
                  "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                  "statsMeasurementID": "1",
                  "modelFileSinkPath": "/tmp/tmpFile"
             },
        "pipe" : [
        {
            "name": "Source 1",
            "id": 1,
            "type": "ReflexNullSourceConnector",
            "parents": [],
            "arguments" : {}
        },
        {
            "name": "Source 2",
            "id": 2,
            "type": "ReflexNullSourceConnector",
            "parents": [],
            "arguments" : {}
        },
        {
            "name": "Union of input",
            "id": 3,
            "type": "ReflexTwoUnionComponent",
            "parents": [{"parent": 1, "output" : 0, "input": 1}, {"parent": 2, "output" : 0, "input": 0}],
            "arguments" : {
            }
        },
        {
            "name": "printer",
            "id": 4,
            "type": "ReflexNullConnector",
            "parents": [{"parent": 3, "output" : 0}]
        }
        ]
    }
    """
    val jsonBuilder = new ReflexPipelineBuilder()
    jsonBuilder.buildPipelineFromJson(goodJson)
    assert(jsonBuilder.pipeInfo.pipe.length == 4, "wrong number of components in a pipe after parsing")
    assert(jsonBuilder.pipeInfo.pipe(2).parents(0).input.get == 0, "'input' field expected to be 0")
    assert(jsonBuilder.pipeInfo.pipe(2).parents(1).input.get == 1, "'input' field expected to be 1")
    assert(jsonBuilder.pipeInfo.pipe(3).parents(0).input.get == 0, "'input' field expected to be 0")
  }


  "Good pipeline - test args" should "be valid" in {

    val json1 =
      """
        {
        "name" : "ReflexPipeline_DoNothing",
        "engineType": "FlinkStreaming",
             "systemConfig" : {
                  "statsDBHost": "localhost",
                  "statsDBPort": 8086,
                  "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                  "statsMeasurementID": "1",
                  "modelFileSinkPath": "/tmp/tmpFile"
             },
        "pipe" : [
           {
                "name": "data parser",
                "id": 77,
                "type": "TestArgsComponent",
                "parents": [{"parent": 88, "output": 0}],
                "arguments" : {
                    "bool-val": true,
                    "int-val": 33,
                    "long-val": 5555,
                    "double-val": 9.73,
                    "string-val": "World"
                }
           },
           {
                "name": "String generator",
                "id": 88,
                "type": "ReflexSocketConnector",
                "parents": [],
                "arguments" : {
                    "socketSourcePort": 9900,
                    "socketSourceHost": "localhost"
                }
           },
           {
               "name": "printer",
               "id": 44,
               "type": "ReflexNullConnector",
               "parents": [{"parent": 77, "output": 0}]
           }
          ]
        }
        """

    val res = DagTestUtil.parseGenerateValidate(json1)
    assert(res != null, "json1 is not valid")
    assert(res.nodeList.length == 3, "wrong number of nodes in DAG after parsing")
  }

  "Good pipeline with type propagation" should "be valid" in {
    val goodJson =
      """
        {
        "name" : "ReflexSamplePipeline",
        "engineType": "FlinkStreaming",
             "systemConfig" : {
                  "statsDBHost": "localhost",
                  "statsDBPort": 8086,
                  "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                  "statsMeasurementID": "1",
                  "modelFileSinkPath": "/tmp/tmpFile"
             },
        "pipe" : [
        {
            "name": "Turbine 1",
            "id": 1,
            "type": "ReflexSocketConnector",
            "parents": [],
            "arguments" : {
                "socketSourcePort": 9900,
                "socketSourceHost": "localhost"
            }
        },
        {
            "name": "Turbine 2",
            "id": 2,
            "type": "ReflexSocketConnector",
            "parents": [],
            "arguments" : {
                "socketSourcePort": 9901,
                "socketSourceHost": "localhost"
            }
        },
        {
            "name": "Union of input",
            "id": 3,
            "type": "ReflexTwoUnionComponent",
            "parents": [{"parent": 1, "output" : 0}, {"parent": 2, "output" : 0}],
            "arguments" : {
            }
        },
        {
          "name": "String to vector",
          "id": 4,
          "type": "ReflexStringToBreezeVectorComponent",
          "parents": [{"parent": 3, "output" : 0}],
            "arguments" : {
                "separator": "comma",
                "attributes": 3,
                "indicesRange": "0-2"
            }
        },
        {
            "name": "printer",
            "id": 5,
            "type": "ReflexNullConnector",
            "parents": [{"parent": 4, "output" : 0}]
        }
        ]
    }
    """
    val res = DagTestUtil.parseGenerateValidate(goodJson)
    assert(res != null, "goodJson is not valid")
    assert(res.nodeList.length == 5, "wrong number of nodes in DAG after parsing")
  }

  "Good pipeline with default output nodes" should "be valid" in {

    val flinkStreamingInfo =
      ReflexComponentFactory
        .getEngineFactory(ComputeEngineType.FlinkStreaming)
        .asInstanceOf[ByClassComponentFactory]

    flinkStreamingInfo.registerComponent(classOf[com.parallelmachines.reflex.test.components.TestAlgoComponent])
    val goodJson =
      """
    {
      "name": "KMeans Training Example",
      "engineType": "FlinkStreaming",
      "systemConfig" : {
        "statsDBHost": "localhost",
        "statsDBPort": 8086,
        "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
        "statsMeasurementID": "1",
        "mlObjectSocketHost": "localhost",
        "mlObjectSocketSinkPort": 7777,
        "mlObjectSocketSourcePort": 7777,
        "modelFileSinkPath": "/tmp/tmpFile"
      },
      "pipe": [
      {
        "name": "Data",
        "id": 1,
        "type": "ReflexNullSourceConnector",
        "parents": []
      },
      {
        "name": "Training",
        "id": 2,
        "type": "TestAlgoComponent",
        "parents": [
        {
          "parent": 1,
          "output": 0
        }
        ]
      },
      {
        "name": "null",
        "id": 5,
        "type": "ReflexNullConnector",
        "parents": [
        {
          "parent": 2,
          "output": 0
        }
        ]
      }
      ]
    }
    """
    val res = DagTestUtil.parseGenerateValidate(goodJson)
    // Expected number of nodes: 5 (3 components itself + 2 default outputs)
    assert(res.nodeList.length == 5, "wrong number of nodes in DAG after parsing")
  }

  "Good pipeline with default input nodes" should "be valid" in {

    val goodJson =
      """
    {
      "name": "Unit Test pipeline",
      "engineType": "FlinkStreaming",
      "systemConfig" : {
        "statsDBHost": "localhost",
        "statsDBPort": 8086,
        "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
        "statsMeasurementID": "1",
        "modelFileSinkPath": "/tmp/tmpFile"
      },
      "pipe": [
      {
        "name": "Test Component with default input",
        "id": 1,
        "type": "TestComponentWithDefaultInput",
        "parents": []
      }
      ]
    }
    """
    val res = DagTestUtil.parseGenerateValidate(goodJson)
    // Expected number of nodes: 2 (component itself + default input)
    assert(res.nodeList.length == 2, "wrong number of nodes in DAG after parsing")
  }

  "Good pipeline with component with three default inputs, where 2nd input is defined with the help of input field" should "be valid" in {

    val goodJson =
      """
    {
      "name": "Unit Test pipeline",
      "engineType": "FlinkStreaming",
      "systemConfig" : {
        "statsDBHost": "localhost",
        "statsDBPort": 8086,
        "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
        "statsMeasurementID": "1",
        "modelFileSinkPath": "/tmp/tmpFile"
      },
      "pipe": [
      {
        "name": "Test Component Source",
        "id": 1,
        "type": "TestComponentSource",
        "parents": []
      },
      {
        "name": "Test Component with default input",
        "id": 2,
        "type": "TestComponentWithThreeDefaultInputs",
        "parents": [{"parent":1, "output": 0, "input" : 1}]
      }
      ]
    }
    """
    val res = DagTestUtil.parseGenerateValidate(goodJson)
    // Expected number of nodes: 4 (component itself + 2 default input + 1 explicit input)
    assert(res.nodeList.length == 4, "wrong number of nodes in DAG after parsing")
  }


  "Good pipeline with default input nodes and side component 1" should "be valid" in {

    val goodJson =
      """
    {
      "name": "Unit Test pipeline",
      "engineType": "FlinkStreaming",
      "systemConfig" : {
        "statsDBHost": "localhost",
        "statsDBPort": 8086,
        "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
        "statsMeasurementID": "1",
        "modelFileSinkPath": "/tmp/tmpFile",
        "healthStatFilePath": "/TMP/TESTPATH"
      },
      "pipe": [
      {
        "name": "Test Component with default input",
        "id": 1,
        "type": "TestComponentWithDefaultInputAndSide1",
        "parents": []
      }
      ]
    }
    """
    val res = DagTestUtil.parseGenerateValidate(goodJson)
    // Expected number of nodes: 4 (component itself + default input + dup + side)
    assert(res.nodeList.length == 4, "wrong number of nodes in DAG after parsing")
  }


  "Good pipeline with default input nodes and side component 2" should "be valid" in {

    val goodJson =
      """
    {
      "name": "Unit Test pipeline",
      "engineType": "FlinkStreaming",
      "systemConfig" : {
        "statsDBHost": "localhost",
        "statsDBPort": 8086,
        "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
        "statsMeasurementID": "1",
        "modelFileSinkPath": "/tmp/tmpFile"
      },
      "pipe": [
      {
        "name": "Test Component with default input",
        "id": 1,
        "type": "TestComponentWithDefaultInputAndSide2",
        "parents": []
      }
      ]
    }
    """
    val res = DagTestUtil.parseGenerateValidate(goodJson)
    // Expected number of nodes: 5 (component itself + default input + dup + side with default output + output)
    assert(res.nodeList.length == 5, "wrong number of nodes in DAG after parsing")
  }

  "Good pipeline with connection inheritance" should "be valid" in {
    val json1 =
      """
        {
        "name" : "ReflexPipeline_1",
        "engineType": "FlinkStreaming",
        "systemConfig" : {
          "statsDBHost": "localhost",
          "statsDBPort": 8086,
          "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
          "statsMeasurementID": "1",
          "modelFileSinkPath": "/tmp/tmpFile"
        },
        "pipe" : [
           {
                 "name": "AA",
                 "id": 1,
                 "type": "TestDataItemBSource",
                 "parents": [],
                 "arguments" : {}
             },
             {
                "name": "BB",
                "id": 2,
                "type": "TestNeedDataItemAInput",
                "parents": [{"parent": 1, "output": 0}],
                "arguments" : {}
            }
            ]
        }
        """

    val res = DagTestUtil.parseGenerateValidate(json1)
    assert(res != null, "json1 is not valid")
    assert(res.nodeList.length == 2, "wrong number of nodes in DAG after parsing")
  }
  "Bad pipeline with infinite default outputs" should "throw Exception with generic message" in {
    val bad_json1 =
      """
            {
            "name": "ReflexPipelineBadExample",
            "engineType": "FlinkStreaming",
             "systemConfig" : {
                "statsDBHost": "localhost",
                "statsDBPort": 8086,
                "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                "statsMeasurementID": "1",
                "modelFileSinkPath": "/tmp/tmpFile"
             },
            "pipe" : [
            {
              "name": "Test Component Source",
              "id": 1,
              "type": "TestComponentSource",
              "parents": []
            },
            {
               "name": "Test",
               "id": 2,
               "type": "TestComponentWithDefaultOutput1",
               "parents": [{"parent":1, "output":0}]
             }
            ]
            }
            """

    val ex = intercept[Exception] {
      DagTestUtil.parseGenerateValidate(bad_json1)
    }
    assert(ex.getMessage.contains("Presumably DAG has infinite dependency"))
  }

  "Good pipeline with two default outputs" should "be valid" in {
    val good_json =
      """
            {
            "name": "ReflexPipelineBadExample",
            "engineType": "FlinkStreaming",
             "systemConfig" : {
                "statsDBHost": "localhost",
                "statsDBPort": 8086,
                "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                "statsMeasurementID": "1",
                "modelFileSinkPath": "/tmp/tmpFile"
             },
            "pipe" : [
            {
              "name": "Test Component Source",
              "id": 1,
              "type": "TestComponentSource",
              "parents": []
            },
            {
               "name": "Test",
               "id": 2,
               "type": "TestComponentWithTwoDefaultOutputs",
               "parents": [{"parent":1, "output":0}]
             }
            ]
            }
            """
    val res = DagTestUtil.parseGenerateValidate(good_json)
    assert(res != null, "good json is not valid")
    assert(res.nodeList.length == 4, "wrong number of nodes in DAG after parsing")
  }

  "Good pipeline with default output, which has two default outputs" should "be valid" in {
    val good_json =
      """
            {
            "name": "ReflexPipelineBadExample",
            "engineType": "FlinkStreaming",
             "systemConfig" : {
                "statsDBHost": "localhost",
                "statsDBPort": 8086,
                "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                "statsMeasurementID": "1",
                "modelFileSinkPath": "/tmp/tmpFile"
             },
            "pipe" : [
            {
              "name": "Test Component Source",
              "id": 1,
              "type": "TestComponentSource",
              "parents": []
            },
            {
               "name": "Test",
               "id": 2,
               "type": "TestComponentWithDefaultOutput",
               "parents": [{"parent":1, "output":0}]
             }
            ]
            }
            """
    val res = DagTestUtil.parseGenerateValidate(good_json)
    assert(res != null, "good json is not valid")
    assert(res.nodeList.length == 5, "wrong number of nodes in DAG after parsing")
  }

  "Bad pipeline with arguments in component and system config" should "throw Exception with generic message" in {

    val bad_json1 =
      """
        {
        "name" : "ReflexPipeline_1",
        "engineType": "FlinkStreaming",
        "systemConfig" : {
          "statsDBHost": "localhost",
          "statsDBPort": 8086,
          "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
          "statsMeasurementID": "1",
          "modelFileSinkPath": "/tmp/tmpFile",
          "socketSourceHost": "localhost"
        },
        "pipe" : [
           {
                 "name": "data parser",
                 "id": 33,
                 "type": "ReflexStringToBreezeVectorComponent",
                 "parents": [{"parent": 22, "output" : 0}],
                 "arguments" : {
                     "separator": "colon",
                     "attributes": 1,
                     "indicesRange": "1"
                 }
             },
             {
                "name": "Turbine general input",
                "id": 22,
                "type": "ReflexSocketConnector",
                "parents": [],
                "arguments" : {
                    "socketSourcePort": 9900,
                    "socketSourceHost": "localhost"
                }
            },
            {
                "name": "printer",
                "id": 44,
                "type": "ReflexNullConnector",
                "parents": [{"parent": 33, "output" : 0}]
            }
            ]
        }
        """

    val ex = intercept[Exception] {
      DagTestUtil.parseGenerateValidate(bad_json1)
    }
    assert(ex.getMessage.contains("Error system argument socketSourceHost already exists in component map"))
  }

  "Bad pipeline" should "throw Exception with generic message" in {
    val bad_json1 =
      """
            {
            "name": "ReflexPipelineBadExample",
            "engineType": "FlinkStreaming",
             "systemConfig" : {
                "statsDBHost": "localhost",
                "statsDBPort": 8086,
                "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                "statsMeasurementID": "1",
                "modelFileSinkPath": "/tmp/tmpFile"
             },

            "pipe" : [
                {
                    "name": "Turbine general input",
                    "id": 22,
                    "type": "ReflexSocketConnector",
                    "parents": [],
                    "arguments" : {
                        "socketSourcePort": 9900,
                        "socketSourceHost": "localhost"
                    }
                },
                {
                    "name": "data parser",
                    "id": 33,
                    "type": "ReflexStringToBreezeVectorComponent",
                    "parents": [],
                    "arguments" : {
                        "separator": "colon",
                        "attributes": 3,
                        "indicesRange": "0-2"
                    }
                },
                {
                    "name": "printer",
                    "id": 44,
                    "type": "ReflexNullConnector",
                    "parents": [{"parent": 22, "output" : 0}]
                }
                ]
            }
            """
    val ex = intercept[Exception] {
      DagTestUtil.parseGenerateValidate(bad_json1)
    }
    assert(ex.getMessage.contains("Graph is invalid, missing output for component id = 33"))
  }

  "Bad pipeline 2" should "throw Exception with generic message" in {
    val json1 =
      """
        {
            "name" : "ReflexSamplePipeline",
            "engineType": "FlinkStreaming",
            "systemConfig" : {
                 "statsDBHost": "localhost",
                 "statsDBPort": 8086,
                 "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                 "statsMeasurementID": "1",
                 "modelFileSinkPath": "/tmp/tmpFile"
            },
            "pipe" : [
            {
                "name": "Turbine general input",
                "id": 1,
                "type": "ReflexSocketConnector",
                "parents": [],
                "arguments" : {
                    "port": 9900,
                    "host": "localhost"
                }
            },
            {
                "name": "Turbine general input",
                "id": 2,
                "type": "TwoDup",
                "parents": [{"parent": 1, "output" : 0}],
                "arguments" : {
                }
            },
            {
                "name": "printer",
                "id": 3,
                "type": "ReflexNullConnector",
                "parents": [{"parent": 2, "output" : 0}]
            }
            ]
        }
       """

    val ex = intercept[Exception] {
      DagTestUtil.parseGenerateValidate(json1)
    }
    assert(ex.getMessage.contains("Graph is invalid, missing output for component id = 2"))
  }

  "Bad pipeline non compatible connection" should "throw Exception with generic message" in {
    val json1 =
      """
        {
            "name" : "ReflexSamplePipeline",
            "engineType": "FlinkStreaming",
            "systemConfig" : {
                 "statsDBHost": "localhost",
                 "statsDBPort": 8086,
                 "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                 "statsMeasurementID": "1",
                 "modelFileSinkPath": "/tmp/tmpFile"
            },
            "pipe" : [
            {
              "name": "Turbine general input",
              "id": 1,
              "type": "ReflexSocketConnector",
              "parents": [],
              "arguments" : {
                "port": 9900,
                "host": "localhost"
              }
            },
            {
                "name": "data parser",
                "id": 2,
                "type": "ReflexStringToBreezeVectorComponent",
                "parents": [{"parent": 1, "output" : 0}],
                "arguments" : {
                  "separator": "colon",
                  "attributes": 3,
                  "indicesRange": "0-2"
                }
            },
            {
              "name": "printer",
              "id": 2,
              "type": "ReflexNullConnector",
              "parents": [{"parent": 2, "output" : 0}]
            }
            ]
        }
       """

    val ex = intercept[Exception] {
      DagTestUtil.parseGenerateValidate(json1)
    }
    assert(ex.getMessage.contains("Pipeline parsing: Found duplicate id in node list 2"))
  }

  "Bad pipeline with no source" should "throw Exception with generic message" in {
    val bad_json1 =
      """
            {
            "name": "ReflexPipelineBadExample",
            "engineType": "FlinkStreaming",
             "systemConfig" : {
                  "statsDBHost": "localhost",
                  "statsDBPort": 8086,
                  "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                  "statsMeasurementID": "1",
                  "modelFileSinkPath": "/tmp/tmpFile"
             },
            "pipe" : [
                {
                    "name": "data parser",
                    "id": 33,
                    "type": "ReflexStringToBreezeVectorComponent",
                    "parents": [],
                    "arguments" : {
                        "separator": "colon",
                        "attributes": 3,
                        "indicesRange": "0-2"
                    }
                },
                {
                    "name": "printer",
                    "id": 44,
                    "type": "ReflexNullConnector",
                    "parents": [{"parent": 33, "output" : 0}]
                }
                ]
            }
            """
    val ex = intercept[Exception] {
      DagTestUtil.parseGenerateValidate(bad_json1)
    }
    assert(ex.getMessage.contains("Graph is invalid, missing input for component id = 33"))
  }

  "Bad pipeline with duplicate id " should "throw Exception with generic message" in {

    val json1 =
      """
        {
        "name" : "ReflexPipeline_1",
        "engineType": "FlinkStreaming",
             "systemConfig" : {
                  "statsDBHost": "localhost",
                  "statsDBPort": 8086,
                  "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                  "statsMeasurementID": "1",
                  "modelFileSinkPath": "/tmp/tmpFile"
        },
        "pipe" : [
             {
                 "name": "Turbine general input",
                 "id": 1,
                 "type": "ReflexSocketConnector",
                 "parents": [],
                 "arguments" : {
                     "port": 9900,
                     "host": "localhost"
                 }
             },
             {
                  "name": "Turbine general input",
                  "id": 1,
                  "type": "ReflexSocketConnector",
                  "parents": [],
                  "arguments" : {
                      "port": 9900,
                      "host": "localhost"
                  }
            },
           {
                 "name": "data parser",
                 "id": 2,
                 "type": "ReflexStringToBreezeVectorComponent",
                 "parents": [{"parent": 1, "output" : 0}],
                 "arguments" : {
                     "separator": "colon",
                     "attributes": 3,
                     "indicesRange": "0-2"
                 }
             },
            {
                "name": "printer",
                "id": 3,
                "type": "ReflexNullConnector",
                "parents": [{"parent": 2, "output" : 0}]
            }
            ]
        }
        """

    val ex = intercept[Exception] {
      DagTestUtil.parseGenerateValidate(json1)
    }
    assert(ex.getMessage.contains("Pipeline parsing: Found duplicate id in node list 1"))
  }

  "Bad pipeline with a loop" should "throw Exception with generic message" in {

    val json1 =
      """
        {
        "name" : "ReflexPipeline_1",
        "engineType": "FlinkStreaming",
             "systemConfig" : {
                  "statsDBHost": "localhost",
                  "statsDBPort": 8086,
                  "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                  "statsMeasurementID": "1",
                  "modelFileSinkPath": "/tmp/tmpFile"
            },
        "pipe" : [
             {
                "name": "Parser1",
                "id": 22,
                "type": "ReflexSocketConnector",
                "parents": [],
                "arguments" : {
                    "socketSourcePort": 9900,
                    "socketSourceHost": "localhost"
                }
            },
            {
                  "name": "parser1",
                  "id": 33,
                  "type": "ReflexStringToBreezeVectorComponent",
                  "parents": [{"parent": 44, "output" : 0}],
                  "arguments" : {
                      "separator": "comma",
                      "attributes": 3,
                      "indicesRange": "0-2"
                  }
              },
            {
                  "name": "data parser",
                  "id": 44,
                  "type": "ReflexStringToBreezeVectorComponent",
                  "parents": [{"parent": 33, "output" : 0}],
                  "arguments" : {
                      "separator": "comma",
                      "attributes": 3,
                      "indicesRange": "0-2"
                  }
              },
            {
                "name": "printer",
                "id": 55,
                "type": "ReflexNullConnector",
                "parents": [{"parent": 22, "output" : 0}]
            }
            ]
        }
        """

    val ex = intercept[Exception] {
      DagTestUtil.parseGenerateValidate(json1)
    }
    assert(ex.getMessage.contains("Dag contains loop"))
  }

  "Bad pipeline mixing 2 engines" should "not be valid" in {
    val tmpFile = File.createTempFile("tmpFile", ".csv")
    tmpFile.deleteOnExit()

    val json1 =
      s"""
        {
        "name" : "ReflexPipeline_1",
        "engineType": "FlinkStreaming",
             "systemConfig" : {
                  "statsDBHost": "localhost",
                  "statsDBPort": 8086,
                  "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                  "statsMeasurementID": "1",
                  "modelFileSinkPath": "/tmp/tmpFile"
             },
        "pipe" : [
             {
                "name": "Parser1",
                "id": 1,
                "type": "FlinkBatchFileConnector",
                "parents": [],
                "arguments" : {
                  "fileName" : "${tmpFile.getAbsolutePath}"
                }
            },
            {
                  "name": "parser1",
                  "id": 2,
                  "type": "ReflexStringToBreezeVectorComponent",
                  "parents": [{"parent": 1, "output" : 0}],
                  "arguments" : {
                      "separator": "comma",
                      "attributes": 3,
                      "indicesRange": "0-2"
                  }
              },
            {
                "name": "printer",
                "id": 3,
                "type": "ReflexNullConnector",
                "parents": [{"parent": 2, "output" : 0}]
            }
            ]
        }
        """

    val ex = intercept[Exception] {
      val res = DagTestUtil.parseGenerateValidate(json1)
    }
    assert(ex.getMessage.contains("Error: component 'FlinkBatchFileConnector' is not supported by 'FlinkStreaming' engine"))
  }

  "Bad Pipeline with multi input component and mixed input index and non input index " should "not be valid" in {
    val json1 =
      """
    {
    "name": "SVM Inference with Model Update 0.9 pipeline",
    "engineType": "FlinkStreaming",
    "systemConfig": {
      "statsDBHost": "daenerys-c17",
      "statsDBPort": 8086,
      "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
      "statsMeasurementID": "1",
      "modelFileSinkPath": "/tmp/model_sink"
    },
    "pipe": [
    {
      "name": "Model updater",
      "id": 1,
      "type": "ReflexNullSourceConnector",
      "parents": []
    },
    {
      "name": "Data",
      "id": 2,
      "type": "ReflexNullSourceConnector",
      "parents": []
    },
    {
      "name": "String to Labeled Vector",
      "id": 3,
      "type": "ReflexStringToLabeledVectorComponent",
      "parents": [
      {
        "parent": 2,
        "output": 0
      }
      ],
      "arguments": {
        "separator": ",",
        "attributes": 26,
        "labelIndex": 0,
        "timestampIndex": 25,
        "indicesRange": "1-24"
      }
    },
    {
      "name": "Inference",
      "id": 4,
      "type": "TestAlgoComponentWithModel",
      "parents": [
      {
        "parent": 1,
        "output": 0
      },
      {
        "parent": 3,
        "output": 0,
        "input": 0
      }
      ]
    },
    {
      "name": "null",
      "id": 5,
      "type": "ReflexNullConnector",
      "parents": [
      {
        "parent": 4,
        "output": 0
      }
      ]
    },
    {
      "name": "null",
      "id": 6,
      "type": "ReflexNullConnector",
      "parents": [
      {
        "parent": 4,
        "output": 1
      }
      ]
    }
    ]
  }
  """
    val ex = intercept[Exception] {
      val res = DagTestUtil.parseGenerateValidate(json1)
    }
    assert(ex.getMessage.contains("Parent JSON is in mixed format - some parents without input index and some with: in TestAlgoComponentWithModel"))
  }

  "System config tests" should "be valid" in {
    val json1 =
      s"""
        {
        "name" : "ReflexPipeline_1",
        "engineType": "FlinkStreaming",
        "systemConfig" : {
                 "statsDBHost": "localhost",
                 "statsDBPort": 8086,
                 "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                 "statsMeasurementID": "1",
                 "modelFileSinkPath": "/tmp/tmpFile",
                 "healthStatFilePath": "/TMP/TESTPATH"
        },
        "pipe" : [
             {
                "name": "Turbine general input",
                "id": 1,
                "type": "TestSystemConfigComponent",
                "parents": [],
                "arguments" : {
                }
            }
            ]
        }
        """

    val fs = ReflexComponentFactory.getEngineFactory(ComputeEngineType.FlinkStreaming).asInstanceOf[FlinkStreamingComponentFactory]
    fs.registerComponent(classOf[com.parallelmachines.reflex.test.components.TestSystemConfigComponent])

    val res = new ReflexPipelineBuilder().buildPipelineFromJson(json1)
    assert(res.nodeList.size == 1, "wrong number of nodes in a pipe after parsing")
  }

  "Parse pipeline info" should "be valid" in {
    val json1 =
      s"""
        {
        "name" : "ReflexPipeline_1",
        "engineType": "FlinkStreaming",
        "language" : "Python",
        "systemConfig" : {
                 "statsDBHost": "localhost",
                 "statsDBPort": 8086,
                 "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                 "statsMeasurementID": "1",
                 "modelFileSinkPath": "/tmp/tmpFile",
                 "healthStatFilePath": "/TMP/TESTPATH"
        },
        "pipe" : []
        }
        """

    val jsonParser = new ReflexPipelineJsonParser()
    val pipelineInfo = jsonParser.parsePipelineInfo(json1)
    assert(pipelineInfo.name == "ReflexPipeline_1")
    assert(pipelineInfo.engineType == ComputeEngineType.FlinkStreaming)
    assert(pipelineInfo.language.get == "Python")
  }

  "Tensorflow component signature parsing" should "be valid" in {
    val compJson =
      s"""
         |{
         |    "engineType": "Generic",
         |    "name": "MnistCnnLayers",
         |    "label": "MnistCNN",
         |    "program": "cnn_mnist.py",
         |    "modelBehavior": "ModelProducer",
         |    "inputInfo": [],
         |    "outputInfo": [],
         |    "group": "Algorithms",
         |    "arguments": [
         |        {
         |            "key": "step_size",
         |            "label": "Step Size",
         |            "type": "float",
         |            "description": "Learning rate",
         |            "optional": true,
         |            "defaultValue": 0.01
         |        },
         |        {
         |            "key": "iterations",
         |            "label": "Number of iterations",
         |            "type": "int",
         |            "description": "Number of training iterations",
         |            "optional": true,
         |            "defaultValue": 20000
         |        },
         |        {
         |            "key": "batch_size",
         |            "label": "Batch size",
         |            "type": "int",
         |            "description": "Training batch input size",
         |            "optional": true,
         |            "defaultValue": 50
         |        },
         |        {
         |            "key": "model_version",
         |            "label": "Model version",
         |            "type": "str",
         |            "description": "Model version",
         |            "optional": true
         |        },
         |        {
         |            "key": "stats_interval",
         |            "label": "Statistics Interval",
         |            "type": "int",
         |            "description": "Print stats after this number of iterations",
         |            "optional": true,
         |            "defaultValue": 100
         |        },
         |        {
         |            "key": "save_dir",
         |            "label": "Model output dir",
         |            "type": "str",
         |            "description": "Directory for saving the trained model",
         |            "optional": false
         |        },
         |        {
         |            "key": "tf_log",
         |            "label": "Log directory",
         |            "type": "str",
         |            "description": "Tensorflow log directory",
         |            "optional": false
         |        },
         |        {
         |            "key": "input_dir",
         |            "label": "Input data directory",
         |            "type": "str",
         |            "description": "Directory for storing input data",
         |            "optional": true,
         |            "defaultValue": "mnist_configuration"
         |        }
         |    ],
         |    "version": 1
         |}
         |
    """.stripMargin


    val p = ComponentJSONSignatureParser
    val compMeta = p.parseSignature(compJson)
    assert(compMeta.engineType == "Generic")
    assert(compMeta.arguments.length == 8)
  }

  "Pipeline with 2 components and 2 implicit input singletons" should "be valid" in {
    val json1 =
      s"""
        {
        "name" : "ReflexPipeline_1",
        "engineType": "FlinkStreaming",
        "systemConfig" : {
                 "statsDBHost": "localhost",
                 "statsDBPort": 8086,
                 "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                 "statsMeasurementID": "1",
                 "modelFileSinkPath": "/tmp/tmpFile",
                 "mlObjectSocketHost": "localhost",
                 "mlObjectSocketSourcePort": 7777,
                 "healthStatFilePath": "/TMP/TESTPATH"
        },
        "pipe" : [
             {
                "name": "Turbine general input",
                "id": 1,
                "type": "TestComponentWithDefaultInputSingleton1",
                "parents": [],
                "arguments" : {
                }
            },
            {
               "name": "Turbine general input",
               "id": 2,
               "type": "TestComponentWithDefaultInputSingleton2",
               "parents": [],
               "arguments" : {
               }
           }
            ]
        }
        """

    val flinkStreamingInfo = ReflexComponentFactory.getEngineFactory(ComputeEngineType.FlinkStreaming).asInstanceOf[ByClassComponentFactory]
    flinkStreamingInfo.registerComponent(classOf[com.parallelmachines.reflex.test.components.TestComponentWithDefaultInputSingleton1])
    flinkStreamingInfo.registerComponent(classOf[com.parallelmachines.reflex.test.components.TestComponentWithDefaultInputSingleton2])

    val res = DagTestUtil.parseGenerateValidate(json1)
    // Expected number of nodes: 5 (2 components itself + 2 defaults + 1 singleton
    val nodeListWithSingletonComponent = res.nodeList.filter(_.component.name == "EventSocketSource")
    assert(nodeListWithSingletonComponent.size == 1)
    assert(nodeListWithSingletonComponent(0).component.inputTypes.size == 0)
    assert(nodeListWithSingletonComponent(0).component.outputTypes.size == 2)
    assert(res.nodeList.length == 5, "wrong number of nodes in DAG after parsing")
  }

  "Pipeline with 2 components and 2 explicit input singletons" should "be valid" in {
    val json1 =
      s"""
          {
          "name" : "ReflexPipeline_1",
          "engineType": "FlinkStreaming",
          "systemConfig" : {
                   "statsDBHost": "localhost",
                   "statsDBPort": 8086,
                   "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                   "statsMeasurementID": "1",
                   "modelFileSinkPath": "/tmp/tmpFile",
                   "mlObjectSocketHost": "localhost",
                   "mlObjectSocketSourcePort": 7777,
                   "healthStatFilePath": "/TMP/TESTPATH"
          },
          "pipe" : [
                {
                   "name": "TestComponentWithDefaultInputSingleton1",
                   "id": 3,
                   "type": "TestComponentWithDefaultInputSingleton1",
                   "parents": [{"parent": 1, "output": 0, "eventType":"Alert"}],
                   "arguments" : {
                   }
                },
                {
                  "name": "Test Socket Source1",
                  "id": 1,
                  "type": "EventSocketSource",
                  "parents": [],
                  "arguments" : {
                  }
               },
               {
                  "name": "Test Socket Source2",
                  "id": 2,
                  "type": "EventSocketSource",
                  "parents": [],
                  "arguments" : {
                  }
               },
               {
                  "name": "TestComponentWithDefaultInputSingleton2",
                  "id": 4,
                  "type": "TestComponentWithDefaultInputSingleton2",
                  "parents": [{"parent": 2, "output": 0, "eventType":"Anomaly"}],
                  "arguments" : {
                 }
               },
               {
                  "name": "TestComponentWithDefaultInputSingleton2",
                  "id": 5,
                  "type": "TestComponentWithDefaultInputSingleton2",
                  "parents": [{"parent": 2, "output": 1, "eventType":"Model"}],
                  "arguments" : {
                 }
               }
              ]
          }
          """

    val flinkStreamingInfo = ReflexComponentFactory.getEngineFactory(ComputeEngineType.FlinkStreaming).asInstanceOf[ByClassComponentFactory]
    flinkStreamingInfo.registerComponent(classOf[com.parallelmachines.reflex.test.components.TestComponentWithDefaultInputSingleton1])
    flinkStreamingInfo.registerComponent(classOf[com.parallelmachines.reflex.test.components.TestComponentWithDefaultInputSingleton2])

    val res = DagTestUtil.parseGenerateValidate(json1)

    val nodeListWithSingletonComponent = res.nodeList.filter(_.component.name == "EventSocketSource")
    assert(nodeListWithSingletonComponent.size == 1)
    assert(nodeListWithSingletonComponent(0).component.inputTypes.size == 0)
    assert(nodeListWithSingletonComponent(0).component.outputTypes.size == 3)
    // Expected number of nodes: 7 (3 components itself + 3 defaults + 1 singleton
    assert(res.nodeList.length == 7, "wrong number of nodes in DAG after parsing")
  }

  "Pipeline with 2 components and 2 output singletons" should "be valid" in {
    val json1 =
      s"""
        {
        "name" : "ReflexPipeline_1",
        "engineType": "FlinkStreaming",
        "systemConfig" : {
                 "statsDBHost": "localhost",
                 "statsDBPort": 8086,
                 "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                 "statsMeasurementID": "1",
                 "modelFileSinkPath": "/tmp/tmpFile",
                 "mlObjectSocketHost": "localhost",
                 "mlObjectSocketSinkPort": 7777,
                 "healthStatFilePath": "/TMP/TESTPATH"
        },
        "pipe" : [

               {
                 "name": "null",
                 "id": 1,
                 "type": "ReflexNullSourceConnector",
                 "parents": []
               },
             {
               "name": "null",
               "id": 2,
               "type": "ReflexNullSourceConnector",
               "parents": []
               },
             {
                "name": "Component with singleton output",
                "id": 3,
                "type": "TestComponentWithDefaultOutputSingleton1",
                "parents": [{"parent": 1, "output": 0}],
                "arguments" : {
                }
            },
            {
               "name": "Component with singleton output",
               "id": 4,
               "type": "TestComponentWithDefaultOutputSingleton2",
               "parents": [{"parent": 2, "output": 0, "eventType":"Anomaly"}],
               "arguments" : {
               }
           }
            ]
        }
        """

    val flinkStreamingInfo = ReflexComponentFactory.getEngineFactory(ComputeEngineType.FlinkStreaming).asInstanceOf[ByClassComponentFactory]
    flinkStreamingInfo.registerComponent(classOf[com.parallelmachines.reflex.test.components.TestComponentWithDefaultOutputSingleton1])
    flinkStreamingInfo.registerComponent(classOf[com.parallelmachines.reflex.test.components.TestComponentWithDefaultOutputSingleton2])

    val res = DagTestUtil.parseGenerateValidate(json1)

    val nodeListWithSingletonComponent = res.nodeList.filter(_.component.name == "EventSocketSink")
    assert(nodeListWithSingletonComponent.size == 1)
    assert(nodeListWithSingletonComponent(0).component.inputTypes.size == 2)
    assert(nodeListWithSingletonComponent(0).component.outputTypes.size == 0)
    // Expected number of nodes: 5 (4 components itself + 1 singleton)
    assert(res.nodeList.length == 5, "wrong number of nodes in DAG after parsing")
  }

  "Pipeline with 2 components and 2 explicit output singletons" should "be valid" in {
    val json1 =
      s"""
        {
        "name" : "ReflexPipeline_1",
        "engineType": "FlinkStreaming",
        "systemConfig" : {
                 "statsDBHost": "localhost",
                 "statsDBPort": 8086,
                 "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                 "statsMeasurementID": "1",
                 "modelFileSinkPath": "/tmp/tmpFile",
                 "mlObjectSocketHost": "localhost",
                 "mlObjectSocketSinkPort": 7777,
                 "healthStatFilePath": "/TMP/TESTPATH"
        },
        "pipe" : [

               {
                 "name": "null",
                 "id": 1,
                 "type": "ReflexNullSourceConnector",
                 "parents": []
               },
             {
               "name": "null",
               "id": 2,
               "type": "ReflexNullSourceConnector",
               "parents": []
               },
              {
                "name": "null",
                "id": 3,
                "type": "ReflexNullSourceConnector",
                "parents": []
             },
             {
                "name": "Component with singleton output",
                "id": 4,
                "type": "TestComponentWithDefaultOutputSingleton1",
                "parents": [{
                  "parent": 1,
                  "output": 0
                }],
                "arguments" : {
                }
            },
            {
               "name": "Component with singleton output",
               "id": 5,
               "type": "TestComponentWithDefaultOutputSingleton2",
               "parents": [{"parent": 2, "output": 0}],
               "arguments" : {
               }
           },
          {
               "name": "Component with singleton output",
               "id": 6,
               "type": "TestComponentWithDefaultOutputSingleton2",
               "parents": [{
                           "parent": 3,
                           "output": 0
                         }],
               "arguments" : {
               }
           },
           {
             "name": "Test Socket Sink1",
             "id": 7,
             "type": "EventSocketSink",
             "parents": [{"parent": 4, "output": 0, "eventType":"Alert"}],
             "arguments" : {
             }
          },
          {
             "name": "Test Socket Sink 2",
             "id": 8,
             "type": "EventSocketSink",
             "parents": [{"parent": 5,"output": 0, "eventType":"Alert"}, {"parent": 6, "output": 0, "eventType":"Alert"}],
             "arguments" : {
             }
          }
            ]
        }
        """

    val flinkStreamingInfo = ReflexComponentFactory.getEngineFactory(ComputeEngineType.FlinkStreaming).asInstanceOf[ByClassComponentFactory]
    flinkStreamingInfo.registerComponent(classOf[com.parallelmachines.reflex.test.components.TestComponentWithDefaultOutputSingleton1])
    flinkStreamingInfo.registerComponent(classOf[com.parallelmachines.reflex.test.components.TestComponentWithDefaultOutputSingleton2])

    val res = DagTestUtil.parseGenerateValidate(json1)

    // Expected number of nodes: 5 (4 components itself + 1 singleton)
    val nodeListWithSingletonComponent = res.nodeList.filter(_.component.name == "EventSocketSink")
    assert(nodeListWithSingletonComponent.size == 1)
    assert(nodeListWithSingletonComponent(0).component.inputTypes.size == 3)
    assert(nodeListWithSingletonComponent(0).component.outputTypes.size == 0)
    // Expected number of nodes: 7 (6 components itself + 1 singleton)
    assert(res.nodeList.length == 7, "wrong number of nodes in DAG after parsing")
  }

  "Pipeline with sink singleton and lacking event type" should "throw Exception" in {
    val json1 =
      s"""
        {
        "name" : "ReflexPipeline_1",
        "engineType": "FlinkStreaming",
        "systemConfig" : {
                 "statsDBHost": "localhost",
                 "statsDBPort": 8086,
                 "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                 "statsMeasurementID": "1",
                 "modelFileSinkPath": "/tmp/tmpFile",
                 "mlObjectSocketHost": "localhost",
                 "mlObjectSocketSinkPort": 7777,
                 "healthStatFilePath": "/TMP/TESTPATH"
        },
        "pipe" : [

               {
                 "name": "null",
                 "id": 1,
                 "type": "ReflexNullSourceConnector",
                 "parents": []
               },

             {
                "name": "Component with singleton output",
                "id": 2,
                "type": "TestComponentWithDefaultOutputSingleton1",
                "parents": [{
                  "parent": 1,
                  "output": 0
                }],
                "arguments" : {
                }
            },
          {
             "name": "Test Socket Sink",
             "id": 3,
             "type": "EventSocketSink",
             "parents": [{"parent": 2,"output": 0}],
             "arguments" : {
             }
          }
            ]
        }
        """

    val flinkStreamingInfo = ReflexComponentFactory.getEngineFactory(ComputeEngineType.FlinkStreaming).asInstanceOf[ByClassComponentFactory]
    flinkStreamingInfo.registerComponent(classOf[com.parallelmachines.reflex.test.components.TestComponentWithDefaultOutputSingleton1])

    val ex = intercept[Exception] {
      val res = DagTestUtil.parseGenerateValidate(json1)
    }
    assert(ex.getMessage.contains("eventType must be defined in EventSocketSink for input: {parent: 2, output: 0}"))
  }

  "Pipeline with source singleton and lacking event type" should "throw Exception" in {
    val json1 =
      s"""
          {
          "name" : "ReflexPipeline_1",
          "engineType": "FlinkStreaming",
          "systemConfig" : {
                   "statsDBHost": "localhost",
                   "statsDBPort": 8086,
                   "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                   "statsMeasurementID": "1",
                   "modelFileSinkPath": "/tmp/tmpFile",
                   "mlObjectSocketHost": "localhost",
                   "mlObjectSocketSourcePort": 7777,
                   "healthStatFilePath": "/TMP/TESTPATH"
          },
          "pipe" : [
                {
                  "name": "Test Socket Source1",
                  "id": 1,
                  "type": "EventSocketSource",
                  "parents": [],
                  "arguments" : {
                  }
               },
               {
                  "name": "TestComponentWithDefaultInputSingleton1",
                  "id": 2,
                  "type": "TestComponentWithDefaultInputSingleton1",
                  "parents": [{"parent": 1, "output": 0}],
                  "arguments" : {
                 }
               }
              ]
          }
          """

    val flinkStreamingInfo = ReflexComponentFactory.getEngineFactory(ComputeEngineType.FlinkStreaming).asInstanceOf[ByClassComponentFactory]
    flinkStreamingInfo.registerComponent(classOf[com.parallelmachines.reflex.test.components.TestComponentWithDefaultInputSingleton1])

    val ex = intercept[Exception] {
      val res = DagTestUtil.parseGenerateValidate(json1)
    }
    assert(ex.getMessage.contains("eventType must be defined in TestComponentWithDefaultInputSingleton1 for input: {parent: 1, output: 0}"))
  }
}
