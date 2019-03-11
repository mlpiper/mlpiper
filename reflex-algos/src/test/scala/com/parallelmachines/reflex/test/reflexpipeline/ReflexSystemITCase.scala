package com.parallelmachines.reflex.test.reflexpipeline

import com.google.protobuf.ByteString
import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent
import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent.EventType
import org.junit.runner.RunWith
import org.mlpiper.infrastructure.{Base64Wrapper, DagGen}
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

@RunWith(classOf[JUnitRunner])
class ReflexSystemITCase extends FlatSpec with TestEnv with Matchers {

  "Good Pipeline 1" should "be valid end to end" in {

    val json =
      s"""
           {
            "name" : "ReflexPipeline_1",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID": "1",
                    "modelFileSinkPath": "/tmp/tmpFile",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "From string collection",
                    "id": 1,
                    "type": "FromStringCollection",
                    "parents": [],
                    "arguments" : {
                        "samples": ["1", "2", "3", "4", "5", "2", "3", "1", "1", "5", "5"]
                    }
                },
                {
                    "name": "Save to file",
                    "id": 2,
                    "type": "SaveToFile",
                    "arguments" : {
                        "filepath" : "${outTmpFilePath}"
                    },
                    "parents": [{"parent": 1, "output": 0}]
                }
            ]
           }
  """.stripMargin
    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    DagGen.main(args)
  }

  "Good Pipeline 1a - Base64" should "be valid end to end" in {

    val json =
      s"""
           {
            "name" : "ReflexPipeline_1",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID": "1",
                    "modelFileSinkPath": "/tmp/tmpFile",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "From string collection",
                    "id": 1,
                    "type": "FromStringCollection",
                    "parents": [],
                    "arguments" : {
                        "samples": ["1", "2", "3", "4", "5", "2", "3", "1", "1", "5", "5"]
                    }
                },
                {
                    "name": "Save to file",
                    "id": 2,
                    "type": "SaveToFile",
                    "arguments" : {
                        "filepath" : "${outTmpFilePath}"
                    },
                    "parents": [{"parent": 1, "output": 0}]
                }
            ]
           }
  """.stripMargin

    val args = Array[String]("--pipe-str-base64", Base64Wrapper.encode(json),
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    DagGen.main(args)
  }


  "Good Pipeline 2 - long lasting operation" should "be valid end to end" in {

    val json =
      s"""
           {
            "name" : "ReflexPipeline_1",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID": "1",
                    "modelFileSinkPath": "/tmp/tmpFile",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "From string collection",
                    "id": 1,
                    "type": "FromStringCollection",
                    "parents": [],
                    "arguments" : {
                        "samples": ["1", "2", "3", "4", "5", "2", "3", "1", "1", "5", "5"]
                    }
                },
                {
                    "name": "Test long lasting operation",
                    "id": 2,
                    "type": "TestLongLastingOperation",
                    "arguments" : {
                        "delay" : 50
                    },
                    "parents": [{"parent": 1, "output": 0}]
                },
                {
                    "name": "Test stdout sink",
                    "id": 3,
                    "type": "TestStdoutSink",
                    "parents": [{"parent": 2, "output": 0}]
                }
            ]
           }
  """.stripMargin
    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--test-mode",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    DagGen.main(args)
  }

  "Good Pipeline 3 - accumulators" should "be valid end to end" in {

    val json =
      s"""
           {
            "name" : "ReflexPipeline_1",
            "engineType": "SparkBatch",
            "systemConfig" : {
                    "statsDBHost": "localhost",
                    "statsDBPort": 8086,
                    "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                    "statsMeasurementID": "1",
                    "modelFileSinkPath": "/tmp/tmpFile",
                    "healthStatFilePath": "/tmp/tmpHealthFile"
                 },
            "pipe" : [
                {
                    "name": "From string collection",
                    "id": 1,
                    "type": "FromStringCollection",
                    "parents": [],
                    "arguments" : {
                        "samples": ["1", "2", "3", "4", "5", "2", "3", "1", "1", "5", "5"]
                    }
                },
                {
                    "name": "Test accumulators",
                    "id": 2,
                    "type": "TestAccumulators",
                    "arguments" : {
                        "numIters" : 100,
                        "sleepMsec" : 100
                    },
                    "parents": [{"parent": 1, "output": 0}]
                },
                {
                    "name": "Test stdout sink",
                    "id": 3,
                    "type": "TestStdoutSink",
                    "parents": [{"parent": 2, "output": 0}]
                }
            ]
           }
  """.stripMargin
    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--test-mode",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    DagGen.main(args)
  }

  "Good Pipeline 4 - Event Socket Sink" should "be valid end to end" in {

    val readServer = new HealthEventWriteReadSocketServer(null, writeMode = false)

    val readServerThread = new Thread {
      override def run(): Unit = {
        readServer.run
      }
    }

    val sinkPort = readServer.port

    val json =
      s"""
          {
           "name" : "ReflexPipeline_1",
           "engineType": "SparkBatch",
           "systemConfig" : {
                   "statsDBHost": "localhost",
                   "statsDBPort": 8086,
                   "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                   "statsMeasurementID": "1",
                   "modelFileSinkPath": "/tmp/tmpFile",
                   "mlObjectSocketHost": "localhost",
                   "mlObjectSocketSinkPort": $sinkPort,
                   "healthStatFilePath": "/tmp/tmpHealthFile"
                },
           "pipe" : [
               {
                   "name": "From string collection",
                   "id": 1,
                   "type": "FromStringCollection",
                   "parents": [],
                   "arguments" : {
                       "samples": ["1", "2", "3", "4", "5", "2", "3", "1", "1", "5", "5"]
                   }
               },
               {
                   "name": "Event Socket Sink",
                   "id": 2,
                   "type": "EventSocketSink",
                   "parents": [{"parent": 1, "output": 0, "eventType":"Model"}]
               }
           ]
          }
  """.stripMargin
    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    readServerThread.start()

    DagGen.main(args)

    assert(readServer.outputList.size == 11)
    readServerThread.join()
  }

  "Good Pipeline 5 - Event Socket Source" should "be valid end to end" in {

    val inputs = List(ReflexEvent(EventType.Model, None, ByteString.copyFrom("data1".map(_.toByte).toArray)),
      ReflexEvent(EventType.MLHealthModel, None, ByteString.copyFrom("data2".map(_.toByte).toArray)),
      ReflexEvent(EventType.MLHealthModel, None, ByteString.copyFrom("data4".map(_.toByte).toArray)),
      ReflexEvent(EventType.Model, Some("model_label"), ByteString.copyFrom("data3".map(_.toByte).toArray)))

    val socketServerSource = new HealthEventWriteReadSocketServer(inputs)
    val sourceServerThread = new Thread {
      override def run(): Unit = {
        socketServerSource.run
      }
    }

    val sourcePort = socketServerSource.port

    val json =
      s"""
          {
           "name" : "ReflexPipeline_1",
           "engineType": "SparkBatch",
           "systemConfig" : {
                   "statsDBHost": "localhost",
                   "statsDBPort": 8086,
                   "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                   "statsMeasurementID": "1",
                   "modelFileSinkPath": "/tmp/tmpFile",
                   "mlObjectSocketHost": "localhost",
                   "mlObjectSocketSourcePort": $sourcePort,
                   "healthStatFilePath": "/tmp/tmpHealthFile"
                },
           "pipe" : [
               {
                   "name": "Event Socket Sink",
                   "id": 1,
                   "type": "EventSocketSource",
                   "parents": []
               },
               {
                   "name": "ReflexNullConnector",
                   "id": 2,
                   "type": "ReflexNullConnector",
                   "parents": [{"parent": 1, "output": 0, "input": 0, "eventType": "${EventType.Model.toString}", "eventLabel": "model_label"}]
               },
                               {
                   "name": "ReflexNullConnector",
                   "id": 3,
                   "type": "ReflexNullConnector",
                   "parents": [{"parent": 1, "output": 1, "input": 0, "eventType": "${EventType.MLHealthModel.toString}"}]
               }
           ]
          }
  """.stripMargin
    val args = Array[String]("--pipe-str", json.filter(_ > ' '), // An effective way to dispose of all white-spaces, including cr and nl.
      "--master", "local[*]",
      "--conf", "spark.executor.heartbeatInterval=10000000,spark.network.timeout=10000000",
      "--rest-server-port", "0")
    sourceServerThread.start()

    noException should be thrownBy DagGen.main(args)
    sourceServerThread.join()
  }
}
