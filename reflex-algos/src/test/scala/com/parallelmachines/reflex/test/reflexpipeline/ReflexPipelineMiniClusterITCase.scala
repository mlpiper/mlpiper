package com.parallelmachines.reflex.test.reflexpipeline

import java.io._
import java.net._

import com.google.protobuf.{ByteString, InvalidProtocolBufferException}
import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent
import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent.EventType
import com.parallelmachines.reflex.factory.{ByClassComponentFactory, ReflexComponentFactory}
import com.parallelmachines.reflex.pipeline._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.test.exampleScalaPrograms.clustering.{AllJobStopper, PMMiniCluster}
import org.junit.Test
import org.scalatest.Matchers

import scala.collection.mutable


class HealthEventWriteReadSocketServer(inputItems: List[ReflexEvent], writeMode: Boolean = true, var waitExplicitStop: Boolean = false) {
  val server: ServerSocket = new ServerSocket(0)
  val port: Int = server.getLocalPort
  val outputList = new mutable.MutableList[ReflexEvent]

  def run(): Unit = {
    var sock: Socket = null
    do {
      var isConnected = false

      try {
        sock = server.accept()
        sock.setSoTimeout(100)
        isConnected = true
      } catch {
        case _: java.net.SocketException =>
          isConnected = false
      }

      if (writeMode) {
        for (item <- inputItems) {
          item.writeDelimitedTo(sock.getOutputStream)
        }
      } else {
        while (isConnected) {
          try {
            val out = ReflexEvent.parseDelimitedFrom(sock.getInputStream)
            if (out.isDefined) {
              val inData = out.get
              outputList += inData
            } else {
              isConnected = false
            }
          } catch {
            case _ | _: SocketTimeoutException =>
            case e: InvalidProtocolBufferException =>
              isConnected = false
              throw e
            case e: java.io.EOFException =>
              isConnected = false
              throw e
          }
        }
      }
      sock.close()
    } while (waitExplicitStop)
  }

  def forceStop(): Unit = {
    waitExplicitStop = false
  }
}

class ReflexPipelineMiniClusterITCase extends PMMiniCluster with Matchers {

  @Test
  def testSingletonSocketSource() {

    val inputs = List(ReflexEvent(EventType.Model, None, ByteString.copyFrom("data1".map(_.toByte).toArray)),
      ReflexEvent(EventType.MLHealthModel, None, ByteString.copyFrom("data2".map(_.toByte).toArray)),
      ReflexEvent(EventType.Model, Some("model_label"), ByteString.copyFrom("data1".map(_.toByte).toArray)),
      ReflexEvent(EventType.Anomaly, None, ByteString.copyFrom("data1".map(_.toByte).toArray)))

    val socketServerSource = new HealthEventWriteReadSocketServer(inputs)
    val sourceServerThread = new Thread {
      override def run(): Unit = {
        socketServerSource.run()
      }
    }

    val sourcePort = socketServerSource.port

    val json1 =
      s"""
        {
        "name" : "ReflexPipeline_1",
        "engineType": "FlinkStreaming",
        "systemConfig" : {
                 "statsDBHost": "localhost",
                 "statsDBPort": 8086,
                 "mlObjectSocketHost": "localhost",
                 "mlObjectSocketSourcePort": $sourcePort,
                 "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                 "statsMeasurementID": "1",
                 "modelFileSinkPath": "/tmp/tmpFile"
        },
        "pipe" : [
                   {
                      "name": "Event receiver 1",
                      "id": 1,
                      "type": "TestComponentWithDefaultInputSingleton1",
                      "parents": [],
                      "arguments" : {
                      }
                  },
                  {
                     "name": "Event receiver 2",
                     "id": 2,
                     "type": "TestComponentWithDefaultInputSingleton2",
                     "parents": [],
                     "arguments" : {
                     }
                 },
                 {
                     "name": "twounion",
                     "id": 3,
                     "type": "ReflexTwoUnionComponent",
                     "arguments" : {
                     },
                     "parents": [{"parent": 1, "output": 0}, {"parent": 2, "output": 0}]
                 },
                 {
                   "name": "Collector",
                   "id": 4,
                   "type": "ReflexCollectConnector",
                   "parents": [
                     {
                       "parent": 3,
                       "output": 0
                     }
                   ],
                   "arguments": {
                     "resultKey": "result_output1"
                   }
                 }
           ]
        }
        """.stripMargin

    sourceServerThread.start()

    DagTestUtil.initComponentFactory()
    CollectedData.clear

    val flinkStreamingInfo = ReflexComponentFactory.getEngineFactory(ComputeEngineType.FlinkStreaming).asInstanceOf[ByClassComponentFactory]
    flinkStreamingInfo.registerComponent(classOf[com.parallelmachines.reflex.test.components.TestComponentWithDefaultInputSingleton1])
    flinkStreamingInfo.registerComponent(classOf[com.parallelmachines.reflex.test.components.TestComponentWithDefaultInputSingleton2])

    val reflexPipe = new ReflexPipelineBuilder().buildPipelineFromJson(json1)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    AllJobStopper.start(env, this.cluster, 5000L)

    // execute will be called by Collect component
    reflexPipe.materialize(EnvironmentWrapper(env))

    AllJobStopper.stop()
    assert(CollectedData.get("result_output1").size == 2)
    ReflexComponentFactory.cleanup()
    sourceServerThread.join()
    socketServerSource.server.close()
  }

  @Test
  def testSingletonSocketSink() {

    val inputs = List("Data message 1", "Data message 2", "Data message 1", "Data message 2")

    val file = File.createTempFile("samples", ".tmp")
    file.deleteOnExit()
    val bw = new PrintWriter(new FileWriter(file))

    for (l <- inputs) {
      bw.println(l)
    }
    bw.close()

    val fileName = file.getAbsolutePath

    val readServer = new HealthEventWriteReadSocketServer(null, writeMode = false)
    val readServerThread = new Thread {
      override def run(): Unit = {
        readServer.run()
      }
    }
    val sourcePort = readServer.port

    val json1 =
      s"""
        {
        "name" : "ReflexPipeline_1",
        "engineType": "FlinkStreaming",
        "systemConfig" : {
                 "statsDBHost": "localhost",
                 "statsDBPort": 8086,
                 "mlObjectSocketHost": "",
                 "mlObjectSocketSinkPort": $sourcePort,
                 "workflowInstanceId": "8117aced55d7427e8cb3d9b82e4e26ac",
                 "statsMeasurementID": "1",
                 "modelFileSinkPath": "/tmp/tmpFile"
        },
        "pipe" : [
                   {
                       "name": "File",
                       "id": 1,
                       "type": "ReflexFileConnector",
                       "parents": [],
                       "arguments" : {
                         "fileName" : "$fileName"
                       }
                   },
                   {
                       "name": "File",
                       "id": 2,
                       "type": "ReflexFileConnector",
                       "parents": [],
                       "arguments" : {
                         "fileName" : "$fileName"
                       }
                   },
                   {
                       "name": "File",
                       "id": 3,
                       "type": "ReflexFileConnector",
                       "parents": [],
                       "arguments" : {
                         "fileName" : "$fileName"
                       }
                   },
                   {
                      "name": "Event producer 1",
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
                     "name": "Event producer 2",
                     "id": 5,
                     "type": "TestComponentWithDefaultOutputSingleton2",
                     "parents": [{
                         "parent": 2,
                         "output": 0
                       }],
                     "arguments" : {
                     }
                 },
                 {
                    "name": "Event producer 2",
                    "id": 6,
                    "type": "TestComponentWithDefaultOutputSingleton2",
                    "parents": [{
                        "parent": 3,
                        "output": 0
                      }],
                    "arguments" : {
                    }
                }
           ]
        }
        """.stripMargin

    readServerThread.start()

    DagTestUtil.initComponentFactory()
    val flinkStreamingInfo = ReflexComponentFactory.getEngineFactory(ComputeEngineType.FlinkStreaming).asInstanceOf[ByClassComponentFactory]
    flinkStreamingInfo.registerComponent(classOf[com.parallelmachines.reflex.test.components.TestComponentWithDefaultOutputSingleton1])
    flinkStreamingInfo.registerComponent(classOf[com.parallelmachines.reflex.test.components.TestComponentWithDefaultOutputSingleton2])

    val reflexPipe = new ReflexPipelineBuilder().buildPipelineFromJson(json1)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    AllJobStopper.start(env, this.cluster, 5000L)

    reflexPipe.materialize(EnvironmentWrapper(env))
    env.execute(reflexPipe.pipeInfo.name)

    AllJobStopper.stop()

    ReflexComponentFactory.cleanup()

    readServerThread.join()

    assert(readServer.outputList.length == inputs.length * 3)
    readServer.server.close()
  }
}
