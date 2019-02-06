package com.parallelmachines.reflex.test.reflexpipeline

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.Matchers
import com.parallelmachines.reflex.pipeline._

import scala.collection.mutable
import java.net._
import java.io._

import com.google.protobuf.{ByteString, InvalidProtocolBufferException}
import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent
import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent.EventType
import com.parallelmachines.reflex.factory.{ByClassComponentFactory, ReflexComponentFactory}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.test.exampleScalaPrograms.clustering.{AllJobStopper, PMMiniCluster}
import org.junit.Test
import scala.collection.mutable.ListBuffer


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
            case e@(_: java.net.SocketException | _: SocketTimeoutException) =>
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

  @Test
  def testBatchEventSocketSinkAPI() {

    val inputs1 = List("Data message 1", "Data message 2", "Data message 3", "Data message 4")
    val inputs2 = List("Data message 5", "Data message 6")

    val file1 = File.createTempFile("samples1", ".tmp")
    val file2 = File.createTempFile("samples2", ".tmp")
    file1.deleteOnExit()
    file2.deleteOnExit()
    val bw1 = new PrintWriter(new FileWriter(file1))
    val bw2 = new PrintWriter(new FileWriter(file2))

    for (l <- inputs1) {
      bw1.println(l)
    }
    bw1.close()

    for (l <- inputs2) {
      bw2.println(l)
    }
    bw2.close()

    val fileName1 = file1.getAbsolutePath
    val fileName2 = file2.getAbsolutePath

    val readServer = new HealthEventWriteReadSocketServer(null, writeMode = false, waitExplicitStop = true)

    val readServerThread = new Thread {
      override def run(): Unit = {
        readServer.run()
      }
    }

    val sinkPort = readServer.port

    readServerThread.start()

    DagTestUtil.initComponentFactory()
    CollectedData.clear

    val flinkBatchInfo = ReflexComponentFactory.getEngineFactory(ComputeEngineType.FlinkBatch).asInstanceOf[ByClassComponentFactory]
    flinkBatchInfo.registerComponent(classOf[com.parallelmachines.reflex.test.components.TestBatchAlgoComponent])

    val testPipeInfo = ReflexPipelineInfo()
    testPipeInfo.engineType = ComputeEngineType.FlinkBatch
    testPipeInfo.systemConfig.statsDBHost = "localhost"
    testPipeInfo.systemConfig.statsDBPort = 8086
    testPipeInfo.systemConfig.mlObjectSocketHost = Some("localhost")
    testPipeInfo.systemConfig.mlObjectSocketSinkPort = Some(sinkPort)
    testPipeInfo.systemConfig.workflowInstanceId = "8117aced55d7427e8cb3d9b82e4e26ac"
    testPipeInfo.systemConfig.statsMeasurementID = "1"
    testPipeInfo.systemConfig.modelFileSinkPath = "/tmp/tmpFile"

    testPipeInfo.addComponent(Component("FlinkBatchFileConnector1", 1, "FlinkBatchFileConnector", ListBuffer[Parent](), Some(Map[String, Any]("fileName" -> fileName1))))
    testPipeInfo.addComponent(Component("FlinkBatchFileConnector1", 2, "FlinkBatchFileConnector", ListBuffer[Parent](), Some(Map[String, Any]("fileName" -> fileName2))))
    testPipeInfo.addComponent(Component("TestBatchAlgoComponent", 3, "TestBatchAlgoComponent", ListBuffer[Parent](Parent(1, 0, None, None, None), Parent(2, 0, None, None, None)), None))
    testPipeInfo.addComponent(Component("EventSocketSink", 4, "EventSocketSink", ListBuffer[Parent](Parent(3, 0, Some(0), Some(ReflexEvent.EventType.Model.toString()), None)), None))
    testPipeInfo.addComponent(Component("EventSocketSink", 5, "EventSocketSink", ListBuffer[Parent](Parent(3, 1, Some(1), Some(ReflexEvent.EventType.MLHealthModel.toString()), None)), None))

    val pipelineBuilder = new ReflexPipelineBuilder()
    val reflexPipe = pipelineBuilder.buildPipelineFromInfo(testPipeInfo)

    assert(reflexPipe.nodeList.size == 4)

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    reflexPipe.materialize(EnvironmentWrapper(env))
    env.execute(reflexPipe.pipeInfo.name)

    Thread.sleep(500)
    readServer.forceStop()

    assert(readServer.outputList.size == 6)
    assert(readServer.outputList.count(_.eventType == EventType.Model) == 4)
    assert(readServer.outputList.count(_.eventType == EventType.MLHealthModel) == 2)

    ReflexComponentFactory.cleanup()
    readServerThread.join(1000)
    readServer.server.close()
  }

  @Test
  def testModelRecivedConfirmator() {
    val inputs = List(ReflexEvent(EventType.Model, None, ByteString.copyFrom("data1".map(_.toByte).toArray), Some("1234")),
      ReflexEvent(EventType.MLHealthModel, Some("b"), ByteString.copyFrom("data2".map(_.toByte).toArray)),
      ReflexEvent(EventType.Model, None, ByteString.copyFrom("data1".map(_.toByte).toArray), Some("5678")))

    val writeServer = new HealthEventWriteReadSocketServer(inputs)
    val readServer = new HealthEventWriteReadSocketServer(null, writeMode = false)

    val writeServerThread = new Thread {
      override def run(): Unit = {
        writeServer.run()
      }
    }

    val readServerThread = new Thread {
      override def run(): Unit = {
        readServer.run()
      }
    }

    val sourcePort = writeServer.port
    val sinkPort = readServer.port

    writeServerThread.start()
    readServerThread.start()

    DagTestUtil.initComponentFactory()
    CollectedData.clear

    val flinkStreamingInfo = ReflexComponentFactory.getEngineFactory(ComputeEngineType.FlinkStreaming).asInstanceOf[ByClassComponentFactory]
    flinkStreamingInfo.registerComponent(classOf[com.parallelmachines.reflex.test.components.TestAlgoComponent])

    val testAlgoPipeInfo = ReflexPipelineInfo()
    testAlgoPipeInfo.addComponent(Component("EventSocketSource", testAlgoPipeInfo.getMaxId + 1, "EventSocketSource", ListBuffer[Parent](), None))
    testAlgoPipeInfo.addComponent(Component("TestAlgoComponent", testAlgoPipeInfo.getMaxId + 1, "TestAlgoComponent", ListBuffer[Parent](Parent(testAlgoPipeInfo.getMaxId, 0, Some(0), Some("Model"), None)), None))

    testAlgoPipeInfo.systemConfig.statsDBHost = "localhost"
    testAlgoPipeInfo.systemConfig.statsDBPort = 8086
    testAlgoPipeInfo.systemConfig.mlObjectSocketHost = Some("localhost")
    testAlgoPipeInfo.systemConfig.mlObjectSocketSourcePort = Some(sourcePort)
    testAlgoPipeInfo.systemConfig.mlObjectSocketSinkPort = Some(sinkPort)
    testAlgoPipeInfo.systemConfig.workflowInstanceId = "8117aced55d7427e8cb3d9b82e4e26ac"
    testAlgoPipeInfo.systemConfig.statsMeasurementID = "1"
    testAlgoPipeInfo.systemConfig.modelFileSinkPath = "/tmp/tmpFile"


    val pipelineBuilder = new ReflexPipelineBuilder()
    val reflexPipe = pipelineBuilder.buildPipelineFromInfo(testAlgoPipeInfo)

    assert(reflexPipe.nodeList.size == 7)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    AllJobStopper.start(env, this.cluster, 5000L)

    reflexPipe.materialize(EnvironmentWrapper(env))
    env.execute(reflexPipe.pipeInfo.name)

    AllJobStopper.stop()
    ReflexComponentFactory.cleanup()
    writeServerThread.join()
    readServerThread.join()

    assert(readServer.outputList.length == 2)
    assert(readServer.outputList.count(_.eventType == EventType.ModelAccepted) == 2)
    assert(readServer.outputList.map(_.modelId.get).contains("1234"))
    assert(readServer.outputList.map(_.modelId.get).contains("5678"))

    readServer.server.close()
    writeServer.server.close()
  }

}