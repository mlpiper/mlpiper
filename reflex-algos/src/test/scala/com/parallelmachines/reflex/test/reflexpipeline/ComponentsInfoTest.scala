package com.parallelmachines.reflex.test.reflexpipeline

import com.parallelmachines.reflex.components._
import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponentFactory
import com.parallelmachines.reflex.factory.{ByClassComponentFactory, ReflexComponentFactory}
import com.parallelmachines.reflex.pipeline.{ComputeEngineType, DagGen}
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._
import org.junit.runner.RunWith
import org.junit.{After, Before, Test}
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class ComponentsInfoTest extends FlatSpec with Matchers {

  @Before
  def init() {
    DagTestUtil.initComponentFactory()
  }

  @After
  def cleanUp() {
    ReflexComponentFactory.cleanup()
  }

  @Test
  def testInfoJSON(): Unit = {
    noException should be thrownBy DagGen.generateComponentInfoJSON()
  }

  @Test
  def testComponentSignature(): Unit = {
    DagTestUtil.initComponentFactory()
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

    val flinkStreamingInfo = ReflexComponentFactory.getEngineFactory(ComputeEngineType.FlinkStreaming).asInstanceOf[ByClassComponentFactory]
    val testCompCl = classOf[com.parallelmachines.reflex.test.components.TestAlgoComponent]
    flinkStreamingInfo.registerComponent(testCompCl)

    val compJson = ReflexComponentFactory.componentSignature(ComputeEngineType.FlinkStreaming, testCompCl.getSimpleName)

    val compMap = parse(compJson).extract[Map[String, JValue]]
    assert(compMap.contains("isVisible"))
    assert(!compMap("isVisible").extract[Boolean])

    assert(compMap.contains("outputInfo"))
    val outputTypes = compMap("outputInfo").extract[List[JValue]]
    assert(outputTypes.size == 3)

    val outputType = outputTypes(2).extract[Map[String, JValue]]
    assert(outputType.contains("isVisible"))
    assert(!outputType("isVisible").extract[Boolean])
  }

  @Test
  def testComponentUniqueInputLabels(): Unit = {
    val fs = ReflexComponentFactory.getEngineFactory(ComputeEngineType.FlinkStreaming).asInstanceOf[FlinkStreamingComponentFactory]
    an[Exception] should be thrownBy fs.registerComponent(classOf[flink.streaming.dummy.TestComponentInputsWithSameLabels])
  }

  @Test
  def testComponentUniqueOutputLabels(): Unit = {
    val fs = ReflexComponentFactory.getEngineFactory(ComputeEngineType.FlinkStreaming).asInstanceOf[FlinkStreamingComponentFactory]
    an[Exception] should be thrownBy fs.registerComponent(classOf[flink.streaming.dummy.TestComponentOutputsWithSameLabels])
  }

  @Test
  def testRegisterTwice(): Unit = {
    val fs = ReflexComponentFactory.getEngineFactory(ComputeEngineType.FlinkStreaming).asInstanceOf[FlinkStreamingComponentFactory]
    an[Exception] should be thrownBy fs.registerComponent(classOf[flink.streaming.dummy.TestArgsComponent])
  }

  @Test
  def testRegisterSameSimpleNameComponent(): Unit = {
    val fs = ReflexComponentFactory.getEngineFactory(ComputeEngineType.FlinkStreaming).asInstanceOf[FlinkStreamingComponentFactory]
    an[Exception] should be thrownBy fs.registerComponent(classOf[flink.streaming.dummy.TwoDup])
  }
}
