package com.parallelmachines.reflex.components.flink.streaming.dummy

import com.parallelmachines.reflex.components.flink.streaming.{FlinkStreamingComponent, StreamExecutionEnvironment}
import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.pipeline._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class TestComponentWithDefaultInputAndSide1 extends FlinkStreamingComponent {
  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.featureEng
  override val label: String = "Test Default Input"
  override val description: String = "Testing component with default input"
  override val version: String = "1.0.0"

  val input = ComponentConnection(
    tag = typeTag[Any],
    defaultComponentClass = Some(classOf[TestComponentSource]),
    sideComponentClass = Some(classOf[TestNullConnector]),
    label = "Input",
    description = "Input",
    group = ConnectionGroups.OTHER)

  override val inputTypes: ConnectionList = ConnectionList(input)
  override var outputTypes: ConnectionList = ConnectionList.empty()

  val pathArg = ComponentAttribute("healthStatFilePath", "", "healthStatFilePath arg from system config", "healthStatFilePath arg from system config")

  attrPack.add(pathArg)

  override def configure(paramMap: Map[String, Any]): Unit = {
    super.configure(paramMap)

    val testValue = "/TMP/TESTPATH"
    require(pathArg.value == testValue, s"healthStatFilePath is not provided in system config or != '${testValue}'")
  }

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {
    return ArrayBuffer[DataWrapperBase]()
  }
}
