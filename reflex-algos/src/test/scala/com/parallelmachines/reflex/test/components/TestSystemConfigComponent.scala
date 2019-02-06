package com.parallelmachines.reflex.test.components

import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponent
import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.pipeline._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.collection.mutable.ArrayBuffer

class TestSystemConfigComponent extends FlinkStreamingComponent {
  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.featureEng
  override val label: String = "Test args component"
  override val description: String = "Test configuration parameters"
  override val version: String = "1.0.0"

  override val inputTypes: ConnectionList = ConnectionList.empty()
  override var outputTypes: ConnectionList = ConnectionList.empty()

  val pathArg = ComponentAttribute("healthStatFilePath", "", "healthStatFilePath arg from system config", "healthStatFilePath arg from system config")

  attrPack.add(pathArg)

  override def configure(paramMap: Map[String, Any]): Unit = {
    super.configure(paramMap)

    val testValue = "/TMP/TESTPATH"
    require(pathArg.value == testValue, s"healthStatFilePath is not provided in system config or != '${testValue}'")
  }

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {
    return ArrayBuffer[DataWrapperBase]()
  }
}