package com.parallelmachines.reflex.components.flink.streaming.connectors

import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponent
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, ConnectionList, _}
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class ReflexNullSourceConnector extends FlinkStreamingComponent {
  val isSource = true

  override val group: String = ComponentsGroups.connectors
  override val label = "Null"
  override val description = "Stub for an input stream"
  override val version = "1.0.0"
  override lazy val paramInfo: String =
    """[]""".stripMargin

  override val inputTypes: ConnectionList = ConnectionList.empty()

  val output = ComponentConnection(
    tag = typeTag[Any],
    label = "Data",
    description = "Empty data",
    group = ConnectionGroups.DATA)

  override var outputTypes: ConnectionList = ConnectionList(output)

  override def configure(paramMap: Map[String, Any]): Unit = {
  }

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {

    val emptyStream = new RichSourceFunction[Any] {
      override def run(ctx: SourceContext[Any]): Unit = {
        ctx.close()
      }

      override def cancel(): Unit = {
        /* Do nothing. */
      }
    }

    val emptySourceStream = env.addSource(emptyStream)
    ArrayBuffer(new DataWrapper(emptySourceStream))
  }
}



