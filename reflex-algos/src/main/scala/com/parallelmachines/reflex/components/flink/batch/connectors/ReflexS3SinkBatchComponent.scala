package com.parallelmachines.reflex.components.flink.batch.connectors

import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.components.flink.batch.FlinkBatchComponent
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.scala.examples.common.serialize.S3SinkForFlink

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class ReflexS3SinkBatchComponent extends FlinkBatchComponent {
  override val isSource = false
  override val group: String = ComponentsGroups.sinks
  override val label = "S3"
  override val description = "Sends data to provided Amazon's S3"
  override val version = "1.0.0"

  val input = ComponentConnection(
    tag = typeTag[Any],
    label = "Data",
    description = "Data to sink",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(input)
  override var outputTypes: ConnectionList = ConnectionList.empty()

  val accessKey: ComponentAttribute.ComponentAttribute[String] = ComponentAttribute("accessKey", "", "S3 Access Key", "S3 access key").setValidator(!_.isEmpty)
  val secretAccessKey: ComponentAttribute.ComponentAttribute[String] = ComponentAttribute("secretAccessKey", "", "S3 Secret Access Key", "S3 secret access key").setValidator(!_.isEmpty)
  val region: ComponentAttribute.ComponentAttribute[String] = ComponentAttribute("region", "", "S3 Region", "Component will sink input data to provided S3 region").setValidator(!_.isEmpty)
  val objectName: ComponentAttribute.ComponentAttribute[String] = ComponentAttribute("objectName", "", "Object Name To Sink Data", "Component will sink input data to provided object name").setValidator(!_.isEmpty)
  val bucketName: ComponentAttribute.ComponentAttribute[String] = ComponentAttribute("bucketName", "", "S3 Bucket Name", "Component will sink input data to provided bucket").setValidator(!_.isEmpty)
  val maxPoint: ComponentAttribute.ComponentAttribute[Int] =
    ComponentAttribute("maxPointToWriteInObject", -1, "Maximum point to write to single object (Provide -1 for dumping whole data to single object)", "Number of max points to write to given object", optional = true)

  attrPack.add(bucketName, accessKey, secretAccessKey, region, objectName, maxPoint)

  override def materialize(env: ExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {
    val s3SinkObject = new S3SinkForFlink[Any](
      objectName = objectName.value,
      bucketName = bucketName.value,
      accessKey = accessKey.value,
      secretAccessKey = secretAccessKey.value,
      region = region.value,
      maxPointToWrite = maxPoint.value)

    dsArr(0)
      .data[DataSet[Any]]()
      .output(s3SinkObject)
      .setParallelism(1)

    ArrayBuffer[DataWrapperBase]()
  }
}



