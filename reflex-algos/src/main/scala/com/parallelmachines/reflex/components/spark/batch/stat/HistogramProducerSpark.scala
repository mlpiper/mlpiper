package com.parallelmachines.reflex.components.spark.batch.stat

import breeze.linalg.{DenseVector => BreezeDenseVector}
import com.parallelmachines.reflex.common._
import com.parallelmachines.reflex.components.spark.batch.SparkBatchComponent
import com.parallelmachines.reflex.pipeline.{ComponentsGroups, ConnectionGroups, _}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class HistogramProducerSpark extends SparkBatchComponent {
  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.stat
  override val label: String = this.getClass.getSimpleName
  override val description: String = "Component calculates histogram metrics (or statistics)"
  override val version: String = "1.0.0"
  override lazy val isVisible = false

  var incomingType: String = _

  val VectorTypeName: String = typeTag[Vector].tpe.toString
  val LabeledPointTypeName: String = typeTag[LabeledPoint].tpe.toString
  val RowMatrixTypeName: String = typeTag[RowMatrix].tpe.toString

  private var enableHealth = true

  private val input1 = ComponentConnection(
    tag = typeTag[Any],
    label = "Vector",
    description = "Vector of attributes",
    group = ConnectionGroups.DATA)


  override val inputTypes: ConnectionList = ConnectionList(input1)
  override var outputTypes: ConnectionList = ConnectionList.empty()

  @throws(classOf[Exception])
  override def validateAndPropagateIncomingTypes(incomingTypes: ConnectionList): Unit = {
    if (incomingTypes.length != inputTypes.length) {
      throw new Exception(s"Error: component inputs number is ${inputTypes.length} while received incoming number is ${incomingTypes.length}")
    }
    incomingType = incomingTypes.head.tag.tpe.toString
  }

  override def configure(paramMap: Map[String, Any]): Unit = {
    super.configure(paramMap)
    enableHealth = paramMap.getOrElse(ReflexSystemConfig.EnableHealthKey, true).asInstanceOf[Boolean]
  }

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    val parsedData: RDD[Vector] = incomingType match {
      case VectorTypeName =>
        dsArr(0).data[RDD[Vector]]()
      case LabeledPointTypeName =>
        dsArr(0).data[RDD[LabeledPoint]]().map(x => x.features)
      case RowMatrixTypeName =>
        dsArr(0).data[RowMatrix]().rows
      case _ => throw new Exception(s"${getClass.getSimpleName}: incoming type is not supported: $incomingType")
    }
    // Convert Spark vector to Breeze vector
    val rddDenseVector = parsedData.map(x => new BreezeDenseVector[Double](x.toDense.values))

    val healthLib = new HealthLibSpark(enableHealth)
    healthLib.setContext(env)
    healthLib.setRddOfDenseVector(rddDenseVector)

    // continuous histogram calculation
    val continuousHistogramForSpark = new ContinuousHistogramForSpark(HealthType.ContinuousHistogramHealth.toString)
    healthLib.addComponent(continuousHistogramForSpark)

    // categorical histogram calculation
    val categoricalHistogramForSpark = new CategoricalHistogramForSpark(HealthType.CategoricalHistogramHealth.toString)
    healthLib.addComponent(categoricalHistogramForSpark)

    healthLib.generateHealth()

    ArrayBuffer[DataWrapperBase]()
  }
}