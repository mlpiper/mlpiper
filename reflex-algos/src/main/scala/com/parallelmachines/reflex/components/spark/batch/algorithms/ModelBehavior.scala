package com.parallelmachines.reflex.components.spark.batch.algorithms

import org.mlpiper.infrastructure.{JsonHeaders, ReflexPipelineComponent}

object ModelBehaviorType extends Enumeration {
  val ModelConsumer, ModelProducer, ModelProducerConsumer, Auxiliary = Value

  private val modelProducerList = List[Value](ModelProducer, ModelProducerConsumer)
  private val modelConsumerList = List[Value](ModelConsumer, ModelProducerConsumer)

  def isModelProducer(modelBehaviorType: Value): Boolean = {
    modelProducerList.contains(modelBehaviorType)
  }

  def isModelConsumer(modelBehaviorType: Value): Boolean = {
    modelConsumerList.contains(modelBehaviorType)
  }
}

/* This trait should be used by algorithms to specify model behavior used in the algorithm. */
trait ModelBehavior extends ReflexPipelineComponent {
  val modelBehaviorType: ModelBehaviorType.Value

  abstract override def buildInfo(): Unit = {
    super.buildInfo()
    addInfoField(JsonHeaders.ModelBehaviorHeader, modelBehaviorType.toString)
  }
}
