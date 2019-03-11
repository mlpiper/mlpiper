package com.parallelmachines.reflex.components.spark.batch

import org.apache.spark.SparkContext
import org.mlpiper.infrastructure.{ComputeEngineType, DataWrapperBase, EnvWrapperBase, ReflexPipelineComponent}

import scala.collection.mutable.ArrayBuffer

trait SparkBatchComponent extends ReflexPipelineComponent {
  override val engineType = ComputeEngineType.SparkBatch

  override def materialize(envWrapper: EnvWrapperBase, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    return materialize(envWrapper.env[SparkContext], dsArr, errPrefixStr)
  }

  def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String): ArrayBuffer[DataWrapperBase]
}

