package org.apache.flink.streaming.scala.examples.clustering.math

import breeze.linalg.DenseVector
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.scala.examples.clustering.utils.ParsingUtils
import org.apache.flink.util.Collector

class DenseVectorToNamedVectorFlatMap extends RichFlatMapFunction[DenseVector[Double], ReflexNamedVector] {
  override def flatMap(value: DenseVector[Double], out: Collector[ReflexNamedVector]): Unit = {
    val vector = ParsingUtils.denseVectorToReflexNamedVector(value)

    if (vector.isDefined && (vector.get.vector.length == value.length)) {
      out.collect(vector.get)
    }
  }
}

class LabeledVectorToNamedVectorFlatMap extends RichFlatMapFunction[LabeledVector[Double], ReflexNamedVector] {
  override def flatMap(value: LabeledVector[Double], out: Collector[ReflexNamedVector]): Unit = {
    val vector = ParsingUtils.labeledVectorToReflexNamedVector(value)

    if (vector.isDefined) {
      out.collect(vector.get)
    }
  }
}
