/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.scala.examples.common.parameters.testing

import org.apache.flink.streaming.scala.examples.common.parameters.common._
import org.apache.flink.streaming.scala.examples.common.parameters.tools.{ArgumentParameterChecker, WithArgumentParameters}

case object Samples extends PositiveIntParameter {
  override val key = "samples"
  override val label = "Samples"
  override val required = true
  override val description = "Number of samples to generate"
}

trait Samples[Self] extends WithArgumentParameters {

  that: Self =>

  def setSamples(samples: Int): Self = {
    this.parameters.add(Samples, samples)
    that
  }
}

case object Epsilon extends PositiveDoubleParameter {
  override val key = "epsilon"
  override val label = "Epsilon"
  override val defaultValue: Option[Double] = Some(0.001)
  override val required = false
  override val description = "Testing epsilon"
}

case object Noise extends DoubleParameter {
  override val key = "noise"
  override val label = "Noise"
  override val defaultValue: Option[Double] = Some(0.0)
  override val required = false
  override val description = "Amount of noise applied to generated data"
  override val errorMessage: String = key + " must be greater or equal to zero"

  override def condition(value: Option[Double],
                         parameters: ArgumentParameterChecker): Boolean = {
    value.isDefined && value.get >= 0
  }
}

trait Noise[Self] extends WithArgumentParameters {

  that: Self =>

  def setNoise(noise: Double): Self = {
    this.parameters.add(Noise, noise)
    that
  }
}

case object IntervalWaitTimeMillis extends LongParameter {
  override val key = "intervalWaitTimeMillis"
  override val label = "Interval Wait Time Millis"
  override val defaultValue: Option[Long] = Some(0L)
  override val required = false
  override val description = "Generation time interval milliseconds"
  override val errorMessage: String = key + " must be greater or equal to 0"

  override def condition(value: Option[Long],
                         parameters: ArgumentParameterChecker) = {
    value.isDefined && value.get >= 0L
  }
}

trait IntervalWaitTimeMillis[Self] extends WithArgumentParameters {

  that: Self =>

  def setIntervalWaitTimeMillis(intervalWaitTimeMillis: Long): Self = {
    this.parameters.add(IntervalWaitTimeMillis, intervalWaitTimeMillis)
    that
  }
}

case object IntervalWaitTimeNanos extends LongParameter {
  override val key = "intervalWaitTimeNanos"
  override val label = "Interval Wait Time Nanos"
  override val defaultValue: Option[Long] = Some(0L)
  override val required = false
  override val description = "Generation time interval nanoseconds"
  override val errorMessage: String = key + " must be greater or equal to 0"

  override def condition(value: Option[Long],
                         parameters: ArgumentParameterChecker) = {
    value.isDefined && value.get >= 0L
  }
}

trait IntervalWaitTimeNanos[Self] extends WithArgumentParameters {

  that: Self =>

  def setIntervalWaitTimeNanos(intervalWaitTimeNanos: Long): Self = {
    this.parameters.add(IntervalWaitTimeNanos, intervalWaitTimeNanos)
    that
  }
}

case object NumClusters extends PositiveIntParameter {
  override val key = "numClusters"
  override val label = "Number of Clusters"
  override val required = true
  override val description = "Number of clusters"
}

trait NumClusters[Self] extends WithArgumentParameters {

  that: Self =>

  def setNumClusters(numClusters: Int): Self = {
    this.parameters.add(NumClusters, numClusters)
    that
  }
}

case object ClusterSeparation extends PositiveDoubleParameter {
  override val key = "clusterSeparation"
  override val label = "Cluster Separation"
  override val required = true
  override val description = "Distance between clusters"
}

trait ClusterSeparation[Self] extends WithArgumentParameters {

  that: Self =>

  def setClusterSeparation(clusterSeparation: Double): Self = {
    this.parameters.add(ClusterSeparation, clusterSeparation)
    that
  }
}