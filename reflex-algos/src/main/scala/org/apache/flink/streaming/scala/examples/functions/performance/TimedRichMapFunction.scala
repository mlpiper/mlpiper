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
package org.apache.flink.streaming.scala.examples.functions.performance

import org.apache.flink.api.common.functions.{RichMapFunction, RuntimeContext}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.scala.examples.common.parameters.performance.{PerformanceMetrics, PrintInterval}
import org.apache.flink.streaming.scala.examples.common.parameters.tools.ArgumentParameterMap
import org.apache.flink.streaming.scala.examples.common.performance.{PerformanceMetricsHash, PerformanceMetricsOutput, RichMapPerformanceMetrics}
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ArrayBuffer

abstract class TimedRichMapFunction[IN, OUT](parameters: ArgumentParameterMap)
  extends RichMapFunction[IN, PerformanceMetricsOutput[OUT]]
    with RichMapPerformanceMetrics[IN, OUT] {

  require(parameters.get(PerformanceMetrics).isDefined, PerformanceMetrics.errorMessage)
  require(parameters.get(PrintInterval).isDefined, PrintInterval.errorMessage)

  this.printInterval = parameters.get(PrintInterval).get
  private val enabledPerformance: Boolean = parameters.get(PerformanceMetrics).get

  override def open(config: Configuration): Unit = {
    super.open(config)
    this.subtaskID = getRuntimeContext.getIndexOfThisSubtask
    this.parallelism = getRuntimeContext.getNumberOfParallelSubtasks
    this.open()
  }

  private val _map: (IN, RuntimeContext) => PerformanceMetricsOutput[OUT] = {
    if (enabledPerformance) {
      this.performanceMapFunction
    } else {
      this.nonPerformanceMapFunction
    }
  }

  override final def map(in: IN): PerformanceMetricsOutput[OUT] = _map(in, this.getRuntimeContext)
}
