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
package org.apache.flink.streaming.scala.examples.flink.utils.functions.performance

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.mlpiper.parameters.common.ArgumentParameterMap
import org.mlpiper.parameters.performance.{PerformanceMetrics, PrintInterval}
import org.apache.flink.streaming.scala.examples.flink.utils.functions.performance.{PerformanceMetricsOutput, RichCoFlatMapPerformanceMetrics, RichMapPerformanceMetrics}
import org.apache.flink.util.Collector

abstract class TimedRichCoFlatMapFunction[IN1, IN2, OUT](parameters: ArgumentParameterMap)
  extends RichCoFlatMapFunction[IN1, IN2, PerformanceMetricsOutput[OUT]]
    with RichCoFlatMapPerformanceMetrics[IN1, IN2, OUT] {

  require(parameters.get(PerformanceMetrics).isDefined, PerformanceMetrics.errorMessage)
  require(parameters.get(PrintInterval).isDefined, PrintInterval.errorMessage)

  this.printInterval = parameters.get(PrintInterval).get
  override def open(config: Configuration): Unit = {
    super.open(config)
    this.subtaskID = getRuntimeContext.getIndexOfThisSubtask
    this.parallelism = getRuntimeContext.getNumberOfParallelSubtasks
    this.open()
  }


  override final def flatMap1(in: IN1, out: Collector[PerformanceMetricsOutput[OUT]])
  : Unit = {
    this.flatMap1Function(in, out, this.getRuntimeContext)
  }

  override def flatMap2(in: IN2, out: Collector[PerformanceMetricsOutput[OUT]])
  : Unit = {
    this.flatMap2Function(in, out)
  }
}
