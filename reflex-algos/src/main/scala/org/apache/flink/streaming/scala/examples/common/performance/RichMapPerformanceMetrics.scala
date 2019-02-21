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
package org.apache.flink.streaming.scala.examples.common.performance

import org.apache.flink.api.common.functions.RuntimeContext

trait RichMapPerformanceMetrics[IN, OUT] extends RichFunctionPerformanceMetrics {

  protected def internalNonPerformanceMapFunction(input: IN): OUT

  protected def internalPerformanceMapFunction(input: IN): OUT = {
    internalNonPerformanceMapFunction(input)
  }

  protected final def performanceMapFunction(input: IN, ctx: RuntimeContext)
  : PerformanceMetricsOutput[OUT] = {
    this.onInput()
    val outputTime = this.time {
      internalPerformanceMapFunction(input)
    }

    val performanceMetrics: Option[PerformanceMetricsHash] = this.printMetricsIfTriggered()
    if (performanceMetrics.isDefined) {
      this.publishLocalStats(ctx)
    }

    PerformanceMetricsOutput(performanceMetrics, Some(outputTime.output))
  }

  protected final def nonPerformanceMapFunction(input: IN, ctx: RuntimeContext): PerformanceMetricsOutput[OUT] = {
    this.onInput()
    PerformanceMetricsOutput(None, Some(internalNonPerformanceMapFunction(input)))
  }

}
