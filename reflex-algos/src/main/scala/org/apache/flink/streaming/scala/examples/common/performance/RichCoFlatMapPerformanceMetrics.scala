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
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

trait RichCoFlatMapPerformanceMetrics[IN1, IN2, OUT] extends RichFunctionPerformanceMetrics {

  class OutCollector extends Collector[OUT] {
    private val collector = new ArrayBuffer[OUT]

    override def collect(record: OUT): Unit = {
      collector += record
    }

    override def close(): Unit = {
      collector.clear()
    }

    private[RichCoFlatMapPerformanceMetrics] def collectRecords(
                                                                 out: Collector[PerformanceMetricsOutput[OUT]])
    : Unit = {
      collector.foreach(x => out.collect(PerformanceMetricsOutput[OUT](None, Some(x))))
      collector.clear()
    }
  }

  protected def internalFlatMap1Function(input: IN1, out: Collector[OUT]): Boolean

  protected final def flatMap1Function(input: IN1,
                                       out: Collector[PerformanceMetricsOutput[OUT]],
                                       ctx: RuntimeContext)
  : Unit = {
    val outCollector = new OutCollector

    val predictionSuccess = internalFlatMap1Function(input, outCollector)

    if (predictionSuccess) {
      this.onInput()
    }

    val performanceMetrics: Option[PerformanceMetricsHash] = this.printMetricsIfTriggered()
    if (performanceMetrics.isDefined) {
      this.publishLocalStats(ctx)
    }

    outCollector.collectRecords(out)
    out.collect(PerformanceMetricsOutput(performanceMetrics, None))
  }

  protected def internalFlatMap2Function(input: IN2, out: Collector[OUT]): Unit

  protected final def flatMap2Function(input: IN2,
                                       out: Collector[PerformanceMetricsOutput[OUT]])
  : Unit = {
    val outCollector = new OutCollector
    internalFlatMap2Function(input, outCollector)
    outCollector.collectRecords(out)
  }
}
