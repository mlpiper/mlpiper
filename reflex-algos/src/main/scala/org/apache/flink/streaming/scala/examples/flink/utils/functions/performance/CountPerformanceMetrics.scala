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

trait CountPerformanceMetrics extends PerformanceMetricsStats {
  protected var count: Long = 0

  /**
    * Should be called on every single input regardless if performance metrics are enabled.
    */
  override protected def onInput(): Unit = {
    count += 1
  }

  protected final def globalAverage[T](sum: T)(implicit x: scala.math.Numeric[T]): Double = {
    x.toDouble(sum) / count.toDouble
  }
}
