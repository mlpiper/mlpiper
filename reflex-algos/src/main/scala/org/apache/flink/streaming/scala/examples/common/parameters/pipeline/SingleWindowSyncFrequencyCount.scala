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
package org.apache.flink.streaming.scala.examples.common.parameters.pipeline

import org.apache.flink.streaming.scala.examples.clustering.utils.PMWindowModes.PMWindowMode
import org.apache.flink.streaming.scala.examples.common.parameters.common.IntParameter
import org.apache.flink.streaming.scala.examples.common.parameters.tools.{ArgumentParameterChecker, WithArgumentParameters}

case object SingleWindowSyncFrequencyCount extends IntParameter {
  override val key = "syncFrequencyCount"
  override val label = "Samples per Window"
  override val required = false
  override val defaultValue = Some(100000)
  override val description = "For a count window, the number of samples to collect before processing the records in a given window. (Default: 100000)"
  override val errorMessage = key + " must be greater than zero and cannot specify " +
    SingleWindowSyncFrequencyMillis.key + " alongside " + key

  override def condition(syncFrequencyCount: Option[Int],
                         parameters: ArgumentParameterChecker)
  : Boolean = {
    syncFrequencyCount.isDefined &&
      syncFrequencyCount.get > 0 &&
      !parameters.containsNonEmpty(SingleWindowSyncFrequencyMillis)
  }
}

trait SingleCountWindow[Self] extends WithArgumentParameters {

  that: Self =>

  def setCountWindow(countSyncFrequency: Int): Self = {
    parameters.add(WindowMode, new PMWindowMode(countSyncFrequency))
    that
  }
}
