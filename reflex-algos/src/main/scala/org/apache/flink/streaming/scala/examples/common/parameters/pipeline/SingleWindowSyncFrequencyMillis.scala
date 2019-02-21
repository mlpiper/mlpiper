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

import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.scala.examples.clustering.utils.PMWindowModes.PMWindowMode
import org.apache.flink.streaming.scala.examples.common.parameters.common.LongParameter
import org.apache.flink.streaming.scala.examples.common.parameters.tools.{ArgumentParameterChecker, WithArgumentParameters}

case object SingleWindowSyncFrequencyMillis extends LongParameter {
  override val key = "syncFrequencyMillis"
  override val label = "Sync Frequency (msec)"
  override val required = false
  override val defaultValue = None
  override val description = "For a time window, the time in msec to wait/buffer before processing samples in the current window"
  override val errorMessage = key + " must be greater or equal to 50 and cannot specify " +
    SingleWindowSyncFrequencyCount.key + " alongside " + key

  override def condition(syncFrequencyTime: Option[Long],
                         parameters: ArgumentParameterChecker)
  : Boolean = {
    syncFrequencyTime.isDefined &&
      syncFrequencyTime.get >= 50L &&
      !parameters.containsNonEmpty(SingleWindowSyncFrequencyCount)
  }
}

trait SingleTimeWindow[Self] extends WithArgumentParameters {
  that: Self =>

  def setTimeWindow(timeSyncFrequency: Time): Self = {
    parameters.add(WindowMode, new PMWindowMode(timeSyncFrequency))
    that
  }
}
