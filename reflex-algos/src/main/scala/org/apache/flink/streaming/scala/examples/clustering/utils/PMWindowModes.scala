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

package org.apache.flink.streaming.scala.examples.clustering.utils

import org.apache.flink.streaming.api.windowing.time.Time

object PMWindowModes {

  private val INVALID_SYNC_FREQUENCY = -1
  private val INVALID_TIME_FREQUENCY = null

  def globalToLocalSyncFrequencyCount(globalSyncFrequencyCount: Int,
                                      parallelism: Int): Int = {
    Math.max(1, globalSyncFrequencyCount / parallelism)
  }

  def CountWindow(globalCountSyncFrequency: Int): PMWindowMode = {
    new PMWindowMode(globalCountSyncFrequency)
  }

  def TimeWindow(timeSyncFrequency: Time): PMWindowMode = {
    new PMWindowMode(timeSyncFrequency)
  }

  class PMWindowMode extends Serializable {
    private var _globalCountSyncFrequency: Int = INVALID_SYNC_FREQUENCY
    private var _timeSyncFrequency: Time = INVALID_TIME_FREQUENCY

    def this(globalCountSyncFrequency: Int) = {
      this
      this._globalCountSyncFrequency = globalCountSyncFrequency
    }

    def this(timeSyncFrequency: Time) = {
      this
      this._timeSyncFrequency = timeSyncFrequency
    }

    def globalCountSyncFrequency: Int = {
      require(this._globalCountSyncFrequency != INVALID_SYNC_FREQUENCY,
        "Not initialized as a count window")
      this._globalCountSyncFrequency
    }

    def localCountSyncFrequency(paralellism: Int): Int = {
      globalToLocalSyncFrequencyCount(globalCountSyncFrequency, paralellism)
    }

    def timeSyncFrequency: Time = {
      require(this._timeSyncFrequency != INVALID_TIME_FREQUENCY,
        "Not initialized as a time window")
      this._timeSyncFrequency
    }

    def isCountWindow: Boolean = {
      this._globalCountSyncFrequency != INVALID_SYNC_FREQUENCY &&
        this._timeSyncFrequency == INVALID_TIME_FREQUENCY
    }

    def isTimeWindow: Boolean = {
      this._timeSyncFrequency != INVALID_TIME_FREQUENCY &&
        this._globalCountSyncFrequency == INVALID_SYNC_FREQUENCY
    }

    def isValid: Boolean = {
      this.isCountWindow ^ this.isTimeWindow
    }
  }

}
