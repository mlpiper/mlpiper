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

import org.apache.flink.streaming.scala.examples.common.parameters.common.{ArgumentParameterType, DefinedParameter, IntParameter}
import org.apache.flink.streaming.scala.examples.common.parameters.tools.{ArgumentParameterChecker, WithArgumentParameters}
import org.apache.flink.streaming.scala.examples.clustering.utils.PMWindowModes.PMWindowMode

case object MaxWindowsToProcess extends IntParameter {
  override val key = "maxWindowsToProcess"
  override val label = "Max Parallel Windows"
  override val defaultValue: Option[Int] = Some(0)
  override val required = false
  val description = "Max number of windows to process. " +
    "Zero indicates process all available windows"

  override val errorMessage: String = key + " must be greater than or equal to zero"

  override def condition(value: Option[Int], parameters: ArgumentParameterChecker) = {
    value.isDefined && value.get >= 0
  }
}

case object WindowMode extends DefinedParameter[PMWindowMode] {
  override val argType = ArgumentParameterType.ClassType
  override lazy val key: String = this.getClass.getSimpleName
  override val label = "Window Mode"
  override val required: Boolean = false
  override val description: String = ""
}

trait SynchronizedSingleWindow[Self] extends WithArgumentParameters
  with SingleCountWindow[Self]
  with SingleTimeWindow[Self] {

  that: Self =>

  def setWindowsToProcessPerSync(windowsToProcessPerSync: Int): Self = {
    parameters.add(MaxWindowsToProcess, windowsToProcessPerSync)
    that
  }
}
