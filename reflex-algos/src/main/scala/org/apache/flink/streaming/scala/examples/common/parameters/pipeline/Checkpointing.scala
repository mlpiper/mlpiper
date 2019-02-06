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

import org.apache.flink.streaming.scala.examples.common.parameters.common.{BooleanParameter, PositiveLongParameter}
import org.apache.flink.streaming.scala.examples.common.parameters.tools.{ArgumentParameterChecker, WithArgumentParameters}

case object Checkpointing extends BooleanParameter {
  override val key = "enableCheckpointing"
  override val label: String = "Enable Checkpointing"
  override val required = false
  override val description = validBooleanParameters + "Toggles checkpointing"

  override def condition(enableCheckpointing: Option[Boolean],
                         parameters: ArgumentParameterChecker)
  : Boolean = {
    super.condition(enableCheckpointing, parameters) && parameters.contains(CheckpointingInterval)
  }
}

case object CheckpointingInterval extends PositiveLongParameter {
  override val key = "checkpointInterval"
  override val label = "Checkpoint Interval"
  override val required = false
  override val description = "Interval in seconds (or milliseconds) before a checkpoint is taken"

  override def condition(interval: Option[Long],
                         parameters: ArgumentParameterChecker)
  : Boolean = {
    super.condition(interval, parameters) && parameters.contains(Checkpointing)
  }
}

trait Checkpointing[Self] extends WithArgumentParameters {

  that: Self =>

  def enableCheckpointing(enableCheckpointing: Boolean): Self = {
    this.parameters.add(Checkpointing, enableCheckpointing)
    that
  }

  def enableCheckpointing(checkpointInterval: Long): Self = {
    this.parameters.add(Checkpointing, true)
    this.parameters.add(CheckpointingInterval, checkpointInterval)
    that
  }
}
