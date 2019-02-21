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
package org.apache.flink.streaming.scala.examples.common.parameters.ml

import org.apache.flink.streaming.scala.examples.common.parameters.common.BooleanParameter
import org.apache.flink.streaming.scala.examples.common.parameters.tools.WithArgumentParameters

case object Validation extends BooleanParameter {
  override val key = "enableValidation"
  override val label: String = "Enable Validation"
  override val required = false
  override val description = "Toggles validation mode. (Default: false)"
}

trait Validation[Self] extends WithArgumentParameters {

  that: Self =>

  def enableValidation(enableValidation: Boolean): Self = {
    this.parameters.add(Validation, enableValidation)
    that
  }
}

case object ValidationAccumulators extends BooleanParameter {
  override val key = "enableAccumulators"
  override val label: String = "Enable Accumulators for validation"
  override val required = false
  override val description = "Toggles use of accumulators for validation. (Default: false)"
}

trait ValidationAccumulators[Self] extends WithArgumentParameters {

  that: Self =>

  def enableAccumulators(enableAccumulators: Boolean): Self = {
    this.parameters.add(ValidationAccumulators, enableAccumulators)
    that
  }
}
