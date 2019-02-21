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

import org.apache.flink.streaming.scala.examples.common.parameters.common.DoubleParameter
import org.apache.flink.streaming.scala.examples.common.parameters.tools.{ArgumentParameterChecker, WithArgumentParameters}

case object Gamma extends DoubleParameter {
  override val key = "gamma"
  override val label: String = "Gamma"
  override val required = false
  override val defaultValue: Option[Double] = Some(1.0)
  override val description = "Decay rate [0 - 1.0]. Factor by which to combine the old model " +
    "with new model. (Default: 1.0)"
  override val errorMessage: String = key + " must be between [0, 1]"

  override def condition(value: Option[Double],
                         parameters: ArgumentParameterChecker): Boolean = {
    value.isDefined && value.get >= 0.0 && value.get <= 1.0
  }
}

trait Gamma[Self] extends WithArgumentParameters {

  that: Self =>

  def setGamma(gamma: Double): Self = {
    this.parameters.add(Gamma, gamma)
    that
  }
}
