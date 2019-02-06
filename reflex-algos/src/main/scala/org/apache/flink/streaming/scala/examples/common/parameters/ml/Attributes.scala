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

import org.apache.flink.streaming.scala.examples.common.parameters.common.PositiveIntParameter
import org.apache.flink.streaming.scala.examples.common.parameters.tools.WithArgumentParameters

case object Attributes extends PositiveIntParameter {
  override val key = "attributes"
  override val label: String = "Attributes"
  override val required = true
  override val description = "Number of attributes in each sample"
}

trait Attributes[Self] extends WithArgumentParameters {

  that: Self =>

  def setAttributes(attributes: Int): Self = {
    this.parameters.add(Attributes, attributes)
    that
  }
}

