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

import org.apache.flink.streaming.scala.examples.common.parameters.common.LongParameter
import org.apache.flink.streaming.scala.examples.common.parameters.tools.{ArgumentParameterChecker, WithArgumentParameters}


case object MaxWaitTimeMillis extends LongParameter {
  override val key = "maxWaitTimeMillis"
  override val label = "Max Wait Time (msec)"
  override val defaultValue: Option[Long] = Some(0L)
  override val required = false
  override val description: String = "Max wait time in milliseconds for an input before the stream is assumed to be " +
    "closed. Note: Zero indicates waiting indefinitely for a new sample to arrive. (Default: 0)"

  override val errorMessage: String = key + " must be greater or equal to zero"

  override def condition(interval: Option[Long],
                         parameters: ArgumentParameterChecker)
  : Boolean = {
    interval.isDefined && interval.get >= 0L
  }
}

trait MaxWaitTimeMillis[Self] extends WithArgumentParameters {

  that: Self =>

  def setMaxWaitTimeMillis(maxWaitTmeMillis: Long): Self = {
    this.parameters.add(MaxWaitTimeMillis, maxWaitTmeMillis)
    that
  }
}
