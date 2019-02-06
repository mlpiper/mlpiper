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
package org.apache.flink.streaming.scala.examples.common.parameters.common

import org.apache.flink.streaming.scala.examples.common.parameters.tools.ArgumentParameterChecker


trait PositiveIntParameter extends IntParameter {
  override val defaultValue: Option[Int] = None
  override lazy val errorMessage: String = key + " must be greater than zero"

  override def condition(x: Option[Int], parameters: ArgumentParameterChecker)
  : Boolean = {
    x.isDefined && x.get > 0
  }
}

trait PositiveLongParameter extends LongParameter {
  override val defaultValue: Option[Long] = None
  override lazy val errorMessage: String = key + " must be greater than zero"

  override def condition(x: Option[Long], parameters: ArgumentParameterChecker)
  : Boolean = {
    x.isDefined && x.get > 0L
  }
}

trait PositiveDoubleParameter extends DoubleParameter {
  override val defaultValue: Option[Double] = None
  override lazy val errorMessage: String = key + " must be greater than zero"

  override def condition(x: Option[Double], parameters: ArgumentParameterChecker)
  : Boolean = {
    x.isDefined && x.get > 0.0
  }
}