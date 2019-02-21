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
package org.apache.flink.streaming.scala.examples.functions.ml

import org.apache.flink.streaming.scala.examples.common.ml.PredictValidationCountersAndFunctions
import org.apache.flink.streaming.scala.examples.common.parameters.tools.ArgumentParameterMap

abstract class TimedRichMapPredictValidationOperator[Input, Prediction, Model](
                                                         model: Model,
                                                         override val params: ArgumentParameterMap)
  extends TimedRichMapPredictOperator[Input, Prediction, Model](model, params)
  with PredictValidationCountersAndFunctions[Input, Prediction] {

  override def open(): Unit = {
    super.open()
    initGlobalCounter(enabledValidation, getRuntimeContext)
  }

  override def close(): Unit = {
    closeGlobalCounter(enabledValidation, getRuntimeContext)
    super.close()
  }

  override protected def internalNonPerformanceMapFunction(input: Input): Prediction = {
    val result = super.internalNonPerformanceMapFunction(input)
    if (enabledValidation) {
      this.validateInput(input, result)
    } else {
      result
    }
  }
}
