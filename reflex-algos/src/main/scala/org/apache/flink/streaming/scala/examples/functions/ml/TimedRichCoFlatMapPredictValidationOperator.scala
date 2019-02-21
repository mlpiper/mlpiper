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
import org.apache.flink.util.Collector

abstract class TimedRichCoFlatMapPredictValidationOperator[Input, Model, Prediction](
                                                                                      inputModel: Model,
                                                                                      override val params: ArgumentParameterMap)
  extends TimedRichCoFlatMapPredictOperator[Input, Model, Prediction](inputModel, params)
    with PredictValidationCountersAndFunctions[Input, Prediction] {

  override def open(): Unit = {
    super.open()
    initGlobalCounter(enabledValidation, getRuntimeContext)
  }

  override def close(): Unit = {
    closeGlobalCounter(enabledValidation, getRuntimeContext)
    super.close()
  }

  override def internalFlatMap1Function(input: Input, out: Collector[Prediction])
  : Boolean = {
    val predictionAndSuccess = this.predict(input)
    val result = predictionAndSuccess._1
    if (enabledValidation) {
      this.validateInput(input, result)
    }
    out.collect(result)

    val success = predictionAndSuccess._2
    success
  }

}
