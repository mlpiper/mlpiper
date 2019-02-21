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

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.scala.examples.common.ml.PredictOperator
import org.apache.flink.streaming.scala.examples.common.parameters.tools.ArgumentParameterMap
import org.apache.flink.streaming.scala.examples.functions.performance.TimedRichMapFunction


abstract class TimedRichMapPredictOperator[Input, Prediction, Model](override protected var model: Model,
                                                                     params: ArgumentParameterMap)
  extends TimedRichMapFunction[Input, Prediction](params)
    with PredictOperator[Input, Prediction, Model] {

  override protected def internalNonPerformanceMapFunction(input: Input): Prediction = {
    val predictionAndSuccess = this.predict(input)
    val prediction = predictionAndSuccess._1
    prediction
  }

  override def open(config: Configuration): Unit = {
    super.open(config)
    this.prePredict()
  }
}
