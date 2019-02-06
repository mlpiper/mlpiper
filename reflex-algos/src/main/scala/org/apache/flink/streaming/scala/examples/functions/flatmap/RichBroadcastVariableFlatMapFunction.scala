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
package org.apache.flink.streaming.scala.examples.functions.flatmap

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration

/**
  * [[RichFlatMapFunction]] with a local variable obtained from a single broadcasted variable.
  *
  * @param broadcastElementName Name of the broadcast variable.
  * @tparam IN Input type
  * @tparam OUT Output type
  * @tparam VAR Type of the broadcast variable
  */
abstract class RichBroadcastVariableFlatMapFunction[IN, OUT, VAR](broadcastElementName: String)
  extends RichFlatMapFunction[IN, OUT] {

  protected var broadcastVariable: VAR = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val broadcastVariableList = getRuntimeContext.getBroadcastVariable[VAR](broadcastElementName)
    require(broadcastVariableList.size() >= 1, "Must contain at least one broadcasted variable")

    broadcastVariable = broadcastVariableList.get(0)
  }
}