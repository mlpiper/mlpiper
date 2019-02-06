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
package org.apache.flink.streaming.scala.examples.common.algorithm

/**
  * A [[ModelInstantiate]] gives the implementing class a [[Model]] variable and
  * a setter function to instantiate the starting model.
  *
  * @tparam Self Type of the implementing class
  * @tparam Model Type of the implementing class's model
  */
trait ModelInstantiate[Self, Model] {

  that: Self =>

  protected var startingModel: Model = _

  /**
    * Sets the starting model.
    * @param model Starting model.
    * @return
    */
  final def setStartingModel(model: Model): Self = {
    this.startingModel = model
    that
  }

}
