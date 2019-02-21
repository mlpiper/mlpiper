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
package org.apache.flink.streaming.scala.examples.clustering.stat.utils

import org.apache.flink.streaming.api.scala.function.RichAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

/**
  * Wrapper class to hold tuple of ID and an element.
  *
  * @param elementID ID of element.
  * @param element   Element.
  * @tparam T Element type.
  */
case class IDElement[T](elementID: Long, element: T)

/**
  * Apply function which prepends an element with its ID (count of element in this case).
  *
  * @tparam IN Input type.
  */
class PrependElementID[IN]
  extends RichAllWindowFunction[IN, IDElement[IN], GlobalWindow] {

  var elementID: Long = 1

  override def apply(window: GlobalWindow, input: Iterable[IN], out: Collector[IDElement[IN]]): Unit = {

    input.foreach(x =>
      out.collect(IDElement(
        elementID = elementID,
        element = x)))

    elementID += 1

    if (elementID == Long.MaxValue) {
      elementID = 1
    }
  }
}
