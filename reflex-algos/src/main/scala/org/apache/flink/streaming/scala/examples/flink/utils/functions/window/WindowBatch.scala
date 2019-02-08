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
package org.apache.flink.streaming.scala.examples.flink.utils.functions.window

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.scala.function.{RichAllWindowFunction, RichWindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow, Window}
import org.apache.flink.streaming.scala.examples.flink.utils.functions.map.{PrependSubtaskID, SubtaskElement}
import org.apache.flink.util.Collector



/**
  * Window function that propagates the [[Iterable]] of [[IN]] downstream.
  * @tparam IN Input type
  * @tparam KEY Key type
  * @tparam W Window type
  */
class WindowBatch[IN, KEY, W <: Window]
  extends RichWindowFunction[IN, Iterable[IN], KEY, W] {

  override def apply(key: KEY,
                     window: W,
                     input: Iterable[IN],
                     out: Collector[Iterable[IN]])
  : Unit = {
    out.collect(input)
  }
}

/**
  * All Window function that propagates the [[Iterable]] of [[IN]] downstream.
  * @tparam IN Input type
  * @tparam W Window type
  */
class AllWindowBatch[IN, W <: Window]
  extends RichAllWindowFunction[IN, Iterable[IN], W] {

  override def apply(window: W,
                     input: Iterable[IN],
                     out: Collector[Iterable[IN]])
  : Unit = {
    out.collect(input)
  }
}


