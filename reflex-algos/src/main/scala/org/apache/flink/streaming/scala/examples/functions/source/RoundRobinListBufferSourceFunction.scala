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
package org.apache.flink.streaming.scala.examples.functions.source

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.scala.examples.clustering.math.LabeledVector

import scala.collection.mutable.ListBuffer

/**
  * Source function to convert a ListBuffer of LabeledVectors to a DataStream of LabeledVectors.
  * Adds data to the stream in a round-robin fashion to every parallel subtask and sleeps
  * per element.
  *
  * @param data ListBuffer of LabeledVectors to convert to a round-robin DataStream
  * @param intervalWaitTimeMillis Sleep time in ms per element
  * @param intervalWaitTimeNanos Sleep time in ns per element
  */
class RoundRobinListBufferSourceFunction(data: ListBuffer[LabeledVector[Double]],
                                         intervalWaitTimeMillis: Long,
                                         intervalWaitTimeNanos: Int)
  extends RoundRobinIterableSourceFunction[LabeledVector[Double]](
    data, intervalWaitTimeMillis, intervalWaitTimeNanos)

/**
  * Source function to convert an Iterable of [[T]] to a DataStream of [[T]].
  * Adds data to the stream in a round-robin fashion to every parallel subtask and sleeps
  * per element.
  *
  * @param data Iterable of [[T]] to convert to a round-robin DataStream
  * @param intervalWaitTimeMillis Sleep time in ms per element
  * @param intervalWaitTimeNanos Sleep time in ns per element
  */
class RoundRobinIterableSourceFunction[T](data: Iterable[T],
                                          intervalWaitTimeMillis: Long,
                                          intervalWaitTimeNanos: Int)
  extends RichParallelSourceFunction[T]() {

  override def run(ctx: SourceFunction.SourceContext[T]) {
    val parallelism = getRuntimeContext.getNumberOfParallelSubtasks
    val subTaskID = getRuntimeContext.getIndexOfThisSubtask
    var waitDuration: Long = 0

    for ((element, i) <- data.view.zipWithIndex) {
      waitDuration = intervalWaitTimeMillis
      // Sends a single row of data to a single subtask
      if (i % parallelism == subTaskID) {
        val preCollectTime = System.currentTimeMillis()
        ctx.collect(element)
        waitDuration -= (System.currentTimeMillis() - preCollectTime)
      }
      Thread.sleep(Math.max(0, waitDuration), intervalWaitTimeNanos)
    }

    Thread.sleep(intervalWaitTimeMillis * 5)
    ctx.close()
  }

  override def cancel(): Unit = { /* Do nothing. */ }
}
