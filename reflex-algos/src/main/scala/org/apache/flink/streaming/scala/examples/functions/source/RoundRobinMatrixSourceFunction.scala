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

import breeze.linalg.{DenseMatrix => BreezeDenseMatrix, DenseVector => BreezeDenseVector}
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

/**
  * Source function to convert a BreezeDenseMatrix to a DataStream of BreezeDenseVectors.
  * Adds data to the stream in a round-robin fashion to every parallel subtask and sleeps
  * per round.
  *
  * @param data Matrix of data to convert to a DataStream
  * @param intervalWaitTimeMillis Sleep time in ms per round
  * @param intervalWaitTimeNanos Sleep time in ns per round.
  */
class RoundRobinMatrixSourceFunction(data: BreezeDenseMatrix[Double],
                                     intervalWaitTimeMillis: Long,
                                     intervalWaitTimeNanos: Int)
  extends RichParallelSourceFunction[BreezeDenseVector[Double]]() {

  override def run(ctx: SourceFunction.SourceContext[BreezeDenseVector[Double]]) {
    val parallelism = getRuntimeContext.getNumberOfParallelSubtasks
    val subTaskID = getRuntimeContext.getIndexOfThisSubtask
    for (i <- 0 until data.rows) {
      // Sleeps after parallelism samples have been collected
      if ((i + 1) % parallelism == 0) {
        Thread.sleep(intervalWaitTimeMillis, intervalWaitTimeNanos)
      }
      // Sends a single row of data to a single subtask
      if (i % parallelism == subTaskID) {
        ctx.collect(data(i, ::).t)
      }
    }
    Thread.sleep(intervalWaitTimeMillis * 5)
    ctx.close()
  }

  override def cancel(): Unit = { /* Do nothing. */ }
}
