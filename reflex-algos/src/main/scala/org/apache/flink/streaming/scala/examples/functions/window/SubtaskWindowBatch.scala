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
package org.apache.flink.streaming.scala.examples.functions.window

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow, Window}
import org.apache.flink.streaming.scala.examples.functions.map.{PrependSubtaskID, SubtaskElement}
import org.apache.flink.util.Collector

object SubtaskWindowBatch {

  /**
    * Mini-Batches the input DataStream using the following steps:
    * <br> 1. Prepend the subtask ID of every element
    *         (where the subtask ID is where the data resides)
    * <br> 2. Key by the subtask ID
    * <br> 3. Perform a window function on each local subtask (time, count, sliding, etc)
    * <br> 4. Perform an apply on the entire window, propagates the iterable of data with a
    *         prepended subtask ID
    * <br> 5. PartitionCustom the data by sending each mini-batch to its original subtask ID,
    *         using the prepended subtask ID
    * <br> 6. Perform a map to remove the prepended subtask ID
    *
    * @param inputStream InputStream to mini-batch
    * @param windowFunction Window function which defines the batch
    *
    * @param keyEvidence TypeInformation of the subtask key (which is of Int)
    * @param subtaskElementEvidence TypeInformation of SubtaskElement of [[T]]
    * @param subtaskIterableEvidence TypeInformation of Iterable of SubtaskElement of [[T]]
    * @param iterableEvidence TypeInformation of the Iterable of [[T]]
    * @tparam T Input DataStream type
    * @tparam W Window type
    *
    * @return Mini-Batched DataStream of T
    */
  def batch[T, W <: Window](inputStream: DataStream[T],
                            windowFunction: KeyedStream[SubtaskElement[T], Int] => WindowedStream[SubtaskElement[T], Int, W]
                           )(
                             implicit keyEvidence: TypeInformation[Int],
                             subtaskElementEvidence: TypeInformation[SubtaskElement[T]],
                             subtaskIterableEvidence: TypeInformation[SubtaskElement[Iterable[T]]],
                             iterableEvidence: TypeInformation[Iterable[T]]
                           )
  : DataStream[Iterable[T]] = {

    // Partitions the data by sending it to its original subtaskID
    val bySubtaskID = new Partitioner[Int] {
      override def partition(subtaskIDKey: Int, numPartitions: Int): Int = subtaskIDKey
    }

    val keyedStream = inputStream
      .map(new PrependSubtaskID[T])(subtaskElementEvidence)
      .keyBy(_.subtaskID)(keyEvidence)

    val windowedStream: WindowedStream[SubtaskElement[T], Int, W] = windowFunction(keyedStream)

    val batchedStream = windowedStream
      .apply(new SubtaskWindowBatch[T, W])(subtaskIterableEvidence)
      .partitionCustom(bySubtaskID, _.subtaskID)
      .map(_.element)(iterableEvidence)

    batchedStream
  }

  /**
    * Mini-batches the input DataStream using tumbling count windows.
    */
  def countBatch[T](inputStream: DataStream[T],
                    size: Long
                   )(
                     implicit keyEvidence: TypeInformation[Int],
                     subtaskElementEvidence: TypeInformation[SubtaskElement[T]],
                     subtaskIterableEvidence: TypeInformation[SubtaskElement[Iterable[T]]],
                     iterableEvidence: TypeInformation[Iterable[T]]
                   )
  : DataStream[Iterable[T]] = {
    val windowFunction = (x: KeyedStream[SubtaskElement[T], Int]) => x.countWindow(size)
    batch[T, GlobalWindow](inputStream, windowFunction)(
      keyEvidence, subtaskElementEvidence, subtaskIterableEvidence, iterableEvidence)
  }

  /**
    * Mini-batches the input DataStream using sliding count windows.
    */
  def countBatch[T](inputStream: DataStream[T],
                    size: Long,
                    slide: Long
                   )(
                     implicit keyEvidence: TypeInformation[Int],
                     subtaskElementEvidence: TypeInformation[SubtaskElement[T]],
                     subtaskIterableEvidence: TypeInformation[SubtaskElement[Iterable[T]]],
                     iterableEvidence: TypeInformation[Iterable[T]]
                   )
  : DataStream[Iterable[T]] = {
    val windowFunction = (x: KeyedStream[SubtaskElement[T], Int]) => x.countWindow(size, slide)
    batch[T, GlobalWindow](inputStream, windowFunction)(
      keyEvidence, subtaskElementEvidence, subtaskIterableEvidence, iterableEvidence)
  }

  /**
    * Mini-batches the input DataStream using tumbling time windows.
    */
  def timeBatch[T](inputStream: DataStream[T],
                   size: Time
                  )(
                    implicit keyEvidence: TypeInformation[Int],
                    subtaskElementEvidence: TypeInformation[SubtaskElement[T]],
                    subtaskIterableEvidence: TypeInformation[SubtaskElement[Iterable[T]]],
                    iterableEvidence: TypeInformation[Iterable[T]]
                  )
  : DataStream[Iterable[T]] = {
    val windowFunction = (x: KeyedStream[SubtaskElement[T], Int]) => x.timeWindow(size)
    batch[T, TimeWindow](inputStream, windowFunction)(
      keyEvidence, subtaskElementEvidence, subtaskIterableEvidence, iterableEvidence)
  }

  /**
    * Mini-batches the input DataStream using sliding time windows.
    */
  def timeBatch[T](inputStream: DataStream[T],
                   size: Time,
                   slide: Time
                  )(
                    implicit keyEvidence: TypeInformation[Int],
                    subtaskElementEvidence: TypeInformation[SubtaskElement[T]],
                    subtaskIterableEvidence: TypeInformation[SubtaskElement[Iterable[T]]],
                    iterableEvidence: TypeInformation[Iterable[T]]
                  )
  : DataStream[Iterable[T]] = {
    val windowFunction = (x: KeyedStream[SubtaskElement[T], Int]) => x.timeWindow(size, slide)
    batch[T, TimeWindow](inputStream, windowFunction)(
      keyEvidence, subtaskElementEvidence, subtaskIterableEvidence, iterableEvidence)
  }
}

/**
  * Window function applied to a keyed DataStream, where the key is the subtask ID.
  * Propagates the [[Iterable]] of [[IN]] downstream along with its subtask ID key downstream.
  *
  * @tparam IN Input type
  * @tparam W Window type
  */
class SubtaskWindowBatch[IN, W <: Window]
  extends RichWindowFunction[SubtaskElement[IN], SubtaskElement[Iterable[IN]], Int, W] {

  override def apply(key: Int,
                     window: W,
                     input: Iterable[SubtaskElement[IN]],
                     out: Collector[SubtaskElement[Iterable[IN]]])
  : Unit = {
    out.collect(SubtaskElement(key, input.map(_.element)))
  }
}