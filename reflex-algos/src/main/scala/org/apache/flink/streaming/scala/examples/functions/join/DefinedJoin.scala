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
package org.apache.flink.streaming.scala.examples.functions.join

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.scala.examples.functions.common.{DefinedThreeTuple, DefinedTwoTuple}

object DefinedJoin {

  def twoJoin[L, R](inputL: DataStream[L],
                    inputR: DataStream[R])
  : DataStream[DefinedTwoTuple[L, R]] = {
    val twoTupleL = inputL.map(l => new DefinedTwoTuple[L, R](Some(l), None))
    val twoTupleR = inputR.map(r => new DefinedTwoTuple[L, R](None, Some(r)))

    twoTupleL.union(twoTupleR)
  }

  def threeJoin[L, M, R](inputL: DataStream[L],
                         inputM: DataStream[M],
                         inputR: DataStream[R])
  : DataStream[DefinedThreeTuple[L, M, R]] = {
    val threeTupleL = inputL.map(l => new DefinedThreeTuple[L, M, R](Some(l), None, None))
    val threeTupleM = inputM.map(m => new DefinedThreeTuple[L, M, R](None, Some(m), None))
    val threeTupleR = inputR.map(r => new DefinedThreeTuple[L, M, R](None, None, Some(r)))

    threeTupleL.union(threeTupleM).union(threeTupleR)
  }
}
