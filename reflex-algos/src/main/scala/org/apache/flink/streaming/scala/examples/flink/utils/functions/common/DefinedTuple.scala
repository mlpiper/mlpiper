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
package org.apache.flink.streaming.scala.examples.flink.utils.functions.common

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._

/**
  * Class with two [[Option]] values of [[L]] and [[R]]. Used for outputting two types
  * from operators with a single output.
  *
  * @param left Optional value of [[L]]
  * @param right Optional value of [[R]]
  * @tparam L Type of value [[L]]
  * @tparam R Type of value [[R]]
  */
class DefinedTwoTuple[L, R](val left: Option[L],
                            val right: Option[R])
  extends Serializable

/**
  * Class with three [[Option]] values of [[L]], [[M]], and [[R]]. Used for outputting three types
  * from operators with a single output.
  *
  * @param left Optional value of [[L]]
  * @param middle Optional value of [[M]]
  * @param right Optional value of [[R]]
  * @tparam L Type of value [[L]]
  * @tparam M Type of value [[M]]
  * @tparam R Type of value [[R]]
  */
class DefinedThreeTuple[L, M, R](val left: Option[L],
                                 val middle: Option[M],
                                 val right: Option[R])
  extends Serializable

object DefinedTwoTuple {

  /**
    * Converts a DataStream of DefinedTwoTuple with a nested DefinedTwoTuple into a DataStream of
    * DefinedThreeTuple
    */
  def nestedRightToDefinedThreeTuple[L, R1, R2, R <: DefinedTwoTuple[R1, R2]](
                                              input: DataStream[_ <: DefinedTwoTuple[L, R]])(
    implicit l: TypeInformation[L],
    r1: TypeInformation[R1],
    r2: TypeInformation[R2],
    r: TypeInformation[R])
  : DataStream[DefinedThreeTuple[L, R1, R2]] = {
    input.map(x =>
      if (x.right.isDefined) {
        val right = x.right.get
        new DefinedThreeTuple[L, R1, R2](x.left, right.left, right.right)
      } else {
        new DefinedThreeTuple[L, R1, R2](x.left, None, None)
      })
  }

  /**
    * Converts a DataStream of DefinedTwoTuple with a nested DefinedTwoTuple into a DataStream of
    * DefinedThreeTuple
    */
  def nestedLeftToDefinedThreeTuple[L1, L2, L <: DefinedTwoTuple[L1, L2], R](
                                              input: DataStream[_ <: DefinedTwoTuple[L, R]])(
    implicit l1: TypeInformation[L1],
    l2: TypeInformation[L2],
    l: TypeInformation[L],
    r: TypeInformation[R])
  : DataStream[DefinedThreeTuple[L1, L2, R]] = {
    input.map(x =>
      if (x.left.isDefined) {
        val left = x.left.get
        new DefinedThreeTuple[L1, L2, R](left.left, left.right, x.right)
      } else {
        new DefinedThreeTuple[L1, L2, R](None, None, x.right)
      })
  }
}
