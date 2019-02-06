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
package org.apache.flink.streaming.scala.examples.functions.split

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.scala.examples.functions.common.{DefinedThreeTuple, DefinedTwoTuple}

import scala.collection.mutable.ListBuffer

object DefinedSplit {

  private val DEFINED_LEFT_KEY: String = "l"
  private val DEFINED_MIDDLE_KEY: String = "m"
  private val DEFINED_RIGHT_KEY: String = "r"

  /**
    * Splits a DataStream of [[L]] and [[R]] within [[DefinedTwoTuple]] on values that are defined.
    *
    * @param input Input DataStream of types [[L]] and [[R]]
    * @param l     Evidence parameter for [[L]]
    * @param r     Evidence parameter for [[R]]
    * @tparam L Value to split if it's defined
    * @tparam R Value to split if it's defined
    * @return DataStream of [[L]] and DataStream of [[R]]
    */
  def twoSplit[L, R](input: DataStream[_ <: DefinedTwoTuple[L, R]])
                    (implicit l: TypeInformation[L],
                     r: TypeInformation[R])
  : (DataStream[L], DataStream[R]) = {

    val splitStream = input.split(entry => {

      val entryKeys = new ListBuffer[String]
      if (entry.left.isDefined) {
        entryKeys += DEFINED_LEFT_KEY
      }
      if (entry.right.isDefined) {
        entryKeys += DEFINED_RIGHT_KEY
      }

      entryKeys
    })


    val aStream = splitStream
      .select(DEFINED_LEFT_KEY)
      .map(_.left.get)

    val bStream = splitStream
      .select(DEFINED_RIGHT_KEY)
      .map(_.right.get)

    (aStream, bStream)
  }

  /**
    * Splits a DataStream of [[L]], [[M]], and [[R]] within [[DefinedThreeTuple]] on values that
    * are defined.
    *
    * @param input Input DataStream of types [[L]] and [[M]]
    * @param l     Evidence parameter for [[L]]
    * @param m     Evidence parameter for [[M]]
    * @param r     Evidence parameter for [[R]]
    * @tparam L Value to split if it's defined
    * @tparam M Value to split if it's defined
    * @tparam R [[DefinedTwoTuple]] of values [[L]] and [[M]]
    * @return DataStream of [[L]] and DataStream of [[M]]
    */
  def threeSplit[L, M, R](input: DataStream[_ <: DefinedThreeTuple[L, M, R]])
                         (implicit l: TypeInformation[L],
                          m: TypeInformation[M],
                          r: TypeInformation[R])
  : (DataStream[L], DataStream[M], DataStream[R]) = {

    val splitStream = input.split(entry => {
      val entryKeys = new ListBuffer[String]
      if (entry.left.isDefined) {
        entryKeys += DEFINED_LEFT_KEY
      }
      if (entry.middle.isDefined) {
        entryKeys += DEFINED_MIDDLE_KEY
      }
      if (entry.right.isDefined) {
        entryKeys += DEFINED_RIGHT_KEY
      }

      entryKeys
    })

    val lStream = splitStream
      .select(DEFINED_LEFT_KEY)
      .map(_.left.get)

    val mStream = splitStream
      .select(DEFINED_MIDDLE_KEY)
      .map(_.middle.get)

    val rStream = splitStream
      .select(DEFINED_RIGHT_KEY)
      .map(_.right.get)

    (lStream, mStream, rStream)
  }

  private def threeSplitOnX[L, M, R, LRET, RRET](input: DataStream[_ <: DefinedThreeTuple[L, M, R]],
                                                 isLeft: DefinedThreeTuple[L, M, R] => Boolean,
                                                 isRight: DefinedThreeTuple[L, M, R] => Boolean,
                                                 toLeft: DefinedThreeTuple[L, M, R] => LRET,
                                                 toRight: DefinedThreeTuple[L, M, R] => RRET)
                                                (implicit l: TypeInformation[L],
                                                 m: TypeInformation[M],
                                                 r: TypeInformation[R],
                                                 lRet: TypeInformation[LRET],
                                                 rRet: TypeInformation[RRET])
  : (DataStream[LRET], DataStream[RRET]) = {

    val splitStream = input.split(entry => {
      val entryKeys = new ListBuffer[String]
      if (isLeft(entry)) {
        entryKeys += DEFINED_LEFT_KEY
      }
      if (isRight(entry)) {
        entryKeys += DEFINED_RIGHT_KEY
      }

      entryKeys
    })

    val lStream = splitStream
      .select(DEFINED_LEFT_KEY)
      .map(toLeft(_))

    val rStream = splitStream
      .select(DEFINED_RIGHT_KEY)
      .map(toRight(_))

    (lStream, rStream)
  }

  def threeSplitOnLeft[L, M, R](input: DataStream[_ <: DefinedThreeTuple[L, M, R]])
                               (implicit l: TypeInformation[L],
                                m: TypeInformation[M],
                                r: TypeInformation[R])
  : (DataStream[L], DataStream[DefinedTwoTuple[M, R]]) = {
    threeSplitOnX(input,
      isLeft = (x: DefinedThreeTuple[L, M, R]) => x.left.isDefined,
      isRight = (x: DefinedThreeTuple[L, M, R]) => x.middle.isDefined || x.right.isDefined,
      toLeft = (x: DefinedThreeTuple[L, M, R]) => x.left.get,
      toRight = (x: DefinedThreeTuple[L, M, R]) => new DefinedTwoTuple[M, R](x.middle, x.right)
    )
  }

  def threeSplitOnMiddle[L, M, R](input: DataStream[_ <: DefinedThreeTuple[L, M, R]])
                                 (implicit l: TypeInformation[L],
                                  m: TypeInformation[M],
                                  r: TypeInformation[R])
  : (DataStream[M], DataStream[DefinedTwoTuple[L, R]]) = {
    threeSplitOnX(input,
      isLeft = (x: DefinedThreeTuple[L, M, R]) => x.middle.isDefined,
      isRight = (x: DefinedThreeTuple[L, M, R]) => x.left.isDefined || x.right.isDefined,
      toLeft = (x: DefinedThreeTuple[L, M, R]) => x.middle.get,
      toRight = (x: DefinedThreeTuple[L, M, R]) => new DefinedTwoTuple[L, R](x.left, x.right)
    )
  }

  def threeSplitOnRight[L, M, R](input: DataStream[_ <: DefinedThreeTuple[L, M, R]])
                                (implicit l: TypeInformation[L],
                                 m: TypeInformation[M],
                                 r: TypeInformation[R])
  : (DataStream[DefinedTwoTuple[L, M]], DataStream[R]) = {
    threeSplitOnX(input,
      isLeft = (x: DefinedThreeTuple[L, M, R]) => x.left.isDefined || x.middle.isDefined,
      isRight = (x: DefinedThreeTuple[L, M, R]) => x.right.isDefined,
      toLeft = (x: DefinedThreeTuple[L, M, R]) => new DefinedTwoTuple[L, M](x.left, x.middle),
      toRight = (x: DefinedThreeTuple[L, M, R]) => x.right.get
    )
  }
}


