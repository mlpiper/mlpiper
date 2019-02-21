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
package org.apache.flink.streaming.scala.examples.common.parsing

import org.apache.flink.streaming.scala.examples.common.parsing.ParameterIndices._

import scala.collection.mutable.ListBuffer

object ParameterIndices {

  private val RANGE_REGEX = "-"
  private val SEPARATOR_REGEX = ","

  private val SINGLE_INDEX = raw"""[0-9]+"""
  private val RANGE_INDEX = raw"""$SINGLE_INDEX$RANGE_REGEX$SINGLE_INDEX"""
  private val SINGLE_OR_RANGE_INDEX = raw"""(?:$SINGLE_INDEX|$RANGE_INDEX)"""

  private[ParameterIndices] val PARAMETER_INDICES_REGEX
    = raw"""^$SINGLE_OR_RANGE_INDEX(?:$SEPARATOR_REGEX$SINGLE_OR_RANGE_INDEX)*$$"""

  private def toRange(indexRange: String): Range = {
    var range: Range = null
    if (indexRange.matches(SINGLE_INDEX)) {
      val index = indexRange.toInt
      range = index to index
    } else if (indexRange.matches(RANGE_INDEX)) {
      val split = indexRange.split(RANGE_REGEX, 2)
      val t0 = split(0).toInt
      val t1 = split(1).toInt

      if (t1 > t0) {
        range = t0 to t1 by 1
      } else {
        range = t0 to t1 by -1
      }
    } else {
      throw new IllegalArgumentException("Not a valid index range: " + indexRange)
    }

    range
  }

  def validParameterIndices(indices: String): Boolean = {
    indices != null && indices.matches(PARAMETER_INDICES_REGEX)
  }
}

/**
  * Parses indices passed as a string into an [[Iterable]] of [[Int]].
  * Accepts zero or positive integers, ranges of integers (both ascending and descending), and
  * duplicates. Creates the iterable with respect to the order of user-passed indices.
  *
  * @param indices Indices string in the form of [[PARAMETER_INDICES_REGEX]]
  */
class ParameterIndices(indices: String) extends Iterable[Int] with Serializable {
  require(validParameterIndices(indices), "Invalid indices: " + indices)

  def this(size: Int) {
    this(raw"""0-${size - 1}""")
  }

  private val rangeList = try {
    indices.split(SEPARATOR_REGEX)
      .map(indexStr => toRange(indexStr))
      .foldLeft(new ListBuffer[Int]) { (list, range) => list ++= range.toList }
  } catch {
    case e: Throwable => throw new IllegalArgumentException(e.getMessage)
  }

  override def iterator: Iterator[Int] = rangeList.toIterator
  override def size: Int = rangeList.size
  def max: Int = rangeList.max
  def min: Int = rangeList.min
}
