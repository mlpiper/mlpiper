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
package org.apache.flink.streaming.scala.examples.functions.conversion

import breeze.linalg.{DenseVector => BreezeDenseVector}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.scala.examples.clustering.utils.ParsingUtils
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/** This class converts the input text stream into a dense vector of Doubles */
class StringToBreezeVectorFlatMap(vectorLength: Int,
                                  separator: Char,
                                  debug: Boolean)
  extends RichFlatMapFunction[String, BreezeDenseVector[Double]] {

  private val logger = LoggerFactory.getLogger(getClass)

  require(vectorLength > 0, "Vector length must be greater than 0")

  override def flatMap(value: String, out: Collector[BreezeDenseVector[Double]]): Unit = {
    val parsedVector = ParsingUtils.stringToBreezeDenseVector(value, separator)

    if (parsedVector.isEmpty) {
      logger.error(raw"Failed to parse ${value}")
      return
    }
    val vector = parsedVector.get
    if (vector.length != vectorLength) {
      logger.error(s"Invalid number of elements, expected ${vectorLength}, got ${vector.length}")
    } else {
      out.collect(vector)
    }
  }
}
