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

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.scala.examples.clustering.math.LabeledVector
import org.apache.flink.streaming.scala.examples.clustering.utils.ParsingUtils
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/** This class converts the input text stream into a labeled dense vector of Doubles */
class StringToLabeledVectorFlatMap(vectorLength: Int,
                                   separator: Char,
                                   labelSeparator: Option[Char] = None,
                                   timestampSeparator: Option[Char] = None,
                                   debug: Boolean = false)
  extends RichFlatMapFunction[String, LabeledVector[Double]] {

  private val logger = LoggerFactory.getLogger(getClass)

  require(vectorLength > 0, "labeled Vector length must be greater than 0")

  override def flatMap(value: String, out: Collector[LabeledVector[Double]]): Unit = {

    val parsedLabeledVector = ParsingUtils.stringToLabeledVector(
      labeledVectorInput = value,
      elementSeparator = separator,
      labelSeparator = labelSeparator,
      timestampSeparator = timestampSeparator)

    if (parsedLabeledVector.isEmpty) {
      logger.error(raw"Failed to parse ${value}")
      return
    }

    val labeledVector = parsedLabeledVector.get
    val labeledVectorLength = labeledVector.vector.length
    if (labeledVectorLength != vectorLength) {
      logger.error(s"Invalid vector length. Expected ${vectorLength} but received ${labeledVectorLength}")
    } else {
      out.collect(labeledVector)
    }
  }
}
