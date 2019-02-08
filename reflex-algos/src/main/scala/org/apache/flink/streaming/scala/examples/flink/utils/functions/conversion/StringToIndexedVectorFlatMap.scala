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
package org.apache.flink.streaming.scala.examples.flink.utils.functions.conversion

import org.apache.flink.streaming.scala.examples.flink.utils.functions.conversion.StringToIndexedVectorFlatMap._
import breeze.linalg.{DenseVector => BreezeDenseVector}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector
import org.mlpiper.datastructures.LabeledVector
import org.mlpiper.utils.ParameterIndices
import org.slf4j.LoggerFactory

object StringToIndexedVectorFlatMap {

  /**
    * Helper class for return values when parsing. Can return either an exception or a successfully
    * parsed value of type [[T]].
    * @param ret [[Either]] successfully parsed [[T]] or an [[Exception]]
    * @tparam T Type of successfully parsed item.
    */
  sealed case class ParseReturn[T](ret: Either[T, Exception]) extends Serializable {

    /** @return True if [[T]] (left) is not defined or if there is an exception. */
    def isException: Boolean = ret.isRight || !ret.isLeft

    /** @return Error message from exception. Otherwise returns string saying unknown error. */
    def getErrorMessage: String = {
      if (ret.isRight) {
        ret.right.get.getMessage
      } else {
        "Unknown error occurred!"
      }
    }

    /** @return Left side of [[Either]]. Assumes Left is defined. */
    def get: T = ret.left.get
  }


  /** Used to reuse parameter checking. */
  sealed abstract class StringToIndexedType[T](nrInputElements: Int,
                                               elementSeparatorRegex: String,
                                               indices: ParameterIndices)
    extends FlatMapFunction[String, T] {

    require(indices != null, "Provided ParameterIndices are null")
    require(nrInputElements > 0, "Number of input elements must be greater than zero")
    require(indices.min >= 0, "Provided index to parse is less than zero")
    require(indices.max < nrInputElements,
      "Provided index to parse exceeds number of input elements")
  }

  private[conversion]
  def parseStringToIndexedBreezeVector(separatedInput: Array[String],
                                       nrInputElements: Int,
                                       indices: ParameterIndices)
  : ParseReturn[BreezeDenseVector[Double]] = {

    // Assures input text has fixed number of specified elements
    if (separatedInput.length != nrInputElements) {
      return ParseReturn(Right(new StringIndexOutOfBoundsException(
        "Invalid number of input elements: " + separatedInput.length + " != " + nrInputElements)))
    }

    val toReturn = new BreezeDenseVector[Double](indices.size)
    val indexIterator = indices.iterator
    var i: Int = 0

    try {
      // Iterates through indices and stores respective value from separatedInput into return vector
      while (indexIterator.hasNext) {
        toReturn.update(i, separatedInput(indexIterator.next()).toDouble)
        i += 1
      }
    } catch {
      case e: Exception => return ParseReturn(Right(new Exception(
        "Invalid element: " + e.getMessage, e.getCause)))
    }

    // indexIterator should always be of length indices.size
    if (i != indices.size) {
      ParseReturn(Right(new IndexOutOfBoundsException("Did not parse expected number of elements: "
        + i + " != " + indices.size)))
    } else {
      ParseReturn(Left(toReturn))
    }
  }

  private[conversion]
  def parseStringToIndexedBreezeVector(input: String,
                                       nrInputElements: Int,
                                       elementSeparatorRegex: String,
                                       indices: ParameterIndices)
  : ParseReturn[BreezeDenseVector[Double]] = {
    val separatedElements = input.split(elementSeparatorRegex).map(s => s.replaceAll("\\s", ""))
    parseStringToIndexedBreezeVector(separatedElements, nrInputElements, indices)
  }

  private[conversion]
  def getTimestamp(separatedInput: Array[String],
                   timestampIndex: Int)
  : ParseReturn[Long] = {
    try {
      ParseReturn(Left(separatedInput(timestampIndex).toLong))
    } catch {
      case e: Exception => ParseReturn(Right(e))
    }
  }

  private[conversion]
  def getLabel(separatedInput: Array[String],
               labelIndex: Int)
  : ParseReturn[Double] = {
    try {
      ParseReturn(Left(separatedInput(labelIndex).toDouble))
    } catch {
      case e: Exception => ParseReturn(Right(e))
    }
  }
}

/**
  * Converts the input text stream into a [[BreezeDenseVector]] of [[Double]] using
  * the provided [[ParameterIndices]] indices, where each index represents the the respective
  * element from the separated input text to convert into a Double and store in the output vector.
  *
  * @param nrInputElements Number of elements separated by [[elementSeparatorRegex]]
  * @param elementSeparatorRegex Char used to separate elements in the input text stream
  * @param indices Indices to parse into an output [[BreezeDenseVector]]
  */
class StringToIndexedBreezeVectorFlatMap(nrInputElements: Int,
                                         elementSeparatorRegex: String,
                                         indices: ParameterIndices)
  extends StringToIndexedType[BreezeDenseVector[Double]](
    nrInputElements, elementSeparatorRegex, indices) {

  private val logger = LoggerFactory.getLogger(getClass)

  override def flatMap(value: String,
                       out: Collector[BreezeDenseVector[Double]])
  : Unit = {
    val ret = parseStringToIndexedBreezeVector(value, nrInputElements, elementSeparatorRegex, indices)
    if (ret.isException) {
      logger.error(raw"""Failed to parse '$value': ${ret.getErrorMessage}""")
    } else {
      out.collect(ret.get)
    }
  }
}

/**
  * Converts the input text stream into a [[LabeledVector]] of [[Double]] using
  * the provided [[ParameterIndices]] indices, where each index represents the the respective
  * element from the separated input text to convert into a Double and store in the output vector.
  *
  * If a labelIndex or timestampIndex is provided, stores that indexed element from input text
  * to the [[LabeledVector]] labelIndex or timestampIndex variable.
  *
  * @param nrInputElements Number of elements separated by [[elementSeparatorRegex]]
  * @param elementSeparatorRegex Char used to separate elements in the input text stream
  * @param indices Indices to parse into an output [[BreezeDenseVector]]
  * @param labelIndex Optional index to store a label from the input text
  * @param timestampIndex Optional index to store a timestamp from the input text
  */
class StringToIndexedLabeledVectorFlatMap(nrInputElements: Int,
                                          elementSeparatorRegex: String,
                                          indices: ParameterIndices,
                                          labelIndex: Option[Int] = None,
                                          timestampIndex: Option[Int] = None)
  extends StringToIndexedType[LabeledVector[Double]](
    nrInputElements, elementSeparatorRegex, indices) {

  private val logger = LoggerFactory.getLogger(getClass)

  if (labelIndex.isDefined) {
    require(labelIndex.get >= 0 && labelIndex.get < nrInputElements, "Label index out of bounds")
  }

  if (timestampIndex.isDefined) {
    require(timestampIndex.get >= 0 && timestampIndex.get < nrInputElements,
      "Timestamp index out of bounds")
  }

  override def flatMap(value: String,
                       out: Collector[LabeledVector[Double]])
  : Unit = {
    val separatedElements = value.split(elementSeparatorRegex).map(s => s.replaceAll("\\s", ""))
    var vector: BreezeDenseVector[Double] = null
    var label: Option[Double] = None
    var timestamp: Option[Long] = None

    // Parses vector portion
    val ret = parseStringToIndexedBreezeVector(separatedElements, nrInputElements, indices)
    if (ret.isException) {
      logger.error(raw"""Failed to parse '$value': ${ret.getErrorMessage}""")
      return
    } else {
      vector = ret.get
    }

    // Parses label if label index is provided
    if (labelIndex.isDefined) {
      val labelRet = getLabel(separatedElements, labelIndex.get)
      if (labelRet.isException) {
        logger.error(raw"""Failed to parse label from '$value': ${labelRet.getErrorMessage}""")
        return
      } else {
        label = Some(labelRet.get)
      }
    }

    // Parses timestamp if timestamp index is provided
    if (timestampIndex.isDefined) {
      val timestampRet = getTimestamp(separatedElements, timestampIndex.get)
      if (timestampRet.isException) {
        logger.error(raw"""Failed to parse timestamp from '$value': ${timestampRet
          .getErrorMessage}""")
        return
      } else {
        timestamp = Some(timestampRet.get)
      }
    }

    out.collect(new LabeledVector[Double](label, vector, timestamp))
  }

}
