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
package org.apache.flink.streaming.scala.examples.common.parameters.common

import org.apache.flink.ml.common.Parameter
import org.apache.flink.streaming.scala.examples.common.parameters.tools.ArgumentParameterChecker
import com.parallelmachines.reflex.pipeline.JsonHeaders

object ArgumentParameterType extends Enumeration {
  val IntType, LongType, DoubleType, BooleanType, StringType, ClassType = Value

  def isValidType(expectedType: ArgumentParameterType.Value, value: Any): Boolean = {
    var ret = false
    if ((expectedType == DoubleType) &&
      (value.isInstanceOf[Int] || value.isInstanceOf[BigInt] || value.isInstanceOf[Double])) {
      ret = true
    } else if ((expectedType == IntType || expectedType == LongType) &&
      (value.isInstanceOf[Int] || value.isInstanceOf[BigInt])) {
      ret = true
    } else if (value.isInstanceOf[String] && expectedType == StringType) {
      ret = true
    } else if (value.isInstanceOf[Boolean] && expectedType == BooleanType) {
      ret = true
    }
    ret
  }
}

//TODO: Add unit tests

/**
  * Trait used to parse arguments from the main method.
  *
  * @tparam T Type of parameter value associated to this parameter key
  */
sealed trait ArgumentParameter[T] extends Parameter[T] {

  /** Argument key used by the user to set this parameter value. */
  val argType: ArgumentParameterType.Value

  val key: String
  val label: String

  val required: Boolean
  val description: String
  val errorMessage: String

  def condition(value: Option[T], parameters: ArgumentParameterChecker): Boolean

  final def jsonPair(key: String, value: String): String = {
    s""""$key":$value"""
  }

  def toJson(): String = {
    var res = "{"
    res += jsonPair(s"${JsonHeaders.KeyHeader}", s""""$key"""") + ", "
    res += jsonPair(s"${JsonHeaders.DescriptionHeader}", s""""$description"""") + ", "
    res += jsonPair(s"${JsonHeaders.LabelHeader}", s""""$label"""")

    var defaultValueString = defaultValue.getOrElse("").toString
    if (defaultValueString != "") {
      res += ", "
      res += jsonPair(s"${JsonHeaders.DefaultValueHeader}", s""""$defaultValueString"""")
    }
    if (required == false) {
      res += ", "
      res += jsonPair(s"${JsonHeaders.OptionalHeader}", true.toString)
    }

    res += "}"
    res
  }

  final def jsonStringPairAppend(jsonString: String, pairString: String): String = {
    jsonString.dropRight(1) + ", " + pairString + "}"
  }
}

trait DefinedParameter[T] extends ArgumentParameter[T] {

  override val defaultValue: Option[T] = None
  override lazy val errorMessage: String = key + " must be defined"

  override def condition(value: Option[T],
                         parameters: ArgumentParameterChecker): Boolean = {
    value.isDefined
  }
}

trait IntParameter extends ArgumentParameter[Int] {
  override val argType: ArgumentParameterType.Value = ArgumentParameterType.IntType

  override def toJson(): String = {
    jsonStringPairAppend(super.toJson(), jsonPair("type", s""""int""""))
  }
}

trait StringParameter extends ArgumentParameter[String] {
  override val argType: ArgumentParameterType.Value = ArgumentParameterType.StringType

  override def toJson(): String = {
    jsonStringPairAppend(super.toJson(), jsonPair("type", s""""string""""))
  }
}

trait DoubleParameter extends ArgumentParameter[Double] {
  override val argType: ArgumentParameterType.Value = ArgumentParameterType.DoubleType

  override def toJson(): String = {
    jsonStringPairAppend(super.toJson(), jsonPair("type", s""""double""""))
  }
}

trait LongParameter extends ArgumentParameter[Long] {
  override val argType: ArgumentParameterType.Value = ArgumentParameterType.LongType

  override def toJson(): String = {
    jsonStringPairAppend(super.toJson(), jsonPair("type", s""""long""""))
  }
}

trait BooleanParameter extends ArgumentParameter[Boolean] {
  override val argType: ArgumentParameterType.Value = ArgumentParameterType.BooleanType
  protected val validBooleanParameters: String = "[true/false] "

  override lazy val defaultValue = Some(false)
  override lazy val errorMessage: String = key + " must be true or false"

  override def toJson(): String = {
    jsonStringPairAppend(super.toJson(), jsonPair("type", s""""boolean""""))
  }

  override def condition(performance: Option[Boolean],
                         parameters: ArgumentParameterChecker)
  : Boolean = {
    performance.isDefined
  }
}
