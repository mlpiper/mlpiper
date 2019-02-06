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
package org.apache.flink.streaming.scala.examples.common.parameters.tools

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.scala.examples.clustering.utils.Constants
import org.apache.flink.streaming.scala.examples.common.parameters.common.{ArgumentParameter, ArgumentParameterType}
import org.apache.flink.streaming.scala.examples.common.parameters.sets.ParameterSet
import org.slf4j.LoggerFactory

import scala.collection.mutable

trait ArgumentParameterChecker {
  def contains(parameter: ArgumentParameter[_]): Boolean

  def containsNonEmpty(parameter: ArgumentParameter[_]): Boolean
}

/**
  * Used to parse arguments from a main method.
  *
  * @param name Name of the program to check argument parameters.
  */
class ArgumentParameterTool(val name: String) extends Serializable
  with ArgumentParameterChecker {

  private val logger = LoggerFactory.getLogger(classOf[ArgumentParameterTool])

  protected val _parameterMap = new mutable.HashMap[String, ArgumentParameter[_]]
  protected var _actualParameterMap: mutable.Map[String, Any] = _
  protected var _parsedParameters: ParameterTool = _

  def add[T](input: ArgumentParameter[T]): Unit = {
    if (_parameterMap.contains(input.key)) {
      throw new IllegalArgumentException("Parameter " + input.key + " already exists")
    }
    _parameterMap.put(input.key, input)
  }

  def add(input: ArgumentParameterTool): Unit = {
    input._parameterMap.foreach(x => this._parameterMap.put(x._1, x._2))
  }

  def add(input: ParameterSet): Unit = {
    add(input.parameterSet)
  }

  def toJson(): String = {
    var ret: String = ""
    for (x <- _parameterMap) {
      ret += x._2.toJson() + ","
    }
    ret.dropRight(1)
  }

  override def contains(parameter: ArgumentParameter[_]): Boolean = {
    _parameterMap.contains(parameter.key)
  }

  override def containsNonEmpty(parameter: ArgumentParameter[_]): Boolean = {

    /** This is transitional check while having old ParameterTool infrastracture and the new without */
    if (_parsedParameters != null) {
      this.contains(parameter) &&
        (parameter.defaultValue.isDefined || _parsedParameters.has(parameter.key))
    } else {
      this.contains(parameter) &&
        (parameter.defaultValue.isDefined || _actualParameterMap.contains(parameter.key))
    }
  }

  def containsUserSpecified(parameter: ArgumentParameter[_]): Boolean = {
    _parsedParameters.has(parameter.key)
  }

  @deprecated
  private def _containsRequiredParameters(parameterTool: ParameterTool): Boolean = {
    require(parameterTool != null)
    val missingParameters = _parameterMap.values
      .filter(x => x.required && !parameterTool.has(x.key))
      .map(_.key)
      .mkString(", ")

    if (missingParameters.nonEmpty) {
      throw new IllegalArgumentException(
        "Missing the following required parameters: " + missingParameters)
    } else {
      true
    }
  }

  private def _containsRequiredParameters(paramMap: Map[String, Any]): Boolean = {
    require(paramMap != null)
    val missingParameters = _parameterMap.values
      .filter(x => x.required && !paramMap.contains(x.key))
      .map(_.key)
      .mkString(", ")

    if (missingParameters.nonEmpty) {
      throw new IllegalArgumentException(
        "Missing the following required parameters: " + missingParameters)
    } else {
      true
    }
  }

  def containsHelp(args: Array[String]): Boolean = {
    args.length == 0 ||
      args.exists(arg => arg.equalsIgnoreCase("--help") || arg.equalsIgnoreCase("-help"))
  }


  private def getKeyPrintout(parameter: ArgumentParameter[_]): String = {
    var key = parameter.key
    if (parameter.defaultValue.isDefined) {
      key += (" [" + parameter.defaultValue.get.toString + "]")
    }
    key
  }


  def getHelpPrintout: String = {
    /** Spacing between the parameter and description. */
    val paramDescriptionSpacing = 5

    /** Aligns the description by the
      * longest parameter name + default value + [[paramDescriptionSpacing]]. */
    val maxKeyLength = _parameterMap
      .map(x => getKeyPrintout(x._2).length)
      .max + paramDescriptionSpacing

    val sortedSeq = _parameterMap.values.toSeq.sortBy(_.key)
    val stb = new StringBuilder
    stb.append(name)
    stb.append(Constants.NEW_LINE)
    sortedSeq.foreach { param =>
      val printout = getKeyPrintout(param)
      stb.append(String.format("--%-" + maxKeyLength + "s %s" + Constants.NEW_LINE,
        printout,
        param.description))
    }
    stb.toString()
  }

  @deprecated
  def validateParameters(parameterTool: ParameterTool): Boolean = {
    val validParameters = _containsRequiredParameters(parameterTool)
    if (validParameters) {
      _parsedParameters = parameterTool
    }
    validParameters
  }

  @deprecated
  def validateParameters(args: Array[String]): Boolean = {
    validateParameters(ParameterTool.fromArgs(args))
  }

  def initializeParameters(paramMap: Map[String, Any]): Boolean = {
    val validParameters = _containsRequiredParameters(paramMap)
    if (validParameters) {
      _actualParameterMap = mutable.Map(paramMap.toSeq: _*)

      for ((key, value) <- paramMap) {
        if (_parameterMap.contains(key)) {
          val param = _parameterMap.get(key).get
          val argType = param.argType
          var valueOpt: Option[_] = None

          if (!ArgumentParameterType.isValidType(argType, value)) {
            throw new IllegalArgumentException(s"""Wrong parameter type: given "${value.getClass}", expected: "${argType.toString}"""")
          }

          argType match {
            case ArgumentParameterType.IntType =>
              value match {
                case n: java.lang.Number => valueOpt = Some(n.intValue())
                case _ => throw new Exception(s"No casting rule for type: " + value.getClass)
              }
            case ArgumentParameterType.LongType =>
              value match {
                case n: java.lang.Number => valueOpt = Some(n.longValue())
                case _ => throw new Exception(s"No casting rule for type: " + value.getClass)
             }
            case ArgumentParameterType.DoubleType =>
              value match {
                case n: java.lang.Number => valueOpt = Some(n.doubleValue())
                case _ => throw new Exception(s"No casting rule for type: " + value.getClass)
              }
           case ArgumentParameterType.BooleanType =>
              valueOpt = Some(value.asInstanceOf[Boolean])
            case ArgumentParameterType.StringType =>
              valueOpt = Some(value.asInstanceOf[String])
            case _ => throw new Exception(s"No casting rule for type: ${argType.toString}")
          }
          _actualParameterMap(key) = _getNonEmpty(key, _getCheckCondition(param.asInstanceOf[ArgumentParameter[Any]], valueOpt))
        } else {
          logger.info(s"""Warning: Parameter "$key" is not supported for "${this.name}"""")
        }
      }
    }
    validParameters
  }

  private def _getCheckCondition[T](param: ArgumentParameter[T],
                                    value: Option[T]): Option[T] = {
    if (param.condition(value, this)) {
      value
    } else {
      throw new IllegalArgumentException(param.errorMessage)
    }
  }

  private def _getFromArgumentParameter[T](parameter: ArgumentParameter[T],
                                           paramGetter: (String) => T): Option[T] = {
    var value: Option[T] = None
    if (_parsedParameters.has(parameter.key)) {
      try {
        value = Some(paramGetter(parameter.key))
      } catch {
        case e: Throwable => throw new IllegalArgumentException(
          "Failed to parse for key '" + parameter.key + "': " + parameter.errorMessage, e)
      }
    } else {
      value = parameter.defaultValue
    }
    _getCheckCondition(parameter, value)
  }

  private def _getFromKey[T](key: String,
                             paramGetter: (String) => T): Option[T] = {
    if (!_parameterMap.contains(key)) {
      throw new NoSuchElementException("Invalid key for " + name + " parameters: '" + key + "'")
    }

    val parameter = _parameterMap(key).asInstanceOf[ArgumentParameter[T]]
    _getFromArgumentParameter(parameter, paramGetter)
  }

  private def _getNonEmpty[T](key: String,
                              value: Option[T]): T = {
    if (value.isDefined) {
      value.get
    } else {
      throw new NoSuchElementException("Key '" + key + "' contains an empty value")
    }
  }

  private def _getFromKeyNonEmpty[T](key: String,
                                     paramGetter: (String) => T): T = {
    _getNonEmpty(key, _getFromKey(key, paramGetter))
  }

  private def _getFromArgumentParameterNonEmpty[T](parameter: ArgumentParameter[T],
                                                   paramGetter: (String) => T): T = {
    _getNonEmpty(parameter.key, _getFromArgumentParameter(parameter, paramGetter))
  }

  //TODO review _getCheckCondition, _getNonEmpty etc functions
  def get[T](param: ArgumentParameter[T]): T = {
    //TODO check that param was registered and exists in _parameterMap
    if (this._actualParameterMap.contains(param.key)) {
      return this._actualParameterMap.get(param.key).get.asInstanceOf[T]
    } else {
      return _getNonEmpty(param.key, _getCheckCondition(param, param.defaultValue))
    }
  }

  def getOption[T](param: ArgumentParameter[T]): Option[T] = {
    var value: Option[T] = None
    //TODO check that param was registered and exists in _parameterMap
    if (this._actualParameterMap.contains(param.key)) {
      value = Some(this._actualParameterMap.get(param.key).get.asInstanceOf[T])
      _getCheckCondition(param, value)
    } else {
      value = param.defaultValue
    }
    value
  }

  def getString(key: String): String = {
    _getFromKeyNonEmpty(key, _parsedParameters.get)
  }

  def getString(parameter: ArgumentParameter[String]): String = {
    _getFromArgumentParameterNonEmpty(parameter, _parsedParameters.get)
  }

  def getOptionString(key: String): Option[String] = {
    _getFromKey(key, _parsedParameters.get)
  }

  def getOptionString(parameter: ArgumentParameter[String]): Option[String] = {
    _getFromArgumentParameter(parameter, _parsedParameters.get)
  }

  def getInt(key: String): Int = {
    _getFromKeyNonEmpty(key, _parsedParameters.getInt)
  }

  def getInt(parameter: ArgumentParameter[Int]): Int = {
    _getFromArgumentParameterNonEmpty(parameter, _parsedParameters.getInt)
  }

  def getOptionInt(key: String): Option[Int] = {
    _getFromKey(key, _parsedParameters.getInt)
  }

  def getOptionInt(parameter: ArgumentParameter[Int]): Option[Int] = {
    _getFromArgumentParameter(parameter, _parsedParameters.getInt)
  }

  def getLong(key: String): Long = {
    _getFromKeyNonEmpty(key, _parsedParameters.getLong)
  }

  def getLong(parameter: ArgumentParameter[Long]): Long = {
    _getFromArgumentParameterNonEmpty(parameter, _parsedParameters.getLong)
  }

  def getOptionLong(key: String): Option[Long] = {
    _getFromKey(key, _parsedParameters.getLong)
  }

  def getOptionLong(parameter: ArgumentParameter[Long]): Option[Long] = {
    _getFromArgumentParameter(parameter, _parsedParameters.getLong)
  }

  def getDouble(key: String): Double = {
    _getFromKeyNonEmpty(key, _parsedParameters.getDouble)
  }

  def getDouble(parameter: ArgumentParameter[Double]): Double = {
    _getFromArgumentParameterNonEmpty(parameter, _parsedParameters.getDouble)
  }

  def getOptionDouble(key: String): Option[Double] = {
    _getFromKey(key, _parsedParameters.getDouble)
  }

  def getOptionDouble(parameter: ArgumentParameter[Double]): Option[Double] = {
    _getFromArgumentParameter(parameter, _parsedParameters.getDouble)
  }

  def getBoolean(key: String): Boolean = {
    _getFromKeyNonEmpty(key, _parsedParameters.getBoolean)
  }

  def getBoolean(parameter: ArgumentParameter[Boolean]): Boolean = {
    _getFromArgumentParameterNonEmpty(parameter, _parsedParameters.getBoolean)
  }

  def getOptionBoolean(key: String): Option[Boolean] = {
    _getFromKey(key, _parsedParameters.getBoolean)
  }

  def getOptionBoolean(parameter: ArgumentParameter[Boolean]): Option[Boolean] = {
    _getFromArgumentParameter(parameter, _parsedParameters.getBoolean)
  }
}
