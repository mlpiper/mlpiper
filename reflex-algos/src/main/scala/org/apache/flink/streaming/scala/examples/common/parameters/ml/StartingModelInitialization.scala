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
package org.apache.flink.streaming.scala.examples.common.parameters.ml

import com.parallelmachines.reflex.pipeline.JsonHeaders
import org.apache.flink.streaming.scala.examples.common.parameters.common.StringParameter
import org.apache.flink.streaming.scala.examples.common.parameters.tools.ArgumentParameterChecker

object ModelInitialization extends Enumeration {

  /** Enumeration of generic centroid initialization methods. */
  val PROVIDED = Value("provided")
  val RANDOM = Value("rand")

  def validModelInitializations: String = {
    '[' + this.values.mkString("/") + ']'
  }

  private def get(name: String): Option[ModelInitialization.Value] = {
    try {
      Some(this.withName(name))
    } catch {
      case e: Throwable => None
    }
  }

  def contains(name: String): Boolean = {
    get(name).isDefined
  }

  def apply(name: String): this.Value = {
    require(this.contains(name), StartingModelInitialization.errorMessage)
    this.withName(name)
  }

}

object StartingModelInitialization extends StringParameter {
  protected val validModelInitializations: String
    = ModelInitialization.validModelInitializations

  override val key: String = "modelInit"
  override val label: String = "Model Initialization"
  override val defaultValue: Option[String] = Some(ModelInitialization.RANDOM.toString)
  override val required: Boolean = false
  override val description: String = "Starting model initialization method. (Default: Random)"

  override val errorMessage: String = "Invalid " + key + " method. Must be one of the following: " +
    ModelInitialization.validModelInitializations


  override def condition(value: Option[String],
                         parameters: ArgumentParameterChecker): Boolean = {
    value.isDefined && ModelInitialization.contains(value.get)
  }

  override def toJson(): String = {
      var res = super.toJson()
      res = res.dropRight(1) + ", "
      res += jsonPair(s"${JsonHeaders.UITypeHeader}", s""""select"""") + ", "
      res += s""""options" : [{"${JsonHeaders.LabelHeader}":"Random", "value":"rand"}, {"${JsonHeaders.LabelHeader}":"Provided", "value":"provided"}]"""
      res += "}"
      res
  }
}
