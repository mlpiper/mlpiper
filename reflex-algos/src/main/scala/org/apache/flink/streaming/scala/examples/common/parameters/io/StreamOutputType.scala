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
package org.apache.flink.streaming.scala.examples.common.parameters.io

import org.apache.flink.streaming.scala.examples.common.parameters.common.StringParameter
import org.apache.flink.streaming.scala.examples.common.parameters.tools.ArgumentParameterChecker

object OutputTypes extends Enumeration {
  val NONE = Value("none")
  val KAFKA_PRODUCER = Value("kafkaProducer")

  def contains(name: String): Boolean = {
    OutputTypes(name).isDefined
  }

  def apply(name: String): Option[OutputTypes.Value] = {
    try {
      Some(OutputTypes.withName(name))
    } catch {
      case e: Throwable => None
    }
  }
}

trait OutputType extends StringParameter {
  override val required: Boolean = false
  override val defaultValue: Option[String] = Some(OutputTypes.NONE.toString)
  override val errorMessage: String = "Invalid output type. " +
    key + " must be [none/kafkaProducer]."

  override def condition(value: Option[String],
                         parameters: ArgumentParameterChecker): Boolean = {
    value.isDefined && OutputTypes.contains(value.get)
  }
}

case object StreamOutputType extends OutputType {
  override val key: String = "streamOutputType"
  override val label: String = "Stream Output Type"
  override val description: String = "Method to output the DataStream"
}
