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

import org.apache.flink.streaming.scala.examples.clustering.utils.PMMatrixIO.FileType
import org.apache.flink.streaming.scala.examples.common.parameters.common.{MaybeExistingFilePathParameter, StringParameter}
import org.apache.flink.streaming.scala.examples.common.parameters.tools.ArgumentParameterChecker

case object StartingModelFilePath extends MaybeExistingFilePathParameter {
  override val key: String = "modelPath"
  override val label: String = "Model Path"
  override val required: Boolean = false
  override val description: String = "File path to the starting model"

  override def toJson(): String = {
    var res = super.toJson()
    res = res.dropRight(1) + ", "
    res += s""""require" : {"key":"modelInit", "value":"provided"}"""
    res += "}"
    res
  }
}

case object StartingModelFileFormat extends StringParameter {
  override val key: String = "modelFileFormat"
  override val label: String = "Model File Format"
  override val defaultValue: Option[String] = Some(FileType.CSV.toString)
  override val required: Boolean = false
  override val description: String = "File format of the starting model"
  override val errorMessage: String = "Invalid " + key

  override def condition(value: Option[String],
                         parameters: ArgumentParameterChecker): Boolean = {
    value.isDefined && FileType.hasValue(value.get)
  }
}