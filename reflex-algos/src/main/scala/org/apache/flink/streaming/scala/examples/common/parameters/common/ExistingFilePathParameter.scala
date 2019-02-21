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

import java.nio.file.{Files, Paths}

import org.apache.flink.streaming.scala.examples.common.parameters.tools.ArgumentParameterChecker

trait ExistingFilePathParameter extends StringParameter {
  override val defaultValue = None
  override lazy val errorMessage = key + " must be an existing file"

  override def condition(filePath: Option[String],
                         parameters: ArgumentParameterChecker)
  : Boolean = {
    filePath.isDefined && Files.exists(Paths.get(filePath.get))
  }
}

trait MaybeExistingFilePathParameter extends StringParameter {
  override val defaultValue = Some("")
  override lazy val errorMessage = key + " must be file name"

  override def condition(filePath: Option[String],
                         parameters: ArgumentParameterChecker)
  : Boolean = {
    filePath.isDefined
  }
}

trait ReadableFilePathParameter extends StringParameter {
  override val defaultValue = None
  override lazy val errorMessage = key + " must be an existing, readable file"

  override def condition(filePath: Option[String],
                         parameters: ArgumentParameterChecker)
  : Boolean = {
    filePath.isDefined && Files.isReadable(Paths.get(filePath.get))
  }
}

trait WritableFilePathParameter extends StringParameter {
  override val defaultValue = None
  override lazy val errorMessage = key + " must be a writable file"

  override def condition(filePath: Option[String],
                         parameters: ArgumentParameterChecker)
  : Boolean = {
    filePath.isDefined &&
      Files.exists(Paths.get(filePath.get).getParent) &&
      Files.isWritable(Paths.get(filePath.get).getParent)
  }
}