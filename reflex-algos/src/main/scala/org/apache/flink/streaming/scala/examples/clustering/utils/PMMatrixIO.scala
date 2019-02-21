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
package org.apache.flink.streaming.scala.examples.clustering.utils

import java.io.Serializable

import breeze.linalg.{DenseMatrix => BreezeDenseMatrix}
import org.slf4j.LoggerFactory

import scala.collection.immutable.HashMap

object PMMatrixIO {
  private val logger = LoggerFactory.getLogger(getClass)

  object ModelSaveFormat extends Enumeration {
    val FILE_SAVE = Value("file")
    val UNIT_TEST_SAVE = Value("test")
    val NO_SAVE = Value("null")

    /** @return CentroidSave of given name. NO_SAVE if name does not match any. */
    def apply(name: String): ModelSaveFormat.Value = {
      try { ModelSaveFormat.withName(name) } catch { case e: Exception => NO_SAVE }
    }
  }

  /** Enumeration of different centroid initialization methods. */
  object FileType extends Enumeration {
    /** Comma separated values. */
    val CSV = Value("csv")
    /** Space separated values */
    val SSV = Value("ssv")

    def hasValue(s: String) = values.exists(_.toString == s)

  }

  class FileFormat(val formatType: FileType.Value,
                   val separator: Char,
                   val quote: Char,
                   val escape: Char,
                   val skipLines: Int) extends Serializable {
    override def toString = s"FileFormat($formatType, $separator, $quote, $escape, $skipLines)"
  }

  /** CSV formatting HashMap containing arguments to BreezeDenseMatrix.csvread */
  object FileFormat {
    private val csvMap = HashMap[FileType.Value, FileFormat] (
      FileType.CSV -> new FileFormat(FileType.CSV, ',', '\u0000', '\\', 0),
      FileType.SSV -> new FileFormat(FileType.SSV, ' ', '\u0000', '\\', 0)
    )

    def apply(fileTypeName: String): FileFormat  = {
      try { csvMap(FileType.withName(fileTypeName)) } catch { case e: Exception => null }
    }
  }

  /**
    * Loads CSV and converts it to BreezeDenseMatrix
    *
    * @param file File to load matrix from.
    * @param fileFormat CSV format of file.
    * @return BreezeDenseMatrix representing CSV. Returns null on error.
    */
  def loadCSV(file: java.io.File,
              fileFormat: FileFormat)
  : BreezeDenseMatrix[Double] = {
    require(file != null)
    require(file.isFile, "Path is not a file: " + file.getAbsolutePath)
    require(file.canRead, "File is not readable: " + file.getAbsolutePath)

    val matrix: BreezeDenseMatrix[Double] = breeze.linalg.csvread(
      file,
      fileFormat.separator,
      fileFormat.quote,
      fileFormat.escape,
      fileFormat.skipLines)

    if (matrix == null) {
      logger.error("Invalid centroid CSV: could not convert to DenseMatrix")
    }
    matrix
  }

  /**
    * Loads CSV and converts it to BreezeDenseMatrix.
    *
    * @param path Path of CSV file.
    * @param csvFormat CSV format. See DataFormat object.
    * @return BreezeDenseMatrix representing CSV. Returns null on error.
    */
  def loadCSV(path: String,
              csvFormat: String)
  : BreezeDenseMatrix[Double] = {
    val fileFormat = FileFormat(csvFormat)
    require(fileFormat != null, "Invalid file format: " + csvFormat)

    val file: java.io.File = new java.io.File(path)
    loadCSV(file, fileFormat)
  }

  def writeCSV(matrix: BreezeDenseMatrix[Double],
               file: java.io.File,
               fileFormat: FileFormat)
  : Unit = {
    require(file != null)
    require(file.isFile, "Path is not a file: " + file.getAbsolutePath)
    require(file.canWrite, "File is not writeable: " + file.getAbsolutePath)

    breeze.linalg.csvwrite(file, matrix,
      fileFormat.separator,
      fileFormat.quote,
      fileFormat.escape,
      fileFormat.skipLines)
  }

  def writeCSV(matrix: BreezeDenseMatrix[Double], path: String, csvFormat: String): Unit = {
    val fileFormat = FileFormat(csvFormat)
    require(fileFormat != null, "Invalid file format: " + csvFormat)

    val file: java.io.File = new java.io.File(path)
    require(!file.exists, "File already exists: " + file.getAbsolutePath)

    file.createNewFile()
    writeCSV(matrix, file, fileFormat)
  }
}
