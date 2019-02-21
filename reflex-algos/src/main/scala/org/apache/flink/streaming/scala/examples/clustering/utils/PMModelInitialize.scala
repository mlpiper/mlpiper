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

import breeze.linalg.{DenseMatrix => BreezeDenseMatrix, DenseVector => BreezeDenseVector}
import Constants.NEW_LINE
import org.apache.flink.streaming.scala.examples.clustering.utils.PMMatrixIO.FileFormat

class PMModelInitialize(nrRows: Int, nrColumns: Int) {

  private var _model: BreezeDenseMatrix[Double] = _
  private var _vectorModel: BreezeDenseVector[Double] = _

  /**
    * Constructor to initialize a vector
    * @param vectorLength Length of the vector
    */
  def this(vectorLength: Int) {
    // A vector represented as a matrix will always have one row
    this(1, vectorLength)
  }

  def validModel(model: BreezeDenseMatrix[Double]): Boolean = {
    model != null && model.rows == nrRows && model.cols == nrColumns
  }

  def validVectorModel(vectorModel: BreezeDenseVector[Double]): Boolean = {
    vectorModel != null && (vectorModel.length == nrColumns)
  }

  def fromMatrix(matrixModel: BreezeDenseMatrix[Double]): PMModelInitialize = {
    require(validModel(matrixModel), "Cannot initialize model: Invalid dimensions")
    this._model = matrixModel
    this
  }

  def fromVector(vectorModel: BreezeDenseVector[Double]): PMModelInitialize = {
    require(validVectorModel(vectorModel), "Cannot initialize model: Invalid dimensions")
    this._vectorModel = vectorModel
    this
  }

  /**
    * Initialize model from a file.
    *
    * @param path Path to centroids file.
    * @param fileFormat Format of the centroids file. Defaults to CSV
    */
  def fromFile(path: String,
               fileFormat:FileFormat = FileFormat("csv")): PMModelInitialize = {
    require(path != null, "Path to model is null")
    require(fileFormat != null, "FileFormat type for model is null")

    val file: java.io.File = new java.io.File(path)
    require(file != null && file.isFile && file.canRead, "Invalid file: " + path)

    val matrix: BreezeDenseMatrix[Double] = breeze.linalg.csvread(
      file,
      fileFormat.separator,
      fileFormat.quote,
      fileFormat.escape,
      fileFormat.skipLines)

    require(matrix != null && matrix.rows == 1, "Invalid model: could not convert to DenseMatrix")
    fromVector(matrix(0,::).t)
  }

  def model: BreezeDenseMatrix[Double] = {
    require(_model != null, "Model is null: has not been properly initialized")
    _model
  }
  def vectorModel: BreezeDenseVector[Double] = {
    require(_vectorModel != null, "Model is null: has not been properly initialized")
    _vectorModel
  }

}
