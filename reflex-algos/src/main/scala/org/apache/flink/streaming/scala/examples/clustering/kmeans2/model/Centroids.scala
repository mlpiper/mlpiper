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
package org.apache.flink.streaming.scala.examples.clustering.kmeans2.model

import java.io.Serializable

import breeze.linalg.functions.euclideanDistance
import breeze.linalg.{*, argmin, max, min, sum, DenseMatrix => BreezeDenseMatrix, DenseVector => BreezeDenseVector}
import breeze.numerics.pow
import org.apache.flink.streaming.scala.examples.clustering.math.LabeledVector
import org.apache.flink.streaming.scala.examples.clustering.utils.PMMatrixIO.FileFormat
import org.apache.flink.streaming.scala.examples.clustering.utils.{Constants, ParsingUtils}
import org.apache.flink.streaming.scala.examples.common.serialize.ECOSerializable

import scala.util.control.Breaks.{break, breakable}

@SerialVersionUID(8532344290292501598L)
class Centroids(var nrCenters: Int,
                var nrAttributes: Int,
                var centers: BreezeDenseMatrix[Double] = null,
                var normCenter: BreezeDenseVector[Double] = null,
                var weights: BreezeDenseVector[Double] = null,
                var distanceMean: BreezeDenseVector[Double] = null,
                var distanceVar: BreezeDenseVector[Double] = null) extends Serializable
  with ECOSerializable {

  if (centers != null) {
    require(validCentroids(centers), "centers do not match specified dimensions")
    if (weights == null) {
      weights = BreezeDenseVector.ones[Double](nrCenters)
    }
    if (normCenter == null) {
      normCenter = calculateSquaredNorms(centers)
    }
  }
  if (weights != null) {
    require(weights.length == nrCenters, "weights do not match specified dimensions")
  }

  /** Stores the squared norms for each centroid. */
  var squaredNormVector: BreezeDenseVector[Double] = _

  /** Initializes centers to be random and weights to ones. */
  def randInit: Centroids = {
    this.centers = BreezeDenseMatrix.rand[Double](nrCenters, nrAttributes)
    this.normCenter = calculateSquaredNorms(this.centers)
    this.weights = BreezeDenseVector.ones[Double](nrCenters)
    this
  }

  /** Sets centers as the matrix and weights to ones. Sets the mean and variance of distances if provided. */
  def matrixInit(matrix: BreezeDenseMatrix[Double]): Centroids = {
    require(validCentroids(matrix), "matrix does not match specified dimensions")
    this.centers = matrix
    this.normCenter = calculateSquaredNorms(this.centers)
    this.weights = BreezeDenseVector.ones[Double](nrCenters)
    this
  }

  /** Sets centers as the matrix and weights to ones. Sets the mean and variance of distances if provided. */
  def distanceStatisticsZeros(): Centroids = {
    this.distanceMean = BreezeDenseVector.zeros[Double](nrCenters)
    this.distanceVar = BreezeDenseVector.zeros[Double](nrCenters)
    this
  }

  /** Sets centers as the matrix and weights to ones. Sets the mean and variance of distances if provided. */
  def matrixInitDistance(matrix: BreezeDenseMatrix[Double],
                         distanceMean: BreezeDenseVector[Double] = null,
                         distanceVar: BreezeDenseVector[Double] = null,
                         weights: BreezeDenseVector[Double] = null): Centroids = {
    require(validCentroids(matrix), "matrix does not match specified dimensions")
    this.centers = matrix
    this.normCenter = calculateSquaredNorms(this.centers)

    if (weights != null) {
      this.weights = weights.copy
    } else {
      this.weights = BreezeDenseVector.zeros[Double](nrCenters)
    }

    if (distanceMean != null) {
      this.distanceMean = distanceMean
    }
    if (distanceVar != null) {
      this.distanceVar = distanceVar
    }
    this
  }

  /** Initialize centroids from a file.
    * TODO: add an option to read other statistics as well like mean and variance of distance from file.
    * */

  def fileInit(path: String, fileFormat: FileFormat = FileFormat("csv")): Centroids = {
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

    this.matrixInit(matrix)

  }

  /** Sets both the centers, weights and distance statistics to zeros. */
  def zeros: Centroids = {
    this.centers = BreezeDenseMatrix.zeros[Double](nrCenters, nrAttributes)
    this.normCenter = BreezeDenseVector.zeros[Double](nrCenters)
    this.weights = BreezeDenseVector.zeros[Double](nrCenters)
    this.distanceMean = BreezeDenseVector.zeros[Double](nrCenters)
    this.distanceVar = BreezeDenseVector.zeros[Double](nrCenters)
    this
  }

  /** Sets both the centers and weights to ones and distance statistics to zeros. */
  def ones: Centroids = {
    this.centers = BreezeDenseMatrix.ones[Double](nrCenters, nrAttributes)
    this.normCenter = calculateSquaredNorms(this.centers)
    this.weights = BreezeDenseVector.ones[Double](nrCenters)
    this.distanceMean = BreezeDenseVector.zeros[Double](nrCenters)
    this.distanceVar = BreezeDenseVector.zeros[Double](nrCenters)
    this
  }

  def nrCentroids: Int = nrCenters

  def attributes: Int = nrAttributes

  /**
    * General equations to calculate how the centroids area progressed during the online kmeans
    * updates and how they are aggregated, and also how the equations of calculating mean and
    * variance metrics and how they are aggregated, are provided  in:
    * https://parallelmachines.atlassian.net/wiki/spaces/DEV/pages/17367067/Online+K-means
    */

  /** @return Copy of this centroid object. */
  def copy: Centroids = {
    require(centers != null, "Centroid's centers cannot be null")
    require(weights != null, "Centroid's associated weights cannot be null")
    require(normCenter != null, "Centroid's associated norm centers cannot be null")
    if (distanceMean != null && distanceVar != null) {
      new Centroids(nrCenters, nrAttributes, centers.copy, normCenter.copy, weights.copy, distanceMean.copy, distanceVar.copy)
    }
    else {
      new Centroids(nrCenters, nrAttributes, centers.copy, normCenter.copy, weights.copy)
    }
  }

  def selfCopy(toCopy: Centroids): Centroids = {
    require(toCopy.centers != null, "null centers cannot be copied")
    require(toCopy.weights != null, "null weights cannot be copied")
    require(normCenter != null, "null norm centers cannot be copied")
    this.centers = toCopy.centers.copy
    this.normCenter = toCopy.normCenter.copy
    this.weights = toCopy.weights.copy
    this.distanceMean = toCopy.distanceMean
    this.distanceVar = toCopy.distanceVar
    this
  }

  /** Scale a Centroids object with a factor. */
  def selfScale(factor: Double): Centroids = {
    this.centers :*= factor
    this.normCenter :*= factor
    this.weights :*= factor
    if (distanceMean != null && distanceVar != null) {
      this.distanceMean :*= factor
      this.distanceVar :*= factor
    }
    this
  }

  /** Calculates the squared norm vector for the centroids. */
  def calculateSquaredNorms: Centroids = {
    this.squaredNormVector = calculateSquaredNorms(centers)
    this
  }

  /** Calculates the squared norm vector for the centers. */
  def calculateSquaredNorms(centers: BreezeDenseMatrix[Double]): BreezeDenseVector[Double] = {
    val squaredMatrix = centers :* centers
    val squaredNormVector = sum(squaredMatrix(*, ::))
    squaredNormVector
  }

  def +(input: Centroids): Centroids = {
    new Centroids(nrCenters, nrAttributes,
      this.centers + input.centers, this.normCenter + input.normCenter, this.weights + input.weights)
  }

  private def replaceZeroWeights(weights: BreezeDenseVector[Double]): (BreezeDenseVector[Double]) = {
    val newWeights = weights.copy
    for (bucket <- 0 to (this.nrCenters - 1)) {
      if (newWeights(bucket) == 0) {
        newWeights(bucket) = 1
      }
    }
    newWeights
  }

  /** This function helps with calculation of distance and mean statistics. */
  private def addDistanceMeanAndVar(input: Centroids): (BreezeDenseVector[Double]) = {
    val addedWeights = this.weights + input.weights
    val newWeightsInput = replaceZeroWeights(input.weights)
    val newWeightsThis = replaceZeroWeights(this.weights)
    val newWeightsAdded = replaceZeroWeights(addedWeights)
    val distanceMeanNew = (this.distanceMean + input.distanceMean) :/ (newWeightsAdded)
    val diffMean1 = this.weights :* pow(distanceMeanNew - this.distanceMean :/ newWeightsThis, 2)
    val diffMean2 = input.weights :* pow(distanceMeanNew - input.distanceMean :/ newWeightsInput, 2)
    val distanceVarNew = this.distanceVar + input.distanceVar + diffMean1 + diffMean2
    distanceVarNew
  }

  /** This function helps with calculation of distance and mean statistics. */
  private def subtractDistanceMeanAndVar(input: Centroids): (BreezeDenseVector[Double]) = {
    val subtractedWeights = this.weights + input.weights
    val newWeightsInput = replaceZeroWeights(input.weights)
    val newWeightsThis = replaceZeroWeights(this.weights)
    val newWeightsSubtracted = replaceZeroWeights(subtractedWeights)
    val distanceMeanNew = (this.distanceMean - input.distanceMean) / (newWeightsSubtracted)
    val diffMean1 = this.weights :* pow(distanceMeanNew - this.distanceMean / newWeightsThis, 2)
    val diffMean2 = input.weights :* pow(distanceMeanNew - input.distanceMean / newWeightsInput, 2)
    val distanceVarNew = this.distanceVar - input.distanceVar + diffMean1 - diffMean2
    distanceVarNew
  }

  /** Add mean and variance statistics. */
  def addMV(input: Centroids): (Centroids) = {
    if (distanceMean != null && distanceVar != null) {
      val distanceVarNew = addDistanceMeanAndVar(input)
      val distanceMeanNew = this.distanceMean + input.distanceMean
      new Centroids(nrCenters, nrAttributes,
        this.centers + input.centers, this.normCenter + input.normCenter, this.weights + input.weights, distanceMeanNew, distanceVarNew)
    }
    else {
      this + input
    }
  }

  /** Add mean and variance statistics. */
  def addMVSelf(input: Centroids): (Centroids) = {

    if (distanceMean != null && distanceVar != null) {
      this.distanceMean += input.distanceMean
      this.distanceVar += input.distanceVar
    }
    this.centers = this.centers + input.centers
    this.normCenter = this.normCenter + input.normCenter
    this.weights = this.weights + input.weights
    this
  }


  def sumDistancesMean(input: Centroids): Centroids = {
    new Centroids(nrCenters, nrAttributes, this.centers, input.normCenter, this.weights + input.weights,
      this.distanceMean + input.distanceMean, this.distanceVar)
  }

  def sumDistancesVar(input: Centroids): Centroids = {
    new Centroids(nrCenters, nrAttributes, this.centers, input.normCenter, this.weights + input.weights,
      this.distanceMean, this.distanceVar + input.distanceVar)
  }

  def +=(input: Centroids) = {
    this.centers += input.centers
    this.normCenter += input.normCenter
    this.weights += input.weights
  }


  def -(input: Centroids): Centroids = {
    new Centroids(nrCenters, nrAttributes,
      this.centers - input.centers, this.normCenter - input.normCenter, this.weights - input.weights)
  }

  def -=(input: Centroids) = {
    this.centers -= input.centers
    this.normCenter -= input.normCenter
    this.weights -= input.weights
  }

  /** Subtract mean and variance statistics. */
  def subtractMV(input: Centroids): Centroids = {
    if (distanceMean != null && distanceVar != null) {
      val distanceVarNew = subtractDistanceMeanAndVar(input)
      new Centroids(nrCenters, nrAttributes,
        this.centers - input.centers, this.normCenter - input.normCenter, this.weights - input.weights, this.distanceMean - input.distanceMean, distanceVarNew)
    }
    else {
      this - input
    }
  }

  /** Compares centroids by calculating Euclidean distance between centers. */
  def equal(other: Centroids,
            epsilon: Double,
            distanceFlag: Boolean = false): Boolean = {
    var centroidsEq: Boolean = true
    if (this.nrCentroids != other.nrCentroids ||
      this.attributes != other.attributes) {
      return false
    }
    breakable {
      for (i <- 0 until this.centers.rows) {
        val dist: Double = euclideanDistance(this.centers(i, ::).t, other.centers(i, ::).t)
        if (dist > epsilon) {
          centroidsEq = false
          break
        }
      }
      if (distanceFlag) {
        for (i <- 0 until this.distanceMean.length) {
          val distMean: Double = Math.abs(this.distanceMean(i) - other.distanceMean(i))
          val distVar: Double = Math.abs(this.distanceVar(i) - other.distanceVar(i))
          if ((distMean > epsilon) || (distVar > epsilon)) {
            centroidsEq = false
            break
          }
        }
      }
    }
    centroidsEq
  }

  /** Adds input to the respective centroid and increments respective weight by one. */
  def addInput(input: BreezeDenseVector[Double], bucket: Int): Centroids = {
    this.weights(bucket) += 1.0
    this.centers(bucket, ::) :+= input.t
    this.normCenter(bucket) += sum(input.t :* input.t)
    this
  }

  def addDistanceStatisticsOnline(input: BreezeDenseVector[Double], bucket: Int, currentCentroid: Centroids, varDist: Double): Centroids = {
    this.distanceMean(bucket) += euclideanDistance(currentCentroid.centers(bucket, ::).t, input)
    this.distanceVar(bucket) += varDist
    this
  }

  /** Adds the sample distance to the closest centroid and increments respective weight by one. */
  def addInputDistanceBatch(input: BreezeDenseVector[Double], bucket: Int): Centroids = {
    this.weights(bucket) += 1.0
    this.distanceMean(bucket) += euclidDistance(input, bucket)
    this
  }

  /** Adds the sample variance to the respective centroid and increments respective weight by one. */
  def addInputVarBatch(input: BreezeDenseVector[Double], bucket: Int): Centroids = {
    this.weights(bucket) += 1.0
    val dist = euclidDistance(input, bucket)
    this.distanceVar(bucket) += (dist - this.distanceMean(bucket)) * (dist - this.distanceMean(bucket))
    this
  }

  def addInput(input: LabeledVector[Double]): Centroids = {
    addInput(input.vector, input.label.toInt)
  }

  def addDistanceStatisticsOnline(input: LabeledVector[Double], currentCentroid: Centroids, varDist: Double): Centroids = {
    addDistanceStatisticsOnline(input.vector, input.label.toInt, currentCentroid, varDist)
  }

  /**
    * Calculates optimized squared distances (||x * x|| - 2 * x * y),
    * and returns the row-vector index with the minimum distance.
    */
  def predict(input: BreezeDenseVector[Double]): Int = {
    val offsetSquaredDistanceVector = this.squaredNormVector - 2.0 * (centers * input).toDenseVector
    argmin(offsetSquaredDistanceVector)
  }

  /**
    * @return Euclidean distance from the input to the centroid at the specified bucket.
    */
  def euclidDistance(input: BreezeDenseVector[Double], bucket: Int): Double = {
    euclideanDistance(centers(bucket, ::).t, input)
  }

  /**
    * Updates the centroid bucket using the formula
    * centroid = centroid + (n_2(input - centroid) / (n_1 + n_2))
    * = (n_1 centroid + n_2 input) / (n_1 + n_2)
    *
    * where n_1 is the weight of the centroid before it was updated
    * n_2 is the weight of the input
    *
    * and updates the squaredNormVector bucket
    */
  def updateCentroids(input: BreezeDenseVector[Double],
                      bucket: Int,
                      weight: Double = 1.0)
  : Centroids = {
    weights(bucket) += weight

    val closestCentroid = centers(bucket, ::)

    centers(bucket, ::) := closestCentroid + ((input.t - closestCentroid) * weight) / weights(bucket)

    // Update squared norm of new centroid
    this.squaredNormVector(bucket) = sum(centers(bucket, ::) :* centers(bucket, ::))
    this
  }

  def updateDistanceStatistics(input: BreezeDenseVector[Double], bucket: Int): (Double, Centroids) = {
    val distCalc = euclidDistance(input, bucket) //problematic - calculates the distance to the new center
    val distMean = distanceMean(bucket)
    distanceMean(bucket) = distMean + (distCalc - distMean) / weights(bucket)
    val varDist = (distCalc - this.distanceMean(bucket)) * (distCalc - distMean)

    if (weights(bucket) > 1) {
      distanceVar(bucket) = ((weights(bucket) - 2) * distanceVar(bucket) + varDist) / (weights(bucket) - 1)
    }
    (varDist, this)
  }

  def mergeDistanceStatistics(mean: Double, vars: Double, bucket: Int, weight: Double): (Centroids) = {
    val distMean = distanceMean(bucket)
    distanceMean(bucket) = (distMean * weights(bucket) + mean * weight) / (weights(bucket) + weight)
    val varDist = weights(bucket) * math.pow(distMean - distanceMean(bucket), 2) +
      weight * math.pow(mean - distanceMean(bucket), 2)

    distanceVar(bucket) = ((weights(bucket) - 1) * distanceVar(bucket) +
      (weight - 1) * vars + varDist) /
      (weights(bucket) + weight - 1)
    this
  }

  def updateNormAVG(input: BreezeDenseVector[Double], bucket: Int): Centroids = {
    val closestNormCenter = normCenter(bucket)
    normCenter(bucket) += (sum(input.t :* input.t) - closestNormCenter) / weights(bucket)
    this
  }

  def silhouetteScore(input: BreezeDenseVector[Double], bucket: Int): Double = {
    /* TODO :
     *  1) Introduce a window concept for the score - not to aggregate it from beginning of time
     *  2) Do a score for the inference kmeans. It is needed to replace in the equation the centers
     *    which in inference are constant and replace it with a running mean of the points.
     */

    val closestNormCenter = normCenter(bucket)
    // a is the average distance of point i from the points of its cluster
    val a_in = sum(input.t :* input.t) + closestNormCenter - 2 * sum(input.t :* centers(bucket, ::))
    var a = 0.0
    if (a_in > 0) {
      a = math.sqrt(a_in)
    }
    // b is the average distance of point i from the points of its nearest cluster
    // for that first calculating bOffset which is the distance to all clusters
    // then putting a very high value in its current cluster value so that it will not have the
    // minimal value and then minimizing on bOffset to find b.
    val bOffset = normCenter - 2.0 * (centers * input).toDenseVector
    bOffset(bucket) = Double.MaxValue

    val b_in = sum(input.t :* input.t) + min(bOffset)
    var b = 0.0
    if (b_in > 0) {
      b = math.sqrt(b_in)
    }

    val max_dist = max(a, b)
    var silhouetteScore = 0.0
    if (max_dist > 0) {
      silhouetteScore = (b - a) / max_dist
    }

    silhouetteScore
  }

  def updateNormAVG(input: LabeledVector[Double]): Centroids = {
    updateNormAVG(input.vector, input.label.toInt)
  }

  def updateCentroids(input: LabeledVector[Double]): Centroids = {
    updateCentroids(input.vector, input.label.toInt)
  }

  def updateDistanceStatistics(input: LabeledVector[Double]): (Double, Centroids) = {
    updateDistanceStatistics(input.vector, input.label.toInt)
  }

  /** @return If input has valid length. */
  def isValidInput(input: BreezeDenseVector[Double]): Boolean = {
    input != null && input.length == nrAttributes
  }

  /** Multiplies the centroids by its weights to obtain the aggregation of points. */
  def selfUnWeight: Centroids = {
    this.centers(::, *) *= this.weights
    this.normCenter *= this.weights
    this
  }

  def weight: Centroids = {
    if (distanceMean != null && distanceVar != null) {
      val newWeights = this.weights.copy
      val weightsVar = replaceZeroWeights(newWeights)
      new Centroids(nrCenters, nrAttributes, this.centers(::, *) / this.weights,
        this.normCenter / this.weights, this.weights.copy, this.distanceMean / this.weights, this.distanceVar / weightsVar)
    }
    else {
      new Centroids(nrCenters, nrAttributes, this.centers(::, *) / this.weights,
        this.normCenter / this.weights, this.weights.copy)
    }
  }

  /** Divides the centroids by its weights to average the points. */
  def selfWeight: Centroids = {
    this.centers(::, *) /= this.weights
    this.normCenter /= this.weights
    if (this.distanceMean != null && this.distanceVar != null) {
      this.distanceMean /= this.weights
      val newWeights = this.weights.copy
      val weightsVar = replaceZeroWeights(newWeights)
      this.distanceVar /= weightsVar
    }
    this
  }

  /** Divides the centroid distance means by its weights to average the points. */
  def selfWeightDistanceMean: Centroids = {
    this.distanceMean /= this.weights
    this
  }

  /** Divides the centroids distance variance by its weights to average the points. */
  def selfWeightDistanceVar: Centroids = {
    this.distanceVar /= this.weights
    this
  }

  /** Scales the centroids by gamma and adds the deltaCentroids. */
  def applyDecay(deltaCentroids: Centroids, gamma: Double): Centroids = {
    this.selfScale(gamma)
    this addMVSelf deltaCentroids
    this
  }

  def calculateDistanceThresholdVector(distanceThresholdValue: Double): BreezeDenseVector[Double] = {
    var newDistanceThresholdVector = BreezeDenseVector.fill[Double](v = distanceThresholdValue, size = this.nrCentroids)

    if (this.distanceMean != null && this.distanceVar != null) {
      newDistanceThresholdVector = this.distanceMean :+ (newDistanceThresholdVector :* pow(this.distanceVar, 0.5))
    }

    newDistanceThresholdVector
  }

  override def toString = s"Centroids($nrCenters, $nrAttributes, $centers, $normCenter, $weights)"

  /** @return True of centersToEvaluate match the Centroid object's specified dimensions. */
  private def validCentroids(centersToEvaluate: BreezeDenseMatrix[Double]): Boolean = {
    centersToEvaluate != null &&
      centersToEvaluate.rows == nrCenters && centersToEvaluate.cols == nrAttributes
  }


  /**
    * Serializes Centroids to a CSV, converted to a byte array then back to a String for ECO to
    * read. [[org.apache.flink.streaming.util.serialization.SimpleStringSchema]] will deserialize
    * it when sending it to a Kafka Producer.
    *
    * The first byte in the String contains the total size of the model.
    * Each row is appended with a new line character.
    *
    * This is temporary for the NEV release. We need to better define serialization methods.
    * We also need a proper byte serializer/deserializer
    *
    * @return
    */
  @deprecated
  override def serialize(): String = {
    val stb = new StringBuilder
    for (i <- 0 until centers.rows) {
      stb.append(ParsingUtils.breezeDenseVectorToString(centers(i, ::).t, ',').get)
      stb.append(Constants.NEW_LINE)
    }
    stb.append(ParsingUtils.breezeDenseVectorToString(distanceMean, ',').get)
    stb.append(Constants.NEW_LINE)
    stb.append(ParsingUtils.breezeDenseVectorToString(distanceVar, ',').get)
    stb.append(Constants.NEW_LINE)

    stb.toString()
  }
}

