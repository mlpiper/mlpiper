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
package org.apache.flink.streaming.scala.examples.clustering.math

import breeze.linalg.{DenseVector => BreezeDenseVector}
import org.apache.flink.streaming.scala.examples.clustering.utils.ParsingUtils

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.{specialized => spec}

/** This class represents a Breeze Dense vector with an associated label as it is required
  * for many supervised learning tasks.
  */
class LabeledVector[@spec(Double, Int, Float, Long) V](labelData: Option[Double],
                                                       data: BreezeDenseVector[V],
                                                       vectorTimestamp: Option[Long])(implicit cm: ClassTag[V])
  extends Serializable {

  /**
    * stores the timestamp when the vector was generated - this provides an approximate
    * measure of when vector entered the system
    */
  val generationTimestamp: Long = System.currentTimeMillis()

  def this(labelData: Double, vector: BreezeDenseVector[V])(implicit cm: ClassTag[V]) {
    this(Some(labelData), vector, None)
  }

  def this(labelData: Double, array: Array[V])(implicit cm: ClassTag[V]) {
    this(Some(labelData), new BreezeDenseVector[V](array), None)
  }

  def this(vector: BreezeDenseVector[V])(implicit cm: ClassTag[V]) {
    this(None, vector, None)
  }

  def this(array: Array[V])(implicit cm: ClassTag[V]) {
    this(None, new BreezeDenseVector[V](array), None)
  }

  def this(labelData: Double, vector: BreezeDenseVector[V], timestamp: Long)(implicit cm: ClassTag[V]) {
    this(Some(labelData), vector, Some(timestamp))
  }

  def this(labelData: Double, array: Array[V], timestamp: Long)(implicit cm: ClassTag[V]) {
    this(Some(labelData), new BreezeDenseVector[V](array), Some(timestamp))
  }

  def this(vector: BreezeDenseVector[V], timestamp: Long)(implicit cm: ClassTag[V]) {
    this(None, vector, Some(timestamp))
  }

  def this(array: Array[V], timestamp: Long)(implicit cm: ClassTag[V]) {
    this(None, new BreezeDenseVector[V](array), Some(timestamp))
  }

  /**
    * @return The [[BreezeDenseVector]] of the [[LabeledVector]].
    */
  def vector: BreezeDenseVector[V] = data

  /**
    * @throws NoSuchElementException If the [[LabeledVector]] does not contain a label.
    * @return Label
    */
  def label: Double = this.labelData.get

  /**
    * @return Whether the [[LabeledVector]] contains a label.
    */
  def hasLabel: Boolean = this.labelData.isDefined

  /**
    * @throws NoSuchElementException If the [[LabeledVector]] does not contain a timestamp.
    * @return Timestamp to
    */
  def timestamp: Long = vectorTimestamp.get

  /**
    * @return Whether the [[LabeledVector]] contains a timestamp.
    */
  def hasTimestamp: Boolean = this.vectorTimestamp.isDefined

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: LabeledVector[V] =>
        val equalVectors = this.vector.equals(that.vector)
        if (this.hasLabel && that.hasLabel) {
          equalVectors && this.label.equals(that.label)
        } else if (!this.hasLabel && !that.hasLabel) {
          equalVectors
        } else { // One vector has a label and the other doesn't
          false
        }
      case _ => false
    }
  }

  def toJson(): String = {
    val map = mutable.Map[String, Any](
      "data" -> vector.toArray.toList
    )
    if (hasLabel) {
      map("label") = label
    }

    if (hasTimestamp) {
      map("timestamp") = timestamp
    }

    ParsingUtils.iterableToJSON(map)
  }
}
