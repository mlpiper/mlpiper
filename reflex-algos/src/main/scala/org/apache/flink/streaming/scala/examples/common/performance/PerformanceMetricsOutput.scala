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
package org.apache.flink.streaming.scala.examples.common.performance

import org.apache.flink.streaming.scala.examples.common.stats.StatInfo
import org.apache.flink.streaming.scala.examples.functions.common.DefinedTwoTuple

import scala.collection.mutable

case class PerformanceMetricsHash(metrics: mutable.HashMap[StatInfo, Any])
  extends Serializable {

  private val names: mutable.HashSet[String] = new mutable.HashSet[String]
  metrics.keys.foreach(addUniqueStatName)

  private def addUniqueStatName(statInfo: StatInfo): Unit = {
    val uniqueName: Boolean = names.add(statInfo.name)
    if (!uniqueName) {
      throw new IllegalArgumentException(s"StatInfo with name ${statInfo.name} already exists in PerformanceMetricsHash")
    }
  }

  def put(keyValues: (StatInfo, Any)*): Unit = {
    keyValues.foreach(kv => {
      addUniqueStatName(kv._1)
      metrics.put(kv._1, kv._2)
    })
  }

  def apply(key: String): Any = {
    val statInfo = metrics.keys.find(_.name == key)
    if (statInfo.isDefined) {
      metrics(statInfo.get)
    } else {
      throw new NoSuchElementException(s"StatInfo with name $key does not exist")
    }
  }

  def contains(key: String): Boolean = names.contains(key)
}

case class PerformanceMetricsOutput[T](metrics: Option[PerformanceMetricsHash],
                                       output: Option[T])
  extends DefinedTwoTuple[PerformanceMetricsHash, T](metrics, output)
