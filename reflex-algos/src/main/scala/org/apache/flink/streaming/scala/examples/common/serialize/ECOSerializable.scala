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
package org.apache.flink.streaming.scala.examples.common.serialize

object ECOSerializable {

  val ECO_CHARSET_NAME = "UTF-8"

  def header(lengthBytes: Long): Array[Byte] = {
    ("PARALLELMACHINES_MODEL:" + lengthBytes + ":END").getBytes(ECO_CHARSET_NAME)
  }

  def serialize(string: String): Array[Byte] = {
    string.getBytes(ECO_CHARSET_NAME)
  }
}

trait ECOSerializable extends FileSerializable {

  final def ecoSerialize(): String = {
    val bytes = ECOSerializable.serialize(this.serialize())
    val header = ECOSerializable.header(bytes.length)
    new String(header ++ bytes)
  }
}
