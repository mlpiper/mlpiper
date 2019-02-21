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

import java.io._
import java.util.{InvalidPropertiesFormatException, Properties}

import org.apache.hadoop.mapred.InvalidFileTypeException
import org.slf4j.LoggerFactory


object PropertiesWrapper {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  /**
    * Converts a file path to [[java.util.Properties]]. Supports files with extensions
    * .properties and .xml.
    *
    * @param filePath File path to convert to [[java.util.Properties]].
    * @return Defined [[Option]] if the properties file was parsed correctly. None otherwise.
    */
  def toProperties(filePath: String): Option[Properties] = {
    var toReturn: Option[Properties] = None
    val properties: Properties = new Properties()

    try {
      val propertiesFile = new File(filePath)
      val propertiesStream = new FileInputStream(propertiesFile)
      if (filePath.endsWith(".properties")) {
        properties.load(propertiesStream)
      } else if (filePath.endsWith(".xml")) {
        properties.loadFromXML(propertiesStream)
      } else {
        throw new InvalidFileTypeException()
      }
      toReturn = Some(properties)
    } catch {
      case invalidFile: InvalidFileTypeException =>
        LOG.error("File must be of type .properties or .xml")
      case fileNotFound: FileNotFoundException =>
        LOG.error("File at path " + filePath + " not found")
      case nullPointer: NullPointerException =>
        LOG.error("File path provided is null")
      case security: SecurityException =>
        LOG.error("Read access is denied for file " + filePath)
      case illegalArgument: IllegalArgumentException =>
        LOG.error("The file " + filePath + " contains a malformed Unicode escape sequence")
      case io: IOException =>
        LOG.error("An error occurred reading " + filePath)
      case unsupportedEncoding: UnsupportedEncodingException =>
        LOG.error("The xml configuration " + filePath + " encoding is not supported")
      case invalidProperties: InvalidPropertiesFormatException =>
        LOG.error("The xml configuration " + filePath + " is not a valid xml file.")
      case default: Throwable =>
        LOG.error(default.getMessage)
    }
    toReturn
  }
}
