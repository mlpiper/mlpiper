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
package org.apache.flink.streaming.scala.examples.functions.sink

import java.io.ObjectOutputStream
import java.net.{ServerSocket, SocketException}

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
  * A server-socket based model sink: Waits for new connections to arrive
  * on [[port]] and writes serialized [[Model]]s to the receiver
  *
  * @param port port at which the sink listens
  * @tparam Model type of Model
  */
class ServerSocketStreamModelSink[Model](port: Int)
  extends RichSinkFunction[Model] {

  var server:ServerSocket = null

  override final def invoke(input: Model): Unit = {
    try {
      if (server == null) {
        server = new ServerSocket(port)
      }
    } catch {
      case e: Exception =>
        throw e
    }

    try {
      val client = server.accept

      val oos = new ObjectOutputStream(client.getOutputStream)
      oos.writeObject(input)
      oos.close()
      client.close()
    } catch {
      // TODO: Ignoring exception for now, figure out ones to handle
      case _: Exception =>
        server.close()
        server = null
    }
  }
}
