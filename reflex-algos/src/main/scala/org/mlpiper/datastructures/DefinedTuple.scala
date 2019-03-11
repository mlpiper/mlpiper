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
package org.mlpiper.datastructures

/**
  * Class with two [[Option]] values of [[L]] and [[R]]. Used for outputting two types
  * from operators with a single output.
  *
  * @param left Optional value of [[L]]
  * @param right Optional value of [[R]]
  * @tparam L Type of value [[L]]
  * @tparam R Type of value [[R]]
  */
class DefinedTwoTuple[L, R](val left: Option[L],
                            val right: Option[R])
  extends Serializable

/**
  * Class with three [[Option]] values of [[L]], [[M]], and [[R]]. Used for outputting three types
  * from operators with a single output.
  *
  * @param left Optional value of [[L]]
  * @param middle Optional value of [[M]]
  * @param right Optional value of [[R]]
  * @tparam L Type of value [[L]]
  * @tparam M Type of value [[M]]
  * @tparam R Type of value [[R]]
  */
class DefinedThreeTuple[L, M, R](val left: Option[L],
                                 val middle: Option[M],
                                 val right: Option[R])
  extends Serializable
