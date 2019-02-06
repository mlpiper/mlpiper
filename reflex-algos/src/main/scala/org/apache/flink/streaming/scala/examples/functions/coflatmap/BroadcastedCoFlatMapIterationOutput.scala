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
package org.apache.flink.streaming.scala.examples.functions.coflatmap

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.scala.examples.common.parameters.pipeline.MaxWaitTimeMillis
import org.apache.flink.streaming.scala.examples.common.parameters.tools.ArgumentParameterMap

object BroadcastedCoFlatMapIterationOutput {

  /**
    * Performs a [[RichCoFlatMapFunction]] on [[IN]] and outputs [[BCF_OUT]].
    * A split is performed on [[BCF_OUT]], creating two DataStreams of [[BCF]] and [[OUT]].
    * [[BCF]] is broadcasted and fed back to the [[RichCoFlatMapFunction]] while [[OUT]] is
    * returned from the iteration.
    *
    * @param inputStream Input stream to perform [[BroadcastedCoFlatMapIterationOutput]] on.
    * @param extractFeedback Function to split [[BCF_OUT]] into two separate DataStreams
    * @param feedbackFunctionBeforeBroadcast Function performed on [[BCF]] DataStream
    *                                        before it is broadcasted.
    * @param coFlatMap [[RichCoFlatMapFunction]] that will broadcast [[BCF]] from [[BCF_OUT]]
    * @param parameters Parameters to extract [[MaxWaitTimeMillis]]
    *
    * @param in Evidence parameter for [[IN]]
    * @param bcf Evidence parameter for [[BCF]]
    * @param out Evidence parameter for [[OUT]]
    * @param bcfOut Evidence parameter for [[BCF_OUT]]
    *
    * @tparam IN Input DataStream type and IN1 of the [[RichCoFlatMapFunction]]
    * @tparam BCF IN2 of the [[RichCoFlatMapFunction]] to be broadcasted from [[BCF_OUT]]
    * @tparam OUT Output of the iteration function after splitting [[BCF_OUT]]
    * @tparam BCF_OUT OUT of the [[RichCoFlatMapFunction]], to be split
    *
    * @return DataStream of [[OUT]]
    */
  def apply[IN, BCF, OUT, BCF_OUT](inputStream: DataStream[IN],
                                   extractFeedback: DataStream[BCF_OUT] => (DataStream[BCF], DataStream[OUT]),
                                   feedbackFunctionBeforeBroadcast: DataStream[BCF] => DataStream[BCF],
                                   coFlatMap: RichCoFlatMapFunction[IN, BCF, BCF_OUT],
                                   parameters: ArgumentParameterMap
                                  )(
                                    implicit in: TypeInformation[IN],
                                    bcf: TypeInformation[BCF],
                                    out: TypeInformation[OUT],
                                    bcfOut: TypeInformation[BCF_OUT]
                                  )
  : (DataStream[OUT]) = {

    inputStream.iterate((input: ConnectedStreams[IN, BCF]) => {
      val feedbackOutputStream: DataStream[BCF_OUT] = input.flatMap(coFlatMap)(bcfOut)

      val feedbackOutputSplit = extractFeedback(feedbackOutputStream)
      val feedbackStream = feedbackOutputSplit._1
      val outputStream = feedbackOutputSplit._2

      // Used the feedbackFunction to manipulate the feedbackDataStream further before broadcasting
      var manipulatedFeedbackStream = feedbackFunctionBeforeBroadcast(feedbackStream)

      // If the output is a SingleOutputStreamOperator (can only have parallelism of one), we
      // must perform a no-op that allows the DataStream's parallelism to change to match the
      // inputStream parallelism
      if (manipulatedFeedbackStream.javaStream.isInstanceOf[SingleOutputStreamOperator[BCF]]) {
        manipulatedFeedbackStream = manipulatedFeedbackStream.map(x => x)
      }

      manipulatedFeedbackStream = manipulatedFeedbackStream
        // We set the parallelism of the feedback stream to match the inputStream parallelism.
        // If the two's parallelism do not match, it throws an UnsupportedOperationException.
        .setParallelism(inputStream.parallelism)
        .broadcast

      (manipulatedFeedbackStream, outputStream)
    }, parameters.get(MaxWaitTimeMillis).get)
  }

  /**
    * Performs a [[RichCoFlatMapFunction]] on [[IN]] and outputs [[BCF_OUT]].
    * A split is performed on [[BCF_OUT]], creating two DataStreams of [[BCF]] and [[OUT]].
    * [[BCF]] is broadcasted and fed back to the [[RichCoFlatMapFunction]] while [[OUT]] is
    * returned from the iteration.
    *
    * @param inputStream Input stream to perform [[BroadcastedCoFlatMapIterationOutput]] on.
    * @param extractFeedback Function to split [[BCF_OUT]] into two separate DataStreams
    * @param coFlatMap [[RichCoFlatMapFunction]] that will broadcast [[BCF]] from [[BCF_OUT]]
    * @param parameters Parameters to extract [[MaxWaitTimeMillis]]
    *
    * @param in Evidence parameter for [[IN]]
    * @param bcf Evidence parameter for [[BCF]]
    * @param out Evidence parameter for [[OUT]]
    * @param bcfOut Evidence parameter for [[BCF_OUT]]
    *
    * @tparam IN Input DataStream type and IN1 of the [[RichCoFlatMapFunction]]
    * @tparam BCF IN2 of the [[RichCoFlatMapFunction]] to be broadcasted from [[BCF_OUT]]
    * @tparam OUT Output of the iteration function after splitting [[BCF_OUT]]
    * @tparam BCF_OUT OUT of the [[RichCoFlatMapFunction]], to be split
    *
    * @return DataStream of [[OUT]]
    */
  def apply[IN, BCF, OUT, BCF_OUT](inputStream: DataStream[IN],
                                   extractFeedback: DataStream[BCF_OUT] => (DataStream[BCF], DataStream[OUT]),
                                   coFlatMap: RichCoFlatMapFunction[IN, BCF, BCF_OUT],
                                   parameters: ArgumentParameterMap
                                  )(
                                   implicit in: TypeInformation[IN],
                                   bcf: TypeInformation[BCF],
                                   out: TypeInformation[OUT],
                                   bcfOut: TypeInformation[BCF_OUT]
                                  )
  : (DataStream[OUT]) = {
    // Does not manipulate the BCF DataStream after coFlatMap,
    // meaning it will be immediately broadcasted and fed back to the coFlatMap
    apply(inputStream, extractFeedback, (x: DataStream[BCF]) => x, coFlatMap, parameters)
  }
}
