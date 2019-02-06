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
package org.apache.flink.streaming.scala.examples.common.ml

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.scala.examples.common.parameters.tools.ArgumentParameterMap
import org.apache.flink.streaming.scala.examples.common.performance.{PerformanceMetricsHash, PerformanceMetricsOutput}
import org.apache.flink.streaming.scala.examples.common.stats.{StatInfo, StatNames, StatPolicy}
import org.apache.flink.streaming.scala.examples.functions.coflatmap.BroadcastedCoFlatMapIterationOutput
import org.apache.flink.streaming.scala.examples.functions.common.{DefinedThreeTuple, DefinedTwoTuple}
import org.apache.flink.streaming.scala.examples.functions.join.DefinedJoin
import org.apache.flink.streaming.scala.examples.functions.split.DefinedSplit
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * Construct types to return a [[Prediction]] DataStream and [[Model]] DataStream from a
  * RichCoFlatMap function that broadcasts its [[LocalAggregation]].
  *
  * Extending this function gives access to an IterationOutput and Output class for sensible
  * outputs to BroadcastedCoFlatMapIterationOutput. An Output object is also accessible for
  * helper functions to easily collect any of the three outputs.
  * {{{
  *   Object TrainingAlgorithm extends TrainingCoFlatMapFactory[LocalAggregation, Prediction, Model]
  *   ...
  *   out.collect( TrainingAlgorithm.Output.fromLocalAggregation(localAggregation) )
  *   out.collect( TrainingAlgorithm.Output.fromGlobalModel(globalModel) )
  *   out.collect( TrainingAlgorithm.Output.fromPrediction(prediction) )
  * }}}
  *
  * You must extend this trait from an Object so the generics resolve before runtime
  * (at compilation time)
  *
  * @tparam LocalAggregation Type of [[LocalAggregation]]
  * @tparam Prediction       Type of [[Prediction]]
  * @tparam Model            Type of [[Model]]
  */
trait TrainingCoFlatMapFactory[LocalAggregation, Prediction, Model] {

  /**
    * Used to output local aggregations, predictions, and global models from a
    * BroadcastedCoFlatMapIterationOutput function.
    *
    * @param localAggregation Local Aggregation to be broadcasted
    * @param prediction       Optional prediction to propagate downstream.
    * @param globalModel      Optional global centroids to propagate downstream.
    */
  case class Output(localAggregation: Option[LocalAggregation],
                    prediction: Option[Prediction],
                    globalModel: Option[Model])
    extends DefinedThreeTuple[LocalAggregation, Prediction, Model](
      localAggregation, prediction, globalModel)

  /**
    * Helper functions to create an Output class.
    */
  object Output {
    def fromLocalAggregation(localAggregation: LocalAggregation): Output = {
      Output(Some(localAggregation), None, None)
    }

    def fromGlobalModel(globalModel: Model): Output = {
      Output(None, None, Some(globalModel))
    }

    def fromPrediction(prediction: Prediction): Output = {
      Output(None, Some(prediction), None)
    }
  }

  def coFlatMapTrain[Input](inputStream: DataStream[Input],
                            coFlatMap: RichCoFlatMapFunction[Input, LocalAggregation, Output],
                            feedbackFunctionBeforeBroadcast: DataStream[LocalAggregation] => DataStream[LocalAggregation],
                            parameters: ArgumentParameterMap
                           )(
                             implicit inputEvidence: TypeInformation[Input],
                             localAggregationEvidence: TypeInformation[LocalAggregation],
                             predictionEvidence: TypeInformation[Prediction],
                             modelEvidence: TypeInformation[Model],
                             outputEvidence: TypeInformation[Output],
                             finalOutputEvidence: TypeInformation[DefinedTwoTuple[Prediction, Model]]
                           )
  : (DataStream[Prediction], DataStream[Model]) = {

    val extractLocalAggregationAndOutputFunction
    : DataStream[Output] =>
      (DataStream[LocalAggregation], DataStream[DefinedTwoTuple[Prediction, Model]]) = {
      coFlatMapOutput =>
        // Splits on LocalAggregation, tuple together Prediction and Model
        DefinedSplit.threeSplitOnLeft(coFlatMapOutput)(
          localAggregationEvidence, predictionEvidence, modelEvidence)
    }

    val iterationOutput: DataStream[DefinedTwoTuple[Prediction, Model]]
    = BroadcastedCoFlatMapIterationOutput[
      // Generics
      Input,
      LocalAggregation,
      DefinedTwoTuple[Prediction, Model],
      Output
      ](
      // Parameters
      inputStream,
      extractLocalAggregationAndOutputFunction,
      feedbackFunctionBeforeBroadcast,
      coFlatMap = coFlatMap,
      parameters = parameters
    )(
      // Implicit Evidence Parameters
      inputEvidence,
      localAggregationEvidence,
      finalOutputEvidence,
      outputEvidence
    )

    DefinedSplit.twoSplit(iterationOutput)
  }

  def coFlatMapTrain[Input](inputStream: DataStream[Input],
                            coFlatMap: RichCoFlatMapFunction[Input, LocalAggregation, Output],
                            parameters: ArgumentParameterMap
                           )(
                             implicit inputEvidence: TypeInformation[Input],
                             localAggregationEvidence: TypeInformation[LocalAggregation],
                             predictionEvidence: TypeInformation[Prediction],
                             modelEvidence: TypeInformation[Model],
                             outputEvidence: TypeInformation[Output],
                             finalOutputEvidence: TypeInformation[DefinedTwoTuple[Prediction, Model]]
                           )
  : (DataStream[Prediction], DataStream[Model]) = {
    coFlatMapTrain(
      // Parameters
      inputStream,
      coFlatMap,
      (x: DataStream[LocalAggregation]) => x,
      parameters
    )(
      // Implicit Evidence Parameters
      inputEvidence,
      localAggregationEvidence,
      predictionEvidence,
      modelEvidence,
      outputEvidence,
      finalOutputEvidence
    )
  }

  def coFlatMapTrainWithMetrics[Input](inputStream: DataStream[Input],
                                       coFlatMapFunction: RichCoFlatMapFunction[Input, LocalAggregation, PerformanceMetricsOutput[Output]],
                                       feedbackFunctionBeforeBroadcast: DataStream[LocalAggregation] => DataStream[LocalAggregation],
                                       parameters: ArgumentParameterMap
                                      )(
                                        implicit inputEvidence: TypeInformation[Input],
                                        localAggregationEvidence: TypeInformation[LocalAggregation],
                                        predictionEvidence: TypeInformation[Prediction],
                                        modelEvidence: TypeInformation[Model],
                                        outputEvidence: TypeInformation[Output],
                                        performanceEvidence: TypeInformation[PerformanceMetricsOutput[Output]],
                                        performanceHashEvidence: TypeInformation[PerformanceMetricsHash],
                                        finalOutputEvidence: TypeInformation[DefinedThreeTuple[PerformanceMetricsHash, Prediction, Model]]
                                      )
  : (DataStream[PerformanceMetricsHash], DataStream[Prediction], DataStream[Model]) = {

    val extractLocalAggregationAndOutputFunction
    : DataStream[PerformanceMetricsOutput[Output]] => (DataStream[LocalAggregation], DataStream[DefinedThreeTuple[PerformanceMetricsHash, Prediction, Model]]) = {
      coFlatMapOutput => {

        // TODO: Incorporate into RichFlatMapFunction, not here underneath the hood!
        // Outputs metrics containing when models were emitted.
        val coFlatMapOutputModelAck = coFlatMapOutput
          .flatMap((x: PerformanceMetricsOutput[Output],
                    out: Collector[PerformanceMetricsOutput[Output]]) => {
            // If the model is defined
            if (x.output.isDefined && x.output.get.globalModel.isDefined) {
              val modelOutputMetric = PerformanceMetricsHash(mutable.HashMap[StatInfo, Any](
                StatInfo(StatNames.ModelOutputTimestamp, StatPolicy.MAX) -> System.currentTimeMillis()
              ))
              // Output metric when it was emitted
              out.collect(new PerformanceMetricsOutput[Output](Some(modelOutputMetric), None))
            }
            // Always collect the original input, which contains all data from coFlatMap function
            out.collect(x)
          })

        // Split on PerformanceMetricsHash and Output
        val performanceOutputSplit = DefinedSplit
          .twoSplit(coFlatMapOutputModelAck)(performanceHashEvidence, outputEvidence)
        val performanceMetricsStream = performanceOutputSplit._1
        val coFlatMapOutputStream = performanceOutputSplit._2

        // Split on LocalAggregation, Prediction, and Model
        val localAggregationOutputSplit = DefinedSplit
          .threeSplit(coFlatMapOutputStream)(localAggregationEvidence, predictionEvidence, modelEvidence)
        val localAggregationStream = localAggregationOutputSplit._1
        val predictionStream = localAggregationOutputSplit._2
        val modelStream = localAggregationOutputSplit._3

        // Construct output stream of PerformanceMetricsHash, Prediction, and Model
        val outputStream = DefinedJoin
          .threeJoin(performanceMetricsStream, predictionStream, modelStream)

        (localAggregationStream, outputStream)
      }
    }

    val iterationOutput: DataStream[DefinedThreeTuple[PerformanceMetricsHash, Prediction, Model]]
    = BroadcastedCoFlatMapIterationOutput[
      // Generics
      Input,
      LocalAggregation,
      DefinedThreeTuple[PerformanceMetricsHash, Prediction, Model],
      PerformanceMetricsOutput[Output]
      ](
      // Parameters
      inputStream,
      extractLocalAggregationAndOutputFunction,
      feedbackFunctionBeforeBroadcast,
      coFlatMapFunction,
      parameters
    )(
      // Implicit Evidence Parameters
      inputEvidence,
      localAggregationEvidence,
      finalOutputEvidence,
      performanceEvidence
    )

    DefinedSplit.threeSplit(iterationOutput)
  }

  def coFlatMapTrainWithMetrics[Input](inputStream: DataStream[Input],
                                       coFlatMapFunction: RichCoFlatMapFunction[Input, LocalAggregation, PerformanceMetricsOutput[Output]],
                                       parameters: ArgumentParameterMap
                                      )(
                                        implicit inputEvidence: TypeInformation[Input],
                                        localAggregationEvidence: TypeInformation[LocalAggregation],
                                        predictionEvidence: TypeInformation[Prediction],
                                        modelEvidence: TypeInformation[Model],
                                        outputEvidence: TypeInformation[Output],
                                        performanceEvidence: TypeInformation[PerformanceMetricsOutput[Output]],
                                        performanceHashEvidence: TypeInformation[PerformanceMetricsHash],
                                        finalOutputEvidence: TypeInformation[DefinedThreeTuple[PerformanceMetricsHash, Prediction, Model]]
                                      )
  : (DataStream[PerformanceMetricsHash], DataStream[Prediction], DataStream[Model]) = {
    coFlatMapTrainWithMetrics(
      // Parameters
      inputStream,
      coFlatMapFunction,
      (x: DataStream[LocalAggregation]) => x,
      parameters
    )(
      // Implicit Evidence Parameters
      inputEvidence,
      localAggregationEvidence,
      predictionEvidence,
      modelEvidence,
      outputEvidence,
      performanceEvidence,
      performanceHashEvidence,
      finalOutputEvidence
    )
  }
}
