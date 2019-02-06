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
package org.apache.flink.streaming.scala.examples.common.algorithm

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.scala.examples.common.parameters.pipeline.Checkpointing
import org.apache.flink.streaming.scala.examples.common.performance.PerformanceMetricsHash

/**
  * [[OnlineInstances]] encapsulates [[OnlineInstances.OnlineInstance]] related properties
  * in order to build upon without exposing its internals.
  */
object OnlineInstances {

  private[OnlineInstances] val preparedErrorMsg: String =
    "This instance has already been prepared for execution. "

  private[OnlineInstances] val notPreparedErrorMsg: String =
    "This instance must be prepared for execution first. "

  sealed trait OnlineInstance[Self, IN] {

    that: Self =>

    /** Input DataStream to perform computation on. */
    private[OnlineInstances] var inputStream: DataStream[IN] = _

    /**
      * Boolean determining whether your Flink job has been assembled by
      * calling [[prepareExecution]].
      */
    private[OnlineInstances] var preparedExecution: Boolean = false

    /**
      * Sets the input DataStream.
      * @param stream Input DataStream to perform computation on.
      * @return
      */
    def setInputDataStream(stream: DataStream[IN]): Self = {
      requirePrePreparedExecution("Must set the DataStream before preparingExecution.")
      this.inputStream = stream
      that
    }

    /**
      * Helper function to require it be called before [[prepareExecution]] is called.
      * @param errorMessage Error message describing why this must be called
      *                     before [[prepareExecution]].
      * @throws IllegalArgumentException
      */
    @throws[IllegalArgumentException]
    final def requirePrePreparedExecution(errorMessage: String): Unit = {
      require(!preparedExecution, preparedErrorMsg + errorMessage)
    }

    /**
      * Helper function to require it be called after [[prepareExecution()]] is called.
      * @param errorMessage Error message describing why this must be called
      *                     after [[prepareExecution]].
      * @throws IllegalArgumentException
      */
    @throws[IllegalArgumentException]
    final def requirePostPreparedExecution(errorMessage: String): Unit = {
      require(preparedExecution, notPreparedErrorMsg + errorMessage)
    }

    /**
      * Assembles the Flink job using the provided [[StreamExecutionEnvironment]].
      * Will call [[checkExecutionConditions]] before preparing execution.
      * @param env [[StreamExecutionEnvironment]] the job will run in.
      */
    final def prepareExecution(env: StreamExecutionEnvironment): Unit = {
      requirePrePreparedExecution("Can not call prepareExecution more than once.")
      checkExecutionConditions()
      compileJob(env)
      preparedExecution = true
    }

    /**
      * Contains all the logic for the Flink job using the provided [[StreamExecutionEnvironment]].
      * Will be called in [[prepareExecution]].
      * @param env [[StreamExecutionEnvironment]] the job will run in.
      */
    private[OnlineInstances] def compileJob(env: StreamExecutionEnvironment): Unit

    /**
      * Called within [[prepareExecution]] before the Flink job is assembled. Checks any
      * conditions the OnlineInstance must meet.
      * @throws IllegalArgumentException
      */
    @throws[IllegalArgumentException]
    protected def checkExecutionConditions(): Unit = {
      require(inputStream != null, "DataStream was not provided")
    }

  }


  sealed trait OnlinePredictionInstance[Self, Input, Prediction]
    extends OnlineInstance[Self, Input] {
    that: Self =>

    /** Output DataStream of inferred [[Prediction]]s. */
    private[OnlineInstances] var predictionStream: DataStream[Prediction] = _

    /**
      * @return [[Prediction]] DataStream. Must be called after [[prepareExecution]] is called.
      */
    final def getPredictionStream: DataStream[Prediction] = {
      this.requirePostPreparedExecution(
        "Must access the prediction stream after preparing execution.")
      predictionStream
    }
  }

  sealed trait OnlineInstanceWithMetrics[Self, Input]
    extends OnlineInstance[Self, Input] {
    that: Self =>

    /** Metrics stream */
    private[OnlineInstances] var metricsStream: DataStream[PerformanceMetricsHash] = _

    /**
      * @return [[PerformanceMetricsHash]] DataStream.
      *          Must be called after [[prepareExecution]] is called.
      */
    final def getMetricsStream: DataStream[PerformanceMetricsHash] = {
      this.requirePostPreparedExecution(
        "Must access the metrics stream after preparing execution.")
      metricsStream
    }
  }

  trait OnlineInstanceWithModelInput[Self, Input, Model]
    extends OnlineInstance[Self, Input] {

    that: Self =>

    private[OnlineInstances] var modelInputStream: DataStream[Model] = _

    def setModelInputStream(stream: DataStream[Model]): Self = {
      requirePrePreparedExecution("Must set the ModelStream before preparingExecution")
      this.modelInputStream = stream
      that
    }

    @throws[IllegalArgumentException]
    override protected def checkExecutionConditions(): Unit = {
      super.checkExecutionConditions()
      require(modelInputStream != null, "ModelInputStream was not provided")
    }

  }

  trait OnlineInferenceInstance[Self, Input, Prediction, Model]
    extends OnlinePredictionInstance[Self, Input, Prediction]
    with Checkpointing[Self] {

    that: Self =>

    /**
      * Infers the Input DataStream of [[Input]] and must output a DataStream of [[Prediction]].
      *
      * @param env StreamExecutionEnvironment the Flink Job is assembled on.
      * @param inputStream Input DataStream of [[Input]] to infer.
      * @return Inferred output DataStream of [[Prediction]].
      */
    protected def inference(env: StreamExecutionEnvironment,
                            inputStream: DataStream[Input]): DataStream[Prediction]

    /**
      * Sets the [[predictionStream]] to the output of [[inference]].
      * @param env [[StreamExecutionEnvironment]] the job will run in.
      */
    override private[OnlineInstances] def compileJob(env: StreamExecutionEnvironment)
    : Unit = {
      this.predictionStream = inference(env, this.inputStream)
    }

  }

  trait OnlineInferenceInstanceWithMetrics[Self, Input, Prediction, Model]
    extends OnlinePredictionInstance[Self, Input, Prediction]
      with OnlineInstanceWithMetrics[Self, Input]
      with Checkpointing[Self] {

    that: Self =>

    /**
      * Infers the Input DataStream of [[Input]] and must output a DataStream of [[Prediction]]
      * and a DataStream of [[PerformanceMetricsHash]].
      *
      * @param env StreamExecutionEnvironment the Flink Job is assembled on.
      * @param inputStream Input DataStream of [[Input]] to infer.
      * @return Inferred output DataStream of [[Prediction]] and
      *         a DataStream of [[PerformanceMetricsHash]]
      */
    protected def inference(env: StreamExecutionEnvironment,
                            inputStream: DataStream[Input])
    : (DataStream[Prediction], DataStream[PerformanceMetricsHash])


    /**
      * Sets the [[predictionStream]] and [[metricsStream]] to the output of [[inference]].
      * @param env [[StreamExecutionEnvironment]] the job will run in.
      */
    override private[OnlineInstances] def compileJob(env: StreamExecutionEnvironment)
    : Unit = {
      val output = inference(env, this.inputStream)
      this.predictionStream = output._1
      this.metricsStream = output._2
    }
  }

  trait OnlineInferenceInstanceWithMetricsAndModelInput[Self, Input, Prediction, Model]
    extends OnlinePredictionInstance[Self, Input, Prediction]
      with OnlineInstanceWithMetrics[Self, Input]
      with OnlineInstanceWithModelInput[Self, Input, Model]
      with Checkpointing[Self] {
    that: Self =>

    protected def inference(env: StreamExecutionEnvironment,
                            inputStream: DataStream[Input],
                            modelStream: DataStream[Model])
    : (DataStream[Prediction], DataStream[PerformanceMetricsHash])

    /**
      * Sets the [[predictionStream]] and [[metricsStream]] to the output of [[inference]].
      * @param env [[StreamExecutionEnvironment]] the job will run in.
      */
    override private[OnlineInstances] def compileJob(env: StreamExecutionEnvironment)
    : Unit = {
      val output =
        inference(
          env,
          this.inputStream,
          this.modelInputStream.broadcast)

      this.predictionStream = output._1
      this.metricsStream = output._2
    }
  }

  sealed trait OnlineInstanceWithModelOutput[Self, Input, Model]
    extends OnlineInstance[Self, Input] {
    that: Self =>

    /** Output DataStream of updating [[Model]]s. */
    private[OnlineInstances] var modelOutputStream: DataStream[Model] = _

    /**
      * @return [[Model]] DataStream. Must be called after [[prepareExecution]] is called.
      */
    final def getModelOutputStream: DataStream[Model] = {
      this.requirePostPreparedExecution(
        "Must access the model stream after preparing execution.")
      modelOutputStream
    }

  }

  trait OnlineTrainingInstance[Self, Input, Prediction, Model]
    extends OnlinePredictionInstance[Self, Input, Prediction]
      with OnlineInstanceWithModelOutput[Self, Input, Model] {

    that: Self =>

    /**
      * Trains the Input DataStream of [[Input]] and outputs a DataStream of [[Prediction]] and a
      * DataStream of [[Model]].
      *
      * @param env StreamExecutionEnvironment the Flink Job is assembled on.
      * @param inputStream Input DataStream of [[Input]] to train with.
      * @return Inferred output DataStream of [[Prediction]] and updated DataStream of [[Model]].
      */
    protected def train(env: StreamExecutionEnvironment,
                        inputStream: DataStream[Input])
    : (DataStream[Prediction], DataStream[Model])


    /**
      * Sets the [[predictionStream]] and [[modelOutputStream]] to the output of [[train]].
      * @param env [[StreamExecutionEnvironment]] the job will run in.
      */
    final override private[OnlineInstances] def compileJob(env: StreamExecutionEnvironment)
    : Unit = {
      val trainingOutput = train(env, this.inputStream)
      this.predictionStream = trainingOutput._1
      this.modelOutputStream = trainingOutput._2
    }
  }

  trait OnlineTrainingInstanceWithMetrics[Self, Input, Prediction, Model]
    extends OnlinePredictionInstance[Self, Input, Prediction]
      with OnlineInstanceWithModelOutput[Self, Input, Model]
      with OnlineInstanceWithMetrics[Self, Input] {

    that: Self =>

    /**
      * Trains the Input DataStream of [[Input]] and outputs a DataStream of [[Prediction]] and a
      * DataStream of [[Model]].
      *
      * @param env StreamExecutionEnvironment the Flink Job is assembled on.
      * @param inputStream Input DataStream of [[Input]] to train with.
      * @return Inferred output DataStream of [[Prediction]] and updated DataStream of [[Model]].
      */
    protected def train(env: StreamExecutionEnvironment,
                        inputStream: DataStream[Input])
    : (DataStream[PerformanceMetricsHash], DataStream[Prediction], DataStream[Model])


    /**
      * Sets the [[predictionStream]] and [[modelOutputStream]] to the output of [[train]].
      * @param env [[StreamExecutionEnvironment]] the job will run in.
      */
    final override private[OnlineInstances] def compileJob(env: StreamExecutionEnvironment)
    : Unit = {
      val trainingOutput = train(env, this.inputStream)
      this.metricsStream = trainingOutput._1
      this.predictionStream = trainingOutput._2
      this.modelOutputStream = trainingOutput._3
    }
  }

}
