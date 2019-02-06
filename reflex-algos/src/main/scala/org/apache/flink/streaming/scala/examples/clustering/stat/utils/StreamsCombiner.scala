package org.apache.flink.streaming.scala.examples.clustering.stat.utils

import org.apache.flink.api.common.functions.RichJoinFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger

/**
  * Class is responsible for Performing a [[RichJoinFunction]] on [[IDElement]]s and outputs [[Tuple2]] of [[IN1]] & [[IN2]].
  * Primary task of the class is to provide join function.
  * join will remove elementID and returns tuple of relevant data.
  */
class StreamsCombiner[IN1, IN2]
  extends RichJoinFunction[IDElement[IN1],
    IDElement[IN2],
    (IN1, IN2)] {

  override def join(first: IDElement[IN1], second: IDElement[IN2]): (IN1, IN2) = {
    (first.element, second.element)
  }
}

object StreamsCombiner {
  /**
    * combines two streams based on assigned element ID
    * function works in following way.
    *
    * @param idStreamX First DataStream
    * @param idStreamY Second DataStream
    * @return combined Stream
    */
  def joinTwoStreams[T](
                         idStreamX: DataStream[IDElement[T]],
                         idStreamY: DataStream[IDElement[T]]
                       )(
                         implicit typeEvidence: TypeInformation[T]
                       )
  : DataStream[(T, T)] = {

    val combinedStreams = idStreamX
      .join(idStreamY)
      .where(_.elementID)
      .equalTo(_.elementID)
      .window(GlobalWindows.create())
      .trigger(CountTrigger.of(1))
      .apply(new StreamsCombiner[T, T])

    combinedStreams
  }
}