package org.apache.flink.streaming.scala.examples.flink.utils

import org.apache.flink.streaming.api.scala.function.RichAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

/**
  * Wrapper class to hold tuple of ID and an element.
  *
  * @param elementID ID of element.
  * @param element   Element.
  * @tparam T Element type.
  */
case class IDElement[T](elementID: Long, element: T)

/**
  * Apply function which prepends an element with its ID (count of element in this case).
  *
  * @tparam IN Input type.
  */
class PrependElementID[IN]
  extends RichAllWindowFunction[IN, IDElement[IN], GlobalWindow] {

  var elementID: Long = 1

  override def apply(window: GlobalWindow, input: Iterable[IN], out: Collector[IDElement[IN]]): Unit = {

    input.foreach(x =>
      out.collect(IDElement(
        elementID = elementID,
        element = x)))

    elementID += 1

    if (elementID == Long.MaxValue) {
      elementID = 1
    }
  }
}
