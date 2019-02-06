package com.parallelmachines.reflex.pipeline

import scala.reflect.runtime.universe._
import scala.collection.mutable

// Note: Extending the mutable list and not the imutable since extending the imutable is much more complex
class ConnectionList(conn: ComponentConnection[_]*) extends mutable.MutableList[ComponentConnection[_]] {
  this ++= conn

  def validate(): Unit = {
    val labels = this.map(_.label)
    if (labels.size != labels.distinct.size) {
      val duplicates = labels.diff(labels.distinct).distinct
      throw new Exception(s"ConnectionList contains connections with the same labels '${duplicates.mkString(",")}'")
    }
  }

  def add(conn: ComponentConnection[_]) : ConnectionList = {
    this += conn
    this
  }
}


object ConnectionList {
  //type ConnectionList = List[ComponentConnection[_]]

  // Note: the typeTag implicit argument was necessary to prevent the 2 apply methods to have the same
  //       signature after type erasure.
  def apply[T](xs: TypeTag[T]*)(implicit tag: TypeTag[T]): ConnectionList = {
    val connIter = xs.map { x => ComponentConnection[T](x) }
    ConnectionList(connIter: _*)
  }

  def apply(xs: ComponentConnection[_]*): ConnectionList = {
    new ConnectionList(xs: _*)
  }

  def apply(): ConnectionList = {
    new ConnectionList()
  }

  def empty(): ConnectionList = {
    new ConnectionList()
  }
}
