package com.parallelmachines.reflex.pipeline


trait DataWrapperBase {

  def data[B](): B
}


case class DataWrapper[+A](e: A)(implicit mf: Manifest[A]) extends DataWrapperBase {

  override def toString: String = s"DataWrapper$mf"

  override def data[B](): B = e.asInstanceOf[B]
}
