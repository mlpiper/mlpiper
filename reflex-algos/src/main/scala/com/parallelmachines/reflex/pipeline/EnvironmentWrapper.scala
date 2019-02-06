package com.parallelmachines.reflex.pipeline


trait EnvWrapperBase {

  def env[B](): B
}

case class EnvironmentWrapper[A](e: A)(implicit mf: Manifest[A]) extends EnvWrapperBase {

  override def toString: String = s"EnvWrapper$mf"

  override def env[B](): B = e.asInstanceOf[B]
}
