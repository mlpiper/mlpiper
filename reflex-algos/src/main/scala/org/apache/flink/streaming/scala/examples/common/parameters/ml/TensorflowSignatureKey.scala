package org.apache.flink.streaming.scala.examples.common.parameters.ml

import org.apache.flink.streaming.scala.examples.common.parameters.common.StringParameter
import org.apache.flink.streaming.scala.examples.common.parameters.tools.{ArgumentParameterChecker, WithArgumentParameters}

case object TensorflowSignatureKey extends StringParameter {
  override val key: String = "tfSigKey"
  override val label: String = "TF Sig Key"
  override val defaultValue: Option[String] = None
  override val required: Boolean = true
  override val description: String = "Name of signature used in the TF SavedModel"
  override val errorMessage: String = "Tensorflow Signature Key must be specified with " + key

  override def condition(value: Option[String], parameters: ArgumentParameterChecker): Boolean = {
    value.isDefined
  }
}

trait TensorflowSignatureKey[Self] extends WithArgumentParameters {

  that: Self =>

  def setTensorflowSignatureKey(signatureKey: String): Self = {
    this.parameters.add(TensorflowSignatureKey, signatureKey)
    that
  }
}
