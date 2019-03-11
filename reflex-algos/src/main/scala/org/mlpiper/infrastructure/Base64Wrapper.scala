package org.mlpiper.infrastructure

import org.apache.commons.codec.binary.Base64

object Base64Wrapper {
  def decode(encoded: String) = new String(Base64.decodeBase64(encoded.getBytes))

  def encode(decoded: String) = new String(Base64.encodeBase64(decoded.getBytes))
}
