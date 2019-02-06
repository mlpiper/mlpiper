package com.parallelmachines.reflex.pipeline

import org.apache.commons.codec.binary.{Base64 => ApacheBase64}

object Base64Wrapper {
    def decode(encoded: String) = new String(ApacheBase64.decodeBase64(encoded.getBytes))
    def encode(decoded: String) = new String(ApacheBase64.encodeBase64(decoded.getBytes))
}
