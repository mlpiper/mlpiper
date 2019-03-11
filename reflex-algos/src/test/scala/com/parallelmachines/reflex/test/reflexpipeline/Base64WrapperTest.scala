package com.parallelmachines.reflex.test.reflexpipeline

import org.junit.runner.RunWith
import org.mlpiper.infrastructure.Base64Wrapper
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class Base64WrapperTest extends FlatSpec with Matchers {

  "Basic encode decode" should "be valid" in {
    val s = "Hellow world"
    val encoded = Base64Wrapper.encode(s)
    val decoded = Base64Wrapper.decode(encoded)
    assert(decoded == s, s"original string [$s] != decoded [$decoded]")
  }

  "With quotes encode decode" should "be valid" in {
    val s = """Hellow world " " ' """
    val encoded = Base64Wrapper.encode(s)
    val decoded = Base64Wrapper.decode(encoded)
    assert(decoded == s, s"original string [$s] != decoded [$decoded]")
  }
}
