package com.parallelmachines.reflex.test.common

import org.scalatest.{FlatSpec, Matchers}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JsonPairRemoveTest extends FlatSpec with Matchers {
  var testJSON =s"""{"field1":"value1","field2":6,"field3":TOKEN}""".stripMargin

  it should "Correctly test RemoveTokenPair()" in {
    TestHelpers.removeTokenPair(testJSON, "field1") == s"""{"field2":6,"field3":TOKEN}""" should be(true)
    TestHelpers.removeTokenPair(testJSON, "TOKEN") == s"""{"field1":"value1","field2":6}""" should be(true)
    TestHelpers.removeTokenPair(testJSON, "field2") == s"""{"field1":"value1","field3":TOKEN}""" should be(true)
    TestHelpers.removeTokenPair(testJSON, "NOT_EXISTING_TOKEN") == testJSON should be(true)
  }
}