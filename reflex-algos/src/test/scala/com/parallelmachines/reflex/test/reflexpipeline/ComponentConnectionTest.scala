package com.parallelmachines.reflex.test.reflexpipeline

import com.parallelmachines.reflex.pipeline.ComponentConnection
import org.apache.flink.streaming.api.scala.DataStream
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import scala.reflect.runtime.universe._

@RunWith(classOf[JUnitRunner])
class ComponentConnectionTest extends FlatSpec {

  class AA(val aa: Int) {
  }

  class BB(val bb: Int) extends AA(bb) {
  }

  class CC(val cc: Int) {
  }

  "Basic connection" should "be valid" in {
    val d1 = ComponentConnection(typeTag[Double])
    val d2 = ComponentConnection(typeTag[Double])
    assert(d1.canConnectTo(d2), "Failed connecting Double to Double")

    val s1 = ComponentConnection(typeTag[String])
    assert(!d1.canConnectTo(s1), "Connection Double to String should fail")
  }

  "Collection with same inner type" should "be valid" in {
    val d1 = ComponentConnection(typeTag[List[Double]])
    val d2 = ComponentConnection(typeTag[List[Double]])
    assert(d1.canConnectTo(d2), "Connection List[Double] to List[Double] should work")
  }

  "Basic connection with inherited type" should "be valid" in {
    val a1 = ComponentConnection(typeTag[AA])
    val b1 = ComponentConnection(typeTag[BB])
    assert(b1.canConnectTo(a1), "Failed to connect B to A - even when B is subclass of A")
    assert(!a1.canConnectTo(b1), "Connection A -> B should fail")
  }

  "Collection with inherited inner type" should "be valid" in {
    val a1 = ComponentConnection(typeTag[List[AA]])
    val b1 = ComponentConnection(typeTag[List[BB]])
    assert(b1.canConnectTo(a1), "Failed to connect List[B] to List[A] - even when B is subclass of A")
    assert(!a1.canConnectTo(b1), "Connection List[A] -> List[B] should fail")
  }

  "DataStream collection with inherited inner type" should "be valid" in {
    val a1 = ComponentConnection(typeTag[DataStream[AA]])
    val b1 = ComponentConnection(typeTag[DataStream[BB]])
    // Currently this test is failing due to the DataStream no being able to get T+
    // This test is here to detect a change in this
    assert(!b1.canConnectTo(a1), "Was able to do DataStream[B] to DataStream[A] - when B is subclass of A - DataStream changed implementation")
    assert(!a1.canConnectTo(b1), "Connection DataStream[A] -> DataStream[B] should fail")
  }

  "Basic connection of 2 different types" should "not be possible" in {
    val a1 = ComponentConnection(typeTag[AA])
    val c1 = ComponentConnection(typeTag[CC])

    assert(!a1.canConnectTo(c1), "AA can not connect to CC")
    assert(!c1.canConnectTo(a1), "CC can not connect to AA")

  }

  "Basic connection of 2 different basic types" should "not be possible" in {
    val a1 = ComponentConnection(typeTag[Int])
    val c1 = ComponentConnection(typeTag[String])

    assert(!a1.canConnectTo(c1), "Int can not connect to String")
    assert(!c1.canConnectTo(a1), "String can not connect to Int")

  }

}
