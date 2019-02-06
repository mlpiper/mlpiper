package com.parallelmachines.reflex.test.reflexpipeline

import java.io._
import java.net._

import com.parallelmachines.reflex.factory.ReflexComponentFactory
import com.parallelmachines.reflex.pipeline._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable
import scala.io._

class SocketServerSource(inputItemsArg: List[String]) {
  val server: java.net.ServerSocket = new ServerSocket(0)
  val port: Int = server.getLocalPort
  val inputItems: List[String] = inputItemsArg


  def run: Unit = {
    val sock = server.accept()
    val out = new PrintStream(sock.getOutputStream)

    for (item <- inputItems) {
      out.println(item)
    }
    sock.close()
  }
}

class SocketServerSink {
  val server: java.net.ServerSocket = new ServerSocket(0)
  val port: Int = server.getLocalPort
  val outputList: mutable.MutableList[String] = new mutable.MutableList[String]

  def run: Unit = {
    val sock = server.accept()
    val in = new BufferedSource(sock.getInputStream).getLines()

    while (in.hasNext) {
      outputList += in.next()
    }
    sock.close()
  }
}

@RunWith(classOf[JUnitRunner])
class ReflexPipelineITCase extends FlatSpec with Matchers {

  "Empty DagGen args" should "throw an exception" in {
    val args = Array[String]()
    intercept[Exception] {
      DagGen.main(args)
    }
  }

  "Components json generation" should "be valid end to end" in {
    val componentsFile = java.io.File.createTempFile("components", ".json")
    componentsFile.deleteOnExit()

    val componentsDir = DagTestUtil.getComponentsDir()

    val args = Array[String]("--comp-desc", s"${componentsFile.getAbsolutePath}", "--external-comp", s"$componentsDir")
    noException should be thrownBy DagGen.main(args)
    assert(componentsFile.length() != 0)
  }
}
