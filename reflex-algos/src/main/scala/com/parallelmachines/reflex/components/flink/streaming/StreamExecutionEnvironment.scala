package com.parallelmachines.reflex.components.flink.streaming

object StreamExecutionEnvironment {
  def apply(): StreamExecutionEnvironment = {
    new StreamExecutionEnvironment()
  }
}
class StreamExecutionEnvironment {
  val dummy:String = ""
  def getExecutionEnvironment(): String = {
    "dummy"
  }
  def setParallelism(p: Int): Unit = {}
  def execute(pipe: String): String = {
    return "dummy"
  }
}
