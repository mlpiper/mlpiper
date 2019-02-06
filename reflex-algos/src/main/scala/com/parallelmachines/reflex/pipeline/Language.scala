package com.parallelmachines.reflex.pipeline

object Language extends Enumeration {
  type Language = Value
  val Python = Value("Python")
  val R = Value("R")
  val Java = Value("Java")
  val Scala = Value("Scala")
}
