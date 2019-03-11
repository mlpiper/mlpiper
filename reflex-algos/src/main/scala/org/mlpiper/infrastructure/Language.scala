package org.mlpiper.infrastructure

object Language extends Enumeration {
  type Language = Value
  val Python = Value("Python")
  val Jupyter = Value("Jupyter")
  val R = Value("R")
  val Java = Value("Java")
  val Scala = Value("Scala")
}
