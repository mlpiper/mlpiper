package org.mlpiper.infrastructure

object SystemEnv {
  val runningFromIntelliJ = _runningFromIntelliJ()

  private def _runningFromIntelliJ(): Boolean = {
    val classPath = System.getProperty("java.class.path")
    return classPath.contains("idea_rt.jar")
  }
}
