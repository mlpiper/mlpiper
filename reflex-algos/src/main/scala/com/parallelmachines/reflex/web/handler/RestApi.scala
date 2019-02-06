package com.parallelmachines.reflex.web.handler


object RestApi {
  val RootPath = "/api/v1"

  val StatsPath = RootPath + "/stats"
  val SystemStats = StatsPath + "/system"
  val AccumStats = StatsPath + "/accumulators"
  val Command = RootPath + "/command"
}

object RestAction {
  val Command = "cmd"
}

object RestCommands {
  val Exit = "exit"
  val Run = "run"
}
