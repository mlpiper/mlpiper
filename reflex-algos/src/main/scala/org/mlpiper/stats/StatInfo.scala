package org.mlpiper.stats

import com.parallelmachines.reflex.common.InfoType.InfoType


object StatTable extends Enumeration with Serializable {
  val PIPELINE = Value("pipelinestat")
  val HEALTH = Value("health")
  val SYSTEM = Value("sysstat")
  val DATA_HEATMAP = Value("dataheatmap")
  val MODEL_STATS = Value("modelstats")
}

object StatInfo {

  /**
    * Creates a StatInfo with table default to PIPELINE, prepending the name.
    * Local policy defaults to REPLACE.
    *
    * @param name Name of the Stat Accumulator, used for keying in Flink/Spark contexts.
    * @param globalPolicy Global policy used for merging elements from every Stat Accumulator.
    * @return New StatInfo with specified name, global policy, and REPLACE local policy.
    */
  def apply(name: String,
            globalPolicy: StatPolicy.Value)
  : StatInfo = {
    new StatInfo(s"${StatTable.PIPELINE.toString}.$name", StatPolicy.REPLACE, globalPolicy)
  }

  def apply(name: String) : StatInfo = {
    new StatInfo(name, StatPolicy.REPLACE, StatPolicy.REPLACE)
  }


  /**
    * Creates a StatInfo using a StatName
    * Local policy defaults to REPLACE.
    */
  def apply(statName: StatNames.StatName,
            globalPolicy: StatPolicy.Value)
  : StatInfo = {
    new StatInfo(statName.toString, StatPolicy.REPLACE, globalPolicy)
  }

  /**
    * Creates a StatInfo with its name formatted as "table.key"
    */
  def apply(key: String,
            statTable: StatTable.Value,
            localPolicy: StatPolicy.Value,
            globalPolicy: StatPolicy.Value)
  : StatInfo = {
    new StatInfo(s"${statTable.toString}.$key", localPolicy, globalPolicy)
  }

  /**
    * Creates a StatInfo with its name set as the table name
    */
  def apply(statTable: StatTable.Value,
            localPolicy: StatPolicy.Value,
            globalPolicy: StatPolicy.Value)
  : StatInfo = {
    new StatInfo(statTable.toString, localPolicy, globalPolicy)
  }

  /**
    * Creates a StatInfo using a StatName
    */
  def apply(statName: StatNames.StatName,
            localPolicy: StatPolicy.Value,
            globalPolicy: StatPolicy.Value)
  : StatInfo = {
    new StatInfo(statName.toString, localPolicy, globalPolicy)
  }
}

/**
  * Class used to describe a Stat Accumulator
  * @param name Name of the Stat, used for keying in Flink/Spark contexts.
  * @param localPolicy Local policy used for subsequent local elements in a Stat Accumulator.
  * @param globalPolicy Global policy used for merging elements from every Stat Accumulator.
  */
case class StatInfo(name: String,
                    localPolicy: StatPolicy.Value,
                    globalPolicy: StatPolicy.Value)
  extends Serializable {
  def getLocalMerge[R](value: R): MergeableStat[R] = StatPolicy(localPolicy, value)
  def getGlobalMerge[R](value: R): MergeableStat[R] = StatPolicy(globalPolicy, value)

  def toGlobalStat[R](value: R,
                      accumDataType: AccumData.GraphType,
                      accumModeType: AccumMode.AccumModeType = AccumMode.TimeSeries,
                      infoType: InfoType,
                      modelId: String = null, // TODO: modelId should not have default value here
                      timestamp_ns: String = null): GlobalAccumulator[R] = {
    new GlobalAccumulator[R](
      name,
      getLocalMerge(value),
      getGlobalMerge(value),
      value,
      accumDataType = accumDataType,
      accumModeType = accumModeType,
      infoType = infoType,
      modelId = modelId,
      timestamp_ns = timestamp_ns)
  }
}

