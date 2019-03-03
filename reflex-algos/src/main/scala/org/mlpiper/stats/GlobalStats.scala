
package org.mlpiper.stats

import com.parallelmachines.reflex.common.InfoType.InfoType
import org.apache.spark.SparkContext
import org.apache.spark.util.AccumulatorV2

/**
  * Homogenized accumulator used to control Flink or Spark accumulators.
  *
  * @param name          Name of the accumulator to display in the Web-UI
  * @param localMerge    Function to handle adding new elements locally (subtask/partition)
  * @param globalMerge   Function to handle globally merging (reducing) every local accumulator value
  * @param startingValue Starting value of the accumulator
  * @param accumDataType Datatype of value or whatever inside value is - SCALAR, TEXT, etc.
  * @param accumModeType Category of accumulator - TIME_SERIES, CONFIG
  * @tparam R Accumulator element type
  */
class GlobalAccumulator[R](name: String,
                           localMerge: MergeableStat[R],
                           globalMerge: MergeableStat[R],
                           startingValue: R,
                           accumDataType: AccumData.GraphType,
                           accumModeType: AccumMode.AccumModeType,
                           infoType: InfoType,
                           modelId: String = null,
                           timestamp_ns: String = null)
  extends Serializable {


  def this(name: String,
           localMerge: (AccumulatorInfo[R], AccumulatorInfo[R]) => AccumulatorInfo[R],
           globalMerge: (AccumulatorInfo[R], AccumulatorInfo[R]) => AccumulatorInfo[R],
           startingValue: R,
           accumDataType: AccumData.GraphType,
           accumModeType: AccumMode.AccumModeType,
           infoType: InfoType,
           modelId: String) = {
    this(
      name,
      new MergeableStat[R] {
        override def statMerge(local: AccumulatorInfo[R],
                               other: AccumulatorInfo[R])
        : AccumulatorInfo[R] = localMerge(local, other)
      },
      new MergeableStat[R] {
        override def statMerge(local: AccumulatorInfo[R],
                               other: AccumulatorInfo[R])
        : AccumulatorInfo[R] = globalMerge(local, other)
      },
      startingValue,
      accumDataType = accumDataType,
      accumModeType = accumModeType,
      infoType = infoType,
      modelId = modelId)
  }


  /**
    * Creates GlobalAccumulator using the provided lambda functions
    */
  def this(name: String,
           localMerge: (AccumulatorInfo[R], AccumulatorInfo[R]) => AccumulatorInfo[R],
           globalMerge: (AccumulatorInfo[R], AccumulatorInfo[R]) => AccumulatorInfo[R],
           startingValue: R,
           accumDataType: AccumData.GraphType,
           accumModeType: AccumMode.AccumModeType,
           infoType: InfoType) = {
    this(
      name,
      new MergeableStat[R] {
        override def statMerge(local: AccumulatorInfo[R],
                               other: AccumulatorInfo[R])
        : AccumulatorInfo[R] = localMerge(local, other)
      },
      new MergeableStat[R] {
        override def statMerge(local: AccumulatorInfo[R],
                               other: AccumulatorInfo[R])
        : AccumulatorInfo[R] = globalMerge(local, other)
      },
      startingValue,
      accumDataType = accumDataType,
      accumModeType = accumModeType,
      infoType = infoType)
  }


  def this(statName: StatNames.StatName,
           localMerge: (AccumulatorInfo[R], AccumulatorInfo[R]) => AccumulatorInfo[R],
           globalMerge: (AccumulatorInfo[R], AccumulatorInfo[R]) => AccumulatorInfo[R],
           startingValue: R,
           accumDataType: AccumData.GraphType,
           accumModeType: AccumMode.AccumModeType,
           infoType: InfoType) = {
    this(
      statName.toString,
      new MergeableStat[R] {
        override def statMerge(local: AccumulatorInfo[R],
                               other: AccumulatorInfo[R])
        : AccumulatorInfo[R] = localMerge(local, other)
      },
      new MergeableStat[R] {
        override def statMerge(local: AccumulatorInfo[R],
                               other: AccumulatorInfo[R])
        : AccumulatorInfo[R] = globalMerge(local, other)
      },
      startingValue,
      accumDataType = accumDataType,
      accumModeType = accumModeType,
      infoType = infoType)
  }

  /* This accumulator is not used, but keep it for a while
  /**
    * Creates GlobalAccumulator using the provided lambda functions and its name formatted as
    * "table.key"
    */
  def this(table: StatTable.Value,
           key: String,
           localMerge: (AccumulatorInfo[R], AccumulatorInfo[R]) => AccumulatorInfo[R],
           globalMerge: (AccumulatorInfo[R], AccumulatorInfo[R]) => AccumulatorInfo[R],
           startingValue: R,
           accumDataType: AccumData.GraphType,
           accumModeType: AccumMode.AccumModeType,
           infoType: InfoType) = {
    this(s"${table.toString}.$key", localMerge, globalMerge, startingValue, accumDataType = accumDataType, accumModeType = accumModeType, infoType = infoType)
  }
  */

  /**
    * Creates GlobalAccumulator with its name set to the table name
    */
  def this(table: StatTable.Value,
           localMerge: (AccumulatorInfo[R], AccumulatorInfo[R]) => AccumulatorInfo[R],
           globalMerge: (AccumulatorInfo[R], AccumulatorInfo[R]) => AccumulatorInfo[R],
           startingValue: R,
           accumDataType: AccumData.GraphType,
           accumModeType: AccumMode.AccumModeType,
           infoType: InfoType) = {
    this(table.toString, localMerge, globalMerge, startingValue, accumDataType = accumDataType, accumModeType = accumModeType, infoType = infoType)
  }

  /* This accumulator is not used, but keep it for a while
  /**
    * Creates GlobalAccumulator with its name formatted as "table.key"
    */
  def this(table: StatTable.Value,
           key: String,
           localMerge: MergeableStat[R],
           globalMerge: MergeableStat[R],
           startingValue: R,
           accumDataType: AccumData.GraphType,
           accumModeType: AccumMode.AccumModeType,
           infoType: InfoType) = {
    this(s"${table.toString}.$key", localMerge, globalMerge, startingValue, accumDataType = accumDataType, accumModeType = accumModeType, infoType = infoType)
  }
  */

  /* This accumulator is not used, but keep it for a while
  /**
    * Creates GlobalAccumulator with its name set to the table name
    */
  def this(table: StatTable.Value,
           localMerge: MergeableStat[R],
           globalMerge: MergeableStat[R],
           startingValue: R,
           accumDataType: AccumData.GraphType,
           accumModeType: AccumMode.AccumModeType,
           infoType: InfoType) = {
    this(table.toString, localMerge, globalMerge, startingValue, accumDataType = accumDataType, accumModeType = accumModeType, infoType = infoType)
  }
  */

  private var localInfo: AccumulatorInfo[R] = AccumulatorInfo(startingValue, accumGraphType = accumDataType,
    accumModeType = accumModeType, name = name, infoType = infoType, modelId = modelId, timestamp_ns = timestamp_ns)

  private val sparkAccumulator = new GlobalStatSparkAccumulator[R](globalMerge)

  def localValue: R = localInfo.value

  def localCount: Long = localInfo.count

  /**
    * Calls the localStat update method to replace the local value, then increments the count.
    *
    * @param otherValue New element to modify the local value.
    */
  final def localUpdate(otherValue: R, timestamp_ns: String = null): Unit = {
    val accInfo = AccumulatorInfo(otherValue, accumGraphType = accumDataType, accumModeType = accumModeType,
      name = name, infoType = infoType, modelId = modelId, timestamp_ns = timestamp_ns)
    localInfo = localMerge.statMerge(localInfo, accInfo)
  }

  final def setLocalCount(count: Long): Unit = {
    localInfo = AccumulatorInfo(localInfo.value, count, accumGraphType = accumDataType, accumModeType = accumModeType, name = name, infoType = infoType, modelId = modelId)
  }

  /**
    * Updates the registered SparkAccumulator with the current localValue. To be used in
    * server-side functions since [[SparkContext]] is not serializable.
    */
  final def updateSparkAccumulator(): Unit = {
    sparkAccumulator.add(localInfo)
  }

  /**
    * Registers the Spark accumulator to the SparkContext if it has not been registered. Uses
    * the localStat name. Replaces the accumulator values with the GlobalStat's count and localValue.
    *
    * In order to push the accumulator to the Web-UI, an RDD is created using the localInfo.
    * A no-op map function is performed, which updates the accumulator.
    *
    * @param ctx SparkContext
    */
  final def updateSparkAccumulator(ctx: SparkContext): Unit = {
    if (!sparkAccumulator.isRegisteredAccumulator) {
      ctx.register(sparkAccumulator, name)
      sparkAccumulator.registerAccumulator()
    }

    val RDD = ctx
      .parallelize(Seq(this.localInfo), 1)
      .map(x => {
        sparkAccumulator.add(x); x
      })

    ctx.runJob(RDD, (iterator: Iterator[AccumulatorInfo[R]]) => {
      while (iterator.hasNext) {
        iterator.next()
      }
    })
  }

  final def getSparkAccumulator: GlobalStatSparkAccumulator[R] = sparkAccumulator
}

////////////////////////////////////////////////////////////////////////////////////
// INTERNAL FLINK/SPARK ACCUMULATORS
////////////////////////////////////////////////////////////////////////////////////

/**
  * Internal trait used for Flink and Spark accumulators. Contains a local value and count.
  *
  * @tparam R Type of the StatAccumulator.
  */
sealed trait StatAccumulator[R] {
  private var registeredAccumulator: Boolean = false
  private var localValue: AccumulatorInfo[R] = _

  protected val globalMerge: MergeableStat[R]

  def add(newValue: AccumulatorInfo[R]): Unit = localValue = newValue

  def get: AccumulatorInfo[R] = localValue

  /**
    * Replaces the count and local value with the merged output.
    *
    * @param otherAccum Other accumulator to merge with.
    */
  def internalMerge(otherAccum: Any)
  : Unit = {
    require(otherAccum.isInstanceOf[StatAccumulator[R]], "The merged accumulator must be of StatAccumulator.")
    val otherValue = otherAccum.asInstanceOf[StatAccumulator[R]].get
    if (localValue != null) {
      localValue = globalMerge.statMerge(localValue, otherValue)
    } else {
      localValue = otherValue
    }
  }

  def isRegisteredAccumulator: Boolean = registeredAccumulator

  def registerAccumulator(): Unit = registeredAccumulator = true

}

/**
  * Internal SparkAccumulator for a GlobalStat. Uses [[AccumulatorInfo]] as input to hold a local
  * and count. Uses [[AccumulatorInfo]] as output to wrap the input type within a Serializable class.
  *
  * @tparam R Type of the StatAccumulator.
  */
sealed class GlobalStatSparkAccumulator[R](override protected val globalMerge: MergeableStat[R])
  extends AccumulatorV2[AccumulatorInfo[R], AccumulatorInfo[R]] with StatAccumulator[R] {

  private var isReset: Boolean = true

  override def add(newValue: AccumulatorInfo[R]): Unit = {
    super.add(newValue)
    isReset = false
  }

  override def value: AccumulatorInfo[R] = this.get

  override def isZero: Boolean = isReset

  override def reset(): Unit = {
    isReset = true
  }

  override def copy(): AccumulatorV2[AccumulatorInfo[R], AccumulatorInfo[R]] = {
    this
  }

  override def merge(otherAccum: AccumulatorV2[AccumulatorInfo[R], AccumulatorInfo[R]]): Unit = {
    this.internalMerge(otherAccum)
  }
}
