package org.apache.flink.streaming.scala.examples.common.stats

/**
  * Policy used for subsequent elements in a Stat Accumulator.
  */
object StatPolicy extends Enumeration with Serializable {
  val SUM = Value
  val REPLACE = Value
  val AVERAGE = Value
  val WEIGHTED_AVERAGE = Value
  val MAX = Value

  def apply[T](policy: StatPolicy.Value, value: T): MergeableStat[T] = {
    value match {
      case i: Int => getNumericStatPolicy(policy, i).asInstanceOf[MergeableStat[T]]
      case l: Long => getNumericStatPolicy(policy, l).asInstanceOf[MergeableStat[T]]
      case d: Double => getNumericStatPolicy(policy, d).asInstanceOf[MergeableStat[T]]
      case f: Float => getNumericStatPolicy(policy, f).asInstanceOf[MergeableStat[T]]
      case s: Short => getNumericStatPolicy(policy, s).asInstanceOf[MergeableStat[T]]
      case _ => getNonNumericStatPolicy(policy)
    }
  }

  private def getNumericStatPolicy[R](policy: StatPolicy.Value,
                                      value: R)(implicit num: Numeric[R])
  : MergeableStat[R] = {
    policy match {
      case REPLACE => new ReplaceStat[R]
      case SUM => new SumStat[R]()(num)
      case MAX => new MaxStat[R]()(num)
      case AVERAGE =>
        require(value.isInstanceOf[Double], "Must have double value type when using Average Policy")
        (new AverageStat).asInstanceOf[MergeableStat[R]]
      case WEIGHTED_AVERAGE =>
        require(value.isInstanceOf[Double], "Must have double value type when using Weighted Average Policy")
        (new WeightedAverageStat).asInstanceOf[MergeableStat[R]]
      case _ => throw new IllegalArgumentException("Invalid StatPolicy type")
    }
  }

  private def getNonNumericStatPolicy[R](policy: StatPolicy.Value)
  : MergeableStat[R] = {
    policy match {
      case REPLACE => new ReplaceStat[R]
      case _ => throw new IllegalArgumentException("Invalid StatPolicy type")
    }
  }
}

/**
  * Stat which adds new elements on arrival.
  */
sealed class SumStat[R]()(implicit num: Numeric[R]) extends MergeableStat[R]() {
  override def statMerge(local: AccumulatorInfo[R], other: AccumulatorInfo[R]): AccumulatorInfo[R] = {
    AccumulatorInfo(num.plus(local.value, other.value), local.count + other.count, accumModeType = local.accumModeType, accumGraphType = local.accumGraphType, name = local.name, infoType = local.infoType)
  }
}

/**
  * Stat which keeps the max element.
  */
sealed class MaxStat[R]()(implicit num: Numeric[R]) extends MergeableStat[R] {
  override def statMerge(local: AccumulatorInfo[R], other: AccumulatorInfo[R]): AccumulatorInfo[R] = {
    AccumulatorInfo(num.max(local.value, other.value), local.count + other.count, accumModeType = local.accumModeType, accumGraphType = local.accumGraphType, name = local.name, infoType = local.infoType)
  }

}

/**
  * Stat which replaces the current value with new elements on arrival.
  */
sealed class ReplaceStat[R] extends MergeableStat[R] {
  override def statMerge(local: AccumulatorInfo[R], other: AccumulatorInfo[R]): AccumulatorInfo[R] = {
    other
  }
}

/**
  * Stat which computes a running average.
  */
sealed class AverageStat extends MergeableStat[Double] {
  override def statMerge(local: AccumulatorInfo[Double], other: AccumulatorInfo[Double]): AccumulatorInfo[Double] = {
    AccumulatorInfo((local.value + other.value) / 2.0, local.count + other.count, accumModeType = local.accumModeType, accumGraphType = local.accumGraphType, name = local.name, infoType = local.infoType)
  }
}

/**
  * Stat which computes a running weighted average.
  */
sealed class WeightedAverageStat extends MergeableStat[Double] {
  override def statMerge(local: AccumulatorInfo[Double], other: AccumulatorInfo[Double]): AccumulatorInfo[Double] = {
    val sum = (local.value * local.count) + (other.value * other.count)
    val countSum = local.count + other.count
    var average: Double = 0
    if (countSum != 0) {
      average = sum / countSum
    }
    AccumulatorInfo(average, countSum, accumModeType = local.accumModeType, accumGraphType = local.accumGraphType, name = local.name, infoType = local.infoType)
  }
}