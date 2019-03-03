
package org.mlpiper.stats

/**
  * Trait which defines merge function between two stats, either local or global.
  * @tparam R Value type of stat.
  */
abstract class MergeableStat[R] extends Serializable {

  /**
    * Method to merge accumulators in a reduce-like fashion.
    * @param local Left accumulator count and value
    * @param other Right accumulator count and value
    * @return Merged accumulator
    */
  def statMerge(local: AccumulatorInfo[R], other: AccumulatorInfo[R]): AccumulatorInfo[R]
}
