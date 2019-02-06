package org.apache.flink.streaming.scala.examples.clustering.svm2.stat

import com.parallelmachines.reflex.common.InfoType
import org.apache.flink.streaming.scala.examples.clustering.utils.ParsingUtils
import org.apache.flink.streaming.scala.examples.common.stats._

import scala.collection.mutable

class MarginDistanceStat(statWindowSize: Long)
  extends Serializable {
  private val mapOfBucketAndDistance: mutable.Map[Double, Double] = mutable.Map[Double, Double]()
  private val mapOfBucketAndTotalCount: mutable.Map[Double, Double] = mutable.Map[Double, Double]()

  // map holds stat information of each bucket in Map format
  private val statOfBuckets: mutable.Map[Double, Double] = mutable.Map[Double, Double]()

  /**
    * Method will return latest statistics of each bucket in Map format.
    */
  def getStatOfBuckets: mutable.Map[Double, Double] = {
    this.statOfBuckets
  }

  /**
    * Method will return latest statistics of each bucket in MarginDistanceStatWrapper wrapper.
    */
  def getStatsOfBucketWrapper: MarginDistanceStatWrapper = {
    MarginDistanceStatWrapper(statsOfBucket = this.getStatOfBuckets)
  }

  /**
    * Method will add new prediction result and update associated map.
    *
    * @param bucket   BucketID to what new prediction was done.
    * @param distance New prediction's margin distance to given class.
    */
  def addNewPrediction(bucket: Double, distance: Double): Unit = {
    // instead of going with per class, it will always be general. Thus, setting bucketId to 0 always
    val bucketID = 0

    // distance will always be needed to be considered as absolute
    val absoluteDistance: Double = distance.abs

    val bucketsTotalDistance = mapOfBucketAndDistance.getOrElseUpdate(bucketID, 0.0) + absoluteDistance
    val bucketsCount = mapOfBucketAndTotalCount.getOrElseUpdate(bucketID, 0.0) + 1.0

    // updating mapOfBucketAndDistance's and mapOfBucketAndTotalCount's associated bucketID details
    mapOfBucketAndDistance(bucketID) = bucketsTotalDistance
    mapOfBucketAndTotalCount(bucketID) = bucketsCount

    // updating statOfBuckets as soon as associated bucket's count reaches upto statWindowSize
    if (bucketsCount == statWindowSize) {
      this.updateStatBucket(bucketID)
    }
  }

  /** Updating stat information of associated bucketID. */
  private def updateStatBucket(bucketID: Double): Unit = {
    val totalNotedDistance: Double = mapOfBucketAndDistance(bucketID)
    val totalNotedElements: Double = mapOfBucketAndTotalCount(bucketID)
    val averagedDistance: Double = totalNotedDistance / totalNotedElements

    statOfBuckets(bucketID) = averagedDistance

    resetDetailsOfBucket(bucketID)
  }

  /** Reseting mapOfBucketAndDistance's and mapOfBucketAndTotalCount's bucketID information. */
  private def resetDetailsOfBucket(bucketID: Double): Unit = {
    mapOfBucketAndDistance(bucketID) = 0.0
    mapOfBucketAndTotalCount(bucketID) = 0.0
  }
}

object MarginDistanceStat {
  def mergeTwoMarginDistanceStatWrapper(x: MarginDistanceStatWrapper,
                                        y: MarginDistanceStatWrapper)
  : MarginDistanceStatWrapper = {
    val totalBuckets = x.statsOfBucket.toArray.map(_._1).union(y.statsOfBucket.toArray.map(_._1)).distinct

    val mapOfBucketAndDistance: mutable.Map[Double, Double] = mutable.Map[Double, Double]()

    val xStatsBucket = x.statsOfBucket
    val yStatsBucket = y.statsOfBucket

    for (eachBucketID <- totalBuckets) {
      if (xStatsBucket.contains(eachBucketID) && yStatsBucket.contains(eachBucketID)) {
        mapOfBucketAndDistance(eachBucketID) = (xStatsBucket(eachBucketID) + yStatsBucket(eachBucketID)) / 2
      }
      else if (xStatsBucket.contains(eachBucketID)) {
        mapOfBucketAndDistance(eachBucketID) = xStatsBucket(eachBucketID)
      }
      else {
        mapOfBucketAndDistance(eachBucketID) = yStatsBucket(eachBucketID)
      }
    }

    MarginDistanceStatWrapper(statsOfBucket = mapOfBucketAndDistance)
  }

  def getAccumulator(startingStatsOfBucket: MarginDistanceStatWrapper)
  : GlobalAccumulator[MarginDistanceStatWrapper] = {
    new GlobalAccumulator[MarginDistanceStatWrapper](
      StatNames.AbsoluteMarginDistance,
      localMerge = (_: AccumulatorInfo[MarginDistanceStatWrapper],
                    newHeatMap: AccumulatorInfo[MarginDistanceStatWrapper]) => {
        newHeatMap
      },
      globalMerge = (x: AccumulatorInfo[MarginDistanceStatWrapper],
                     y: AccumulatorInfo[MarginDistanceStatWrapper]) => {
        AccumulatorInfo(
          value = mergeTwoMarginDistanceStatWrapper(x.value, y.value),
          count = x.count + y.count,
          accumModeType = x.accumModeType,
          accumGraphType = x.accumGraphType,
          name = x.name,
          infoType = x.infoType
        )
      },
      startingValue = startingStatsOfBucket,
      accumDataType = AccumData.getGraphType(startingStatsOfBucket.statsOfBucket),
      accumModeType = AccumMode.TimeSeries,
      infoType = InfoType.InfoType.General
    )
  }
}

case class MarginDistanceStatWrapper(statsOfBucket: mutable.Map[Double, Double]) {
  override def toString: String = {
    ParsingUtils
      .iterableToJSON(statsOfBucket.map(x => (x._1.toString, x._2)))
  }
}
