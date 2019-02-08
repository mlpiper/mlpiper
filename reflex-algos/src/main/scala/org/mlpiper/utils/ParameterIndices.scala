package org.mlpiper.utils

import scala.collection.mutable.ListBuffer

object ParameterIndices {

  private val RANGE_REGEX = "-"
  private val SEPARATOR_REGEX = ","

  private val SINGLE_INDEX = raw"""[0-9]+"""
  private val RANGE_INDEX = raw"""$SINGLE_INDEX$RANGE_REGEX$SINGLE_INDEX"""
  private val SINGLE_OR_RANGE_INDEX = raw"""(?:$SINGLE_INDEX|$RANGE_INDEX)"""

  private[ParameterIndices] val PARAMETER_INDICES_REGEX
  =
    raw"""^$SINGLE_OR_RANGE_INDEX(?:$SEPARATOR_REGEX$SINGLE_OR_RANGE_INDEX)*$$"""

  private def toRange(indexRange: String): Range = {
    var range: Range = null
    if (indexRange.matches(SINGLE_INDEX)) {
      val index = indexRange.toInt
      range = index to index
    } else if (indexRange.matches(RANGE_INDEX)) {
      val split = indexRange.split(RANGE_REGEX, 2)
      val t0 = split(0).toInt
      val t1 = split(1).toInt

      if (t1 > t0) {
        range = t0 to t1 by 1
      } else {
        range = t0 to t1 by -1
      }
    } else {
      throw new IllegalArgumentException("Not a valid index range: " + indexRange)
    }

    range
  }

  def validParameterIndices(indices: String): Boolean = {
    indices != null && indices.matches(PARAMETER_INDICES_REGEX)
  }
}

/**
  * Parses indices passed as a string into an [[Iterable]] of [[Int]].
  * Accepts zero or positive integers, ranges of integers (both ascending and descending), and
  * duplicates. Creates the iterable with respect to the order of user-passed indices.
  */
class ParameterIndices(indices: String) extends Iterable[Int] with Serializable {
  require(ParameterIndices.validParameterIndices(indices), "Invalid indices: " + indices)

  def this(size: Int) {
    this(raw"""0-${size - 1}""")
  }

  private val rangeList = try {
    indices.split(ParameterIndices.SEPARATOR_REGEX)
      .map(indexStr => ParameterIndices.toRange(indexStr))
      .foldLeft(new ListBuffer[Int]) { (list, range) => list ++= range.toList }
  } catch {
    case e: Throwable => throw new IllegalArgumentException(e.getMessage)
  }

  override def iterator: Iterator[Int] = rangeList.toIterator

  override def size: Int = rangeList.size

  def max: Int = rangeList.max

  def min: Int = rangeList.min
}
