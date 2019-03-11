package org.mlpiper.infrastructure

import scala.collection.mutable

/**
  * CollectedData is used by tests to provide convenient way for output comparisons.
  */
object CollectedData {
  private var collectedData = mutable.Map[String, Iterator[Any]]()

  def set(key: String, data: Iterator[Any]): Unit = {
    require(!collectedData.contains(key), s"Collected data already contains the key: $key")
    collectedData.put(key, data)
  }

  def get(key: String): Iterator[Any] = {
    require(collectedData.contains(key), s"Collected data does not contain the key: $key")
    collectedData(key)
  }

  def clear: Unit = {
    collectedData.clear
  }

  def isEmpty: Boolean = {
    collectedData.isEmpty
  }
}
