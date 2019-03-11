package org.mlpiper.infrastructure

/**
  * Object will hold all compatibility related to spark.
  * We should use this exclusively to prevent fatal failure because of weird engine connection.
  * For example, user attach component in ION which requires Hive and Hive is not supported,
  * then we should raise concern in graceful manner!
  */
object SparkCapability {
  var HiveSupport: Boolean = false
}
