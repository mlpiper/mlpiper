package com.parallelmachines.reflex.test.common

import com.parallelmachines.mlops.MLOps
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class InputStatFromDFStatTest extends FlatSpec with Matchers {
  /**
    * Calling Input Stat Function. It should execute without any error
    */
  it should "Execute Input Stat Function" in {
    val sparkSession = SparkSession.builder.
      master("local[*]").appName("Good DA And Health").getOrCreate()

    val SQLContext = sparkSession.sqlContext
    import SQLContext.implicits._

    val account = sparkSession.
      sparkContext.
      parallelize(Seq(
        (1, null.asInstanceOf[Double], 2, "F"),
        (2, 2.0, 4, "F"),
        (3, 3.0, 6, "N"),
        (4, null.asInstanceOf[Double], 8, null.asInstanceOf[String])))

    //+---+----+---+----+
    //|  A|   B|  C|   D|
    //+---+----+---+----+
    //|  1|null|  2|   F|
    //|  2| 2.0|  4|   F|
    //|  3| 3.0|  6|   N|
    //|  4|null|  8|null|
    //+---+----+---+----+
    val df: DataFrame = account
      .toDF("A", "B", "C", "D")

    MLOps.sparkContext = sparkSession.sparkContext

    noException should be thrownBy MLOps.inputStatsFromDataFrame(name = "Good DA With NA",
      modelId = null,
      df = df,
      histRDD = sparkSession.sparkContext.emptyRDD[String],
      sparkMLModel = null)

    //+---+---+---+----+
    //|  A|  B|  C|   D|
    //+---+---+---+----+
    //|  1|NaN|  2|   F|
    //|  2|2.0|  4|   F|
    //|  3|3.0|  6|   N|
    //|  4|NaN|  8|null|
    //+---+---+---+----+
    val naDF = df.na.fill(Double.NaN, df.columns)

    noException should be thrownBy MLOps.inputStatsFromDataFrame(name = "Good DA With NA",
      modelId = null,
      df = naDF,
      histRDD = sparkSession.sparkContext.emptyRDD[String],
      sparkMLModel = null)

    sparkSession.sparkContext.stop()
  }

  it should "Execute Input Stat Function For Whole Empty Column/NaN/Null" in {
    val sparkSession = SparkSession.builder.
      master("local[*]").appName("Good DA And Health").getOrCreate()

    val SQLContext = sparkSession.sqlContext
    import SQLContext.implicits._

    val account = sparkSession.
      sparkContext.
      parallelize(Seq(
        (1, Double.NaN),
        (2, Double.NaN),
        (3, Double.NaN),
        (4, Double.NaN)))

    //+---+---+
    //|  A|  B|
    //+---+---+
    //|  1|NaN|
    //|  2|NaN|
    //|  3|NaN|
    //|  4|NaN|
    //+---+---+
    val df: DataFrame = account
      .toDF("A", "B")

    MLOps.sparkContext = sparkSession.sparkContext

    noException should be thrownBy MLOps.inputStatsFromDataFrame(name = "Good DA With NA",
      modelId = null,
      df = df,
      histRDD = sparkSession.sparkContext.emptyRDD[String],
      sparkMLModel = null)

    sparkSession.sparkContext.stop()
  }
}
