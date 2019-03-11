package org.mlpiper.stats

import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import org.mlpiper.stat.histogram.continuous.Histogram
import org.mlpiper.utils.ParsingUtils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

trait GraphFormatOrdered extends Serializable with BarGraphFormatOrdered {

  def getDataMap: mutable.LinkedHashMap[String, Double]

  //TODO: toJsonable should be called according to currently set format
  def toGraphString: String = {
    ParsingUtils.iterableToJSON(toGraphJsonable)
  }

  def toGraphJsonable: Iterable[_] = {
    toJsonable(getDataMap)
  }
}

trait BarGraphFormatOrdered extends Serializable {
  /**
    * Sort, if possible, incoming strings by number values they represent
    **/
  def sort(str1: String, str2: String): Boolean = {
    var ret = false
    try {
      val split1 = str1.split(Histogram.Separator)
      val split2 = str2.split(Histogram.Separator)

      val d1 = if (split1(0).trim == Histogram.NegInfString) Int.MinValue.toDouble else split1(0).toDouble
      val d2 = if (split2(0).trim == Histogram.NegInfString) Int.MinValue.toDouble else split2(0).toDouble
      ret = d1 < d2
    } catch {
      case _: Throwable =>
    }
    ret
  }

  /** Outputs data in a jsonable data structure */
  def toJsonable(in: mutable.LinkedHashMap[String, Double]): List[mutable.LinkedHashMap[String, Double]] = {
    var retList: ListBuffer[mutable.LinkedHashMap[String, Double]] = ListBuffer()
    for (key <- in.keys.toList.sortWith(sort)) {
      retList += mutable.LinkedHashMap[String, Double](key -> in(key))
    }
    retList.toList
  }
}

object BarGraphFormatOrdered {
  def stringToLinkedHashMap(barGraphString: String): mutable.LinkedHashMap[String, Double] = {
    val listOfCategoricalHistogram: List[mutable.LinkedHashMap[String, Double]] =
      Json(DefaultFormats)
        .parse(barGraphString)
        .values.asInstanceOf[List[mutable.LinkedHashMap[String, Double]]]

    listMapToLinkedHashMap(listOfCategoricalHistogram)
  }

  def listMapToLinkedHashMap(listOfCategoricalHistogram: List[mutable.LinkedHashMap[String, Double]]): mutable.LinkedHashMap[String, Double] = {
    val listMap: mutable.LinkedHashMap[String, Double] = new mutable.LinkedHashMap[String, Double]()

    listOfCategoricalHistogram.foreach(x => listMap ++= x)
    listMap
  }
}
