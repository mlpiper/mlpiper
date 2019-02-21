package org.apache.flink.streaming.scala.examples.clustering.math

import breeze.linalg.DenseVector

/** Class will hold vectors in each column */
case class ReflexColumnVectorEntry(var columnName: String,
                                   columnValue: DenseVector[_])
  extends Serializable {
  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case that: ReflexColumnVectorEntry =>
        this.columnName.equals(that.columnName) && this.columnValue.equals(that.columnValue)
      case _ =>
        false
    }
  }

  override def toString: String = {
    s"$columnName : $columnValue"
  }
}

/** Class is responsible for holding [[ReflexColumnVectorEntry]] in array */
case class ReflexNamedMatrix(var arrayOfVector: Array[ReflexColumnVectorEntry])
  extends Serializable {
  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case that: ReflexNamedMatrix =>
        if (that.arrayOfVector.size == this.arrayOfVector.size) {

          var vectorMatch = true

          that.arrayOfVector.zip(this.arrayOfVector).foreach(eachZippedVals => {
            vectorMatch = eachZippedVals._1.equals(eachZippedVals._2)
          })

          vectorMatch
        } else {
          false
        }
      case _ =>
        false
    }
  }

  override def toString: String = {
    arrayOfVector.mkString("\n")
  }

  def getColumnEntryFromColumnName(columnName: String): Option[ReflexColumnVectorEntry] = {
    for (columnEntry: ReflexColumnVectorEntry <- arrayOfVector) {
      if (columnEntry.columnName.equals(columnName)) {
        return Some(columnEntry)
      }
    }
    None
  }
}
