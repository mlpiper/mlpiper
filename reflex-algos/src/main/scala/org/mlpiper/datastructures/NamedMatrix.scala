package org.mlpiper.datastructures

import breeze.linalg.DenseVector

/** Class will hold vectors in each column */
case class ColumnVectorEntry(var columnName: String,
                             columnValue: DenseVector[_])
  extends Serializable {
  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case that: ColumnVectorEntry =>
        this.columnName.equals(that.columnName) && this.columnValue.equals(that.columnValue)
      case _ =>
        false
    }
  }

  override def toString: String = {
    s"$columnName : $columnValue"
  }
}

/** Class is responsible for holding [[ColumnVectorEntry]] in array */
case class NamedMatrix(var arrayOfVector: Array[ColumnVectorEntry])
  extends Serializable {
  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case that: NamedMatrix =>
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

  def getColumnEntryFromColumnName(columnName: String): Option[ColumnVectorEntry] = {
    for (columnEntry: ColumnVectorEntry <- arrayOfVector) {
      if (columnEntry.columnName.equals(columnName)) {
        return Some(columnEntry)
      }
    }
    None
  }
}
