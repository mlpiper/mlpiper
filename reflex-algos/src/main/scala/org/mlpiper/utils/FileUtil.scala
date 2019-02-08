package org.mlpiper.utils

object FileUtil {
  def readContent(filePath: String, encoding: String = "utf-8", sep: String = ""): String = {
    val sourceBuffer  = scala.io.Source.fromFile(filePath, encoding)
    val content = sourceBuffer.getLines().mkString(sep)
    sourceBuffer.close()
    content
  }

  def readContent(file: java.io.File): String = {
    readContent(file.getAbsolutePath)
  }

  def readContent(file: java.io.File, encoding: String): String = {
    readContent(file.getAbsolutePath, encoding)
  }

  def readContent(file: java.io.File, encoding: String, sep: String): String = {
    readContent(file.getAbsolutePath, encoding, sep)
  }
}
