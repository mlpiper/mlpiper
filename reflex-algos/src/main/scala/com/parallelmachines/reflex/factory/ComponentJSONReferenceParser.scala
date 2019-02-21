package com.parallelmachines.reflex.factory

import java.io.{File, IOException, PrintWriter}
import java.nio.file.Paths

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write
import org.slf4j.LoggerFactory

import com.parallelmachines.reflex.common.FileUtil

/**
  * Represents a reference structure, which points to the component's metadata (signature/description) file.
  *
  * @param metadataFilename  the component's description file name
  */
case class ComponentReference(metadataFilename: String) {
  def metadataFullPathname(dirPath: String): String = {
    Paths.get(dirPath, metadataFilename).toString
  }
}

/**
  * The ComponentJSONReferenceParser is designed to load and store a reference file, which points to
  * the component's metadata file (signature / description). The reference file name is constant and
  * therefor can always be detected regardless the component's metadata file name. The reference file
  * is designed to point to the real component's metadata file name, which can be different from one
  * component to another. The reference file is generated internally, when the component is loaded to
  * our system.
  */
object ComponentJSONReferenceParser {
  private val logger = LoggerFactory.getLogger(getClass)

  def parseReferenceJson(compRefJson: String) : ComponentReference = {
    implicit val format = org.json4s.DefaultFormats
    parse(compRefJson).extract[ComponentReference]
  }

  def parseReferenceFile(compRefFilePath: String) : ComponentReference = {
    val compRefJson = FileUtil.readContent(compRefFilePath)
    parseReferenceJson(compRefJson)
  }

  @throws(classOf[IOException])
  def saveReference(componentReference: ComponentReference, filePath: String): Unit = {
    implicit val formats = DefaultFormats
    val jsonStr = write(componentReference)

    val pw = new PrintWriter(new File(filePath))
    try {
      pw.write(jsonStr)
    } finally {
      pw.close()
    }
  }

  @throws(classOf[IOException])
  def generateReference(metadataFilename: String, dirPath: String): Unit = {
    val refPath = Paths.get(dirPath, componentReferenceFilename)
    saveReference(ComponentReference(metadataFilename), refPath.toString)
  }
}
