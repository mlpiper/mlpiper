package com.parallelmachines.reflex.factory

import java.io.{File, IOException, PrintWriter}
import java.nio.file.Paths

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

case class ArgumentFields(key: String,
                          `type`: String,
                          label: String,
                          description: String,
                          optional: Option[Boolean],
                          defaultValue: Option[Double],
                          tag: Option[String])


case class InputOutputFields(description: String,
                             label: String,
                             defaultComponent: String,
                             `type`: String,
                             group: String)


class ComponentMetadata(val engineType: String,
                        val language: Option[String],
                        var name: String,
                        var label: String,
                        val userStandalone: Option[Boolean],
                        val version: String,
                        val group: String,
                        val program: Option[String],
                        val useMLOps: Option[Boolean],
                        val modelBehavior: Option[String],
                        var transient: Option[Boolean],
                        var componentClass: Option[String],
                        var includeGlobPatterns: Option[String],
                        var excludeGlobPatterns: Option[String],
                        var deps: Option[List[String]],
                        var tags: Option[List[String]],
                        val inputInfo: Option[List[InputOutputFields]],
                        val outputInfo: Option[List[InputOutputFields]],
                        val arguments: List[ArgumentFields]) {
  var signature: String = _

  def tagNameToKeyName(tagName: String): Option[String] = {
    val l = arguments.filter(x => x.tag.isDefined && x.tag.get == tagName)
   if (l.isEmpty) {
      None
    } else {
      Some(l.head.key)
    }
  }

  def modelDirArgument(): Option[String] = {
    tagNameToKeyName(ComponentArgumentsTags.OutputModelPath.toString)
  }

  def inputModelPathArgument(): Option[String] = {
    tagNameToKeyName(ComponentArgumentsTags.InputModelPath.toString)
  }

  def modelVersionArgument(): Option[String] = {
    tagNameToKeyName(ComponentArgumentsTags.ModelVersion.toString)
  }

  def logDirArgument(): Option[String] = {
    tagNameToKeyName(ComponentArgumentsTags.TFLogDir.toString)
  }

  def publicPortArgument(): Option[String] = {
    tagNameToKeyName(ComponentArgumentsTags.PublicPort.toString)
  }

  def programPath(): String = {
    val p = program.getOrElse("")
    Paths.get(name, p).toString
  }

  def isUsingMLOps: Boolean = {
    useMLOps.getOrElse(false)
  }

  def setName(name: String): Unit = {
    this.name = name
  }

  def setLabel(label: String): Unit = {
    this.label = label
  }

  def isTransient(): Boolean = {
    transient.getOrElse(false)
  }

  def setTransient(transient: Boolean): Unit = {
    this.transient = Some(transient)
  }

  def isUserStandalone: Boolean = {
    userStandalone.getOrElse(true)  // If missing, it is assumed as user's standalone app in order
                                    // to be backwards compatible with user's Python standalone app
  }

  def getTags(): Option[List[String]] = {
    tags
  }

}


object ComponentJSONSignatureParser {

  private val logger = LoggerFactory.getLogger(getClass)

  def parseSignature(jsonSignature: String) : ComponentMetadata = {

    logger.debug("About to parse component json")
    implicit val format = org.json4s.DefaultFormats
    try {
      parse(jsonSignature).extract[ComponentMetadata]
    } catch {
      case e : Throwable =>
        val msg = s"Invalid component's description file json format!"
        logger.error(s"$msg, exception: ${e.getMessage}")
        throw new Exception(msg)
    }
  }

  @throws(classOf[IOException])
  def saveSignature(componentMetadata: ComponentMetadata, filePath: String): Unit = {
    implicit val formats = DefaultFormats
    val jsonStr = write(componentMetadata)

    val pw = new PrintWriter(new File(filePath))
    try {
      pw.write(jsonStr)
    } finally {
      pw.close()
    }
  }
}
