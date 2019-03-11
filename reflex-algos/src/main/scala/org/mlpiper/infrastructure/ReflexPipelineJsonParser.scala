package org.mlpiper.infrastructure

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
  * ComponentForParsing class is the same as Component
  * with the only difference for 'parents' type.
  *
  * jpmml requires json4s-jackson 3.2.11 version
  * which seems doesn't support ListBuffer when parsing
  *
  **/
sealed case class ComponentForParsing(name: String,
                                      id: Int,
                                      `type`: String,
                                      parents: List[Parent],
                                      arguments: Option[Map[String, Any]])

sealed case class PipelineForParsing(name: String,
                                     engineType: String,
                                     language: Option[String],
                                     systemConfig: ReflexSystemConfig,
                                     pipe: List[ComponentForParsing])

class ReflexPipelineJsonParser {

  private val logger = LoggerFactory.getLogger(classOf[ReflexPipelineJsonParser])


  /**
    * Parse and return the PipelineInfo part (metadata). Do not generate components
    *
    * @param jsonStr JSON string to parse
    * @return ReflexPipelineInfo object with the pipeline information (not component)
    */

  def parsePipelineInfo(jsonStr: String): ReflexPipelineInfo = {
    implicit val format = DefaultFormats

    logger.info("Parsing json pipeline ...")
    val json = parse(jsonStr)
    val pipeline = json.extract[PipelineForParsing]

    require(ComputeEngineType.values.exists(_.toString == pipeline.engineType),
      s"Not supported engine type: ${pipeline.engineType}")

    val pipeConverted = pipeline.pipe.map(x => Component(x.name, x.id, x.`type`, x.parents.to[ListBuffer], x.arguments)).to[ListBuffer]

    val pipeInfo = new ReflexPipelineInfo(
      name = pipeline.name,
      engineType = ComputeEngineType.withName(pipeline.engineType),
      language = pipeline.language,
      systemConfig = pipeline.systemConfig,
      pipe = pipeConverted)

    logger.debug("Pipeline extracted, name: " + pipeInfo.name)

    pipeInfo
  }
}
