package com.parallelmachines.reflex.web

import com.parallelmachines.mlops.{MLOpsEnvConstants, MLOpsEnvVariables}
import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent.EventType
import com.parallelmachines.reflex.common.enums.ModelFormat
import com.parallelmachines.reflex.common.events.{EventDescription, ModelAccepted}
import com.parallelmachines.reflex.common.mlobject.MLObjectType.MLObjectType
import com.parallelmachines.reflex.common.mlobject.Model
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.LoggerFactory

import scala.collection.mutable

object RestApis {
  private val logger = LoggerFactory.getLogger(getClass)

  private var scheme: String = "http"

  private def isReady: Boolean = {
    val ready = MLOpsEnvVariables.agentRestHost.isDefined && MLOpsEnvVariables.agentRestPort.isDefined
    if (!ready) {
      logger.error(s"Agent REST host/port env vars ${MLOpsEnvConstants.MLOPS_DATA_REST_SERVER.toString}/${MLOpsEnvConstants.MLOPS_DATA_REST_PORT.toString} are not set. All rest calls are ignored")
    }
    ready
  }

  /**
    * Set scheme to use in REST calls.
    *
    * @param scheme a string representing the scheme
    * @return
    */
  def setScheme(scheme: String): Unit = {
    this.scheme = scheme
  }

  /**
    * Get scheme used in REST calls.
    *
    * @return scheme
    */
  def getScheme: String = {
    scheme
  }

  /**
    * Build URI path,
    * in form '/path_component1/path_component2'
    *
    * @paramc pathComponents comma separated path components
    * @return scheme
    */
  def buildURIPath(pathComponents: String*): String = {
    s"/${pathComponents.mkString("/")}"
  }

  /**
    * Generate UUID on MCenter server.
    *
    * @param mlobjectType type of the object UUID is generated for
    * @return String generate UUID
    */
  def generateUUID(mlobjectType: MLObjectType): String = {
    var ret: String = ""
    if (!isReady) {
      return ret
    }
    val params = Map[String, String]("type" -> mlobjectType.toString)
    val cl = new RestClient(scheme, MLOpsEnvVariables.agentRestHost.get, Some(MLOpsEnvVariables.agentRestPort.get.toInt))
    val uri = buildURIPath(RestApiName.mlopsPrefix.toString, RestApiName.uuid.toString)
    ret = cl.getRequestAsString(uri, params)
    implicit val format = DefaultFormats
    ret = parse(ret).extract[Map[String, String]].get("id").get
    ret
  }

  /**
    * Post model to MCenter
    *
    * @param Model The Model
    * @return
    */
  def publishModel(model: Model): Unit = {
    if (!isReady) {
      return
    }
    if (model.getData.isEmpty) {
      logger.error(s"Model data is not defined, model will not be published")
      return
    }
    val params = mutable.Map[String, String]("modelName" -> model.getName,
      "description" -> model.getDescription,
      "modelId" -> model.getId,
      "format" -> model.getFormat.toString,
      "workflowInstanceId" -> MLOpsEnvVariables.workflowInstanceId.getOrElse(""),
      "pipelineInstanceId" -> MLOpsEnvVariables.pipelineInstanceId.getOrElse("")
    )
    val uri = buildURIPath(RestApiName.models.toString, params.get("pipelineInstanceId").get)
    RestApis.postBinaryContent(MLOpsEnvVariables.agentRestHost.get, MLOpsEnvVariables.agentRestPort.get.toInt, uri, model.getData.get, params.toMap)
  }

  /**
    * Post ModelAccepted to MCenter
    *
    * @param Model The Model
    * @return
    */
  def postModelAccepted(model: Model): Unit = {
    if (!isReady) {
      return
    }
    val params = Map[String, String]("pipelineInstanceId" -> MLOpsEnvVariables.pipelineInstanceId.getOrElse(""))

    val ed = new EventDescription(new ModelAccepted(model.getId))

    val cl = new RestClient(scheme, MLOpsEnvVariables.agentRestHost.get, Some(MLOpsEnvVariables.agentRestPort.get.toInt))
    val uri = buildURIPath(RestApiName.events.toString, params.get("pipelineInstanceId").get)
    cl.postString(uri, params, ed.toJson)
  }

  /**
    * Post content as multipart binary array.
    *
    * @param host    REST server host
    * @param port    REST server port
    * @param path    REST server api path
    * @param content Content
    * @param params  Request params
    * @return
    */
  private def postBinaryContent(host: String, port: Int, path: String, content: Array[Byte], params: Map[String, String]): Unit = {
    val cl = new RestClient(scheme, host, Some(port))
    cl.postBinary(path, params, content)
  }

  /**
    * Fetch last approved model metadata
    *
    * @param mlAppId            MLApp id
    * @param pipelineInstanceId pipeline instance id
    * @return Option[Map[String, Any]] metadata a map object
    **/
  private def getLastApprovedModelMetadata(mlAppId: String, pipelineInstanceId: String): Option[Map[String, Any]] = {
    var ret: Option[Map[String, Any]] = None
    val params = Map[String, String]("ionId" -> mlAppId,
      "pipelineInstanceId" -> pipelineInstanceId,
      "modelType" -> "lastApproved")
    val cl = new RestClient(scheme, MLOpsEnvVariables.agentRestHost.get, Some(MLOpsEnvVariables.agentRestPort.get.toInt))
    val uri = buildURIPath(RestApiName.mlopsPrefix.toString, RestApiName.models.toString)
    val response = cl.getRequestAsString(uri, params)

    implicit val format = DefaultFormats
    try {
      val lst = parse(response).extract[List[Map[String, Any]]]
      if (lst.nonEmpty) {
        ret = Some(lst.head)
      }
    } catch {
      case e: Throwable =>
        logger.error(s"Failed to parse response: '$response'", e.toString)
    }
    ret
  }

  /**
    * Download model data by id
    *
    * @param modelId model id
    * @return Option[Array[Byte]] model data
    */
  private def downloadModelById(modelId: String): Option[Array[Byte]] = {
    val params = Map[String, String]()
    val cl = new RestClient(scheme, MLOpsEnvVariables.agentRestHost.get, Some(MLOpsEnvVariables.agentRestPort.get.toInt))
    val uri = buildURIPath(RestApiName.mlopsPrefix.toString, RestApiName.models.toString, modelId, RestApiName.download.toString)
    cl.getRequestAsByteArray(uri, params)
  }

  /**
    * Fetch last approved model for given MLApp/Pipeline
    *
    * @param mlAppId            MLApp id
    * @param pipelineInstanceId pipeline instance id
    * @return Option[Model] approved model if found, else None
    */
  def getLastApprovedModel(mlAppId: String, pipelineInstanceId: String): Option[Model] = {
    if (!isReady) {
      return None
    }
    var retModel: Option[Model] = None
    val metadataMap: Option[Map[String, Any]] = this.getLastApprovedModelMetadata(mlAppId, pipelineInstanceId)
    if (metadataMap.isDefined) {
      val metadata = metadataMap.get
      val m = Model(metadata.get("name").get.asInstanceOf[String],
        ModelFormat.fromString(metadata.get("format").get.asInstanceOf[String]),
        metadata.get("stateDescription").get.asInstanceOf[String],
        Some(metadata.get("modelId").get.asInstanceOf[String]))
      val modelData = downloadModelById(m.getId)
      m.setData(modelData.get)
      retModel = Some(m)
    }
    retModel
  }

  /**
    * Fetch health stats for given model
    *
    * @param Model model to fetch stats for
    * @return String json string for health stats
    */
  def getModelHealthStats(model: Model): String = {
    var ret: String = ""
    if (!isReady) {
      return ret
    }
    val params = Map[String, String](
      "modelId" -> model.getId)
    val cl = new RestClient(scheme, MLOpsEnvVariables.agentRestHost.get, Some(MLOpsEnvVariables.agentRestPort.get.toInt))
    val uri = buildURIPath(RestApiName.mlopsPrefix.toString, RestApiName.modelStats.toString)
    val response = cl.getRequestAsString(uri, params)

    implicit val format = DefaultFormats
    try {
      val listOfMap = parse(response).extract[List[Map[String, String]]]
      if (listOfMap.nonEmpty) {
        val listOfMLHealthModel = listOfMap.filter(m => (m.get("type").get == EventType.MLHealthModel.name))
        ret = listOfMLHealthModel.head.get("data").get
      }
    } catch {
      case e: Throwable =>
        logger.error(s"Failed to parse response: '", e.toString)
    }
    ret
  }
}
