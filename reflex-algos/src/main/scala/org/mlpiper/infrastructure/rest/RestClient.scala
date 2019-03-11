package org.mlpiper.infrastructure.rest

import org.mlpiper.mlops.MLOpsEnvVariables
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.utils.URIBuilder
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.entity.mime.MultipartEntityBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.slf4j.LoggerFactory
import java.net.URLEncoder

import org.apache.http.HttpResponse

/**
  * Generic REST client implementation.
  * Currently string and binary requests are supported.
  */

class RestClient(scheme: String, host: String, port: Option[Int] = None) {
  private val logger = LoggerFactory.getLogger(getClass)

  val cl = HttpClients.createDefault()
  val uriBuilder = new URIBuilder()
  uriBuilder.setScheme(scheme).setHost(host)

  if (port.isDefined) {
    uriBuilder.setPort(port.get)
  }

  /**
    * Response content from get request can be fetched as a string, byte array
    * and others. When content expected to be JSON, it is fetched as String.
    * When content is e.g. model, content is byte array.
    * Generally speaking it is all about encoding, so when we use these
    * methods encoding is selected automatically according to content.
    */

  private def doGetRequest(path: String, params: Map[String, String]): Option[HttpResponse] = {
    var retResponse: Option[HttpResponse] = None
    val getRequest = new HttpGet()

    uriBuilder.setPath(path)

    if (MLOpsEnvVariables.token.isDefined) {
      getRequest.setHeader("Cookie", s"$getCookie;")
    }
    params.foreach(param => uriBuilder.setParameter(param._1, param._2))

    val uri = uriBuilder.build()
    getRequest.setURI(uri)

    try {
      val response = cl.execute(getRequest)
      if (response == null) {
        logger.error(s"Failed to make get request to $uri")
        return None
      }
      if (response.getStatusLine.getStatusCode != 200) {
        logger.error(s"Failed to get data from $uri, Http error code: ${response.getStatusLine.getStatusCode} - ${response.getStatusLine.getReasonPhrase}")
        response.close()
        return None
      }
      retResponse = Some(response)
    }
    catch {
      case e: Throwable =>
        logger.error(s"Failed to execute get request to $uri", e.toString)
    }
    retResponse
  }

  def getRequestAsString(path: String, params: Map[String, String]): String = {
    var ret: String = ""
    val response = doGetRequest(path, params)

    if (response.isDefined) {
      val entity = response.get.getEntity
      ret = EntityUtils.toString(entity)
      EntityUtils.consume(entity)
    }
    ret
  }

  def getRequestAsByteArray(path: String, params: Map[String, String]): Option[Array[Byte]] = {
    var ret: Option[Array[Byte]] = None

    val response = doGetRequest(path, params)

    if (response.isDefined) {
      val entity = response.get.getEntity
      ret = Some(EntityUtils.toByteArray(entity))
      EntityUtils.consume(entity)
    }
    ret
  }

  def postBinary(path: String, params: Map[String, String], content: Array[Byte]): Unit = {
    val post = new HttpPost()

    val entityBuilder = MultipartEntityBuilder.create
    entityBuilder.addBinaryBody("upload_binary_data", content, ContentType.APPLICATION_OCTET_STREAM, "binary_data_filename")
    val entity = entityBuilder.build()

    post.setEntity(entity)

    postEntity(path, params, post)
  }

  /**
    * TODO: keep an eye on this function and review later, as this call is blocking
    * and can cause performance issues.
    **/
  def postString(path: String, params: Map[String, String], content: String): Unit = {
    val post = new HttpPost()

    post.setHeader("Content-type", "application/json")
    post.setHeader("Accept", "application/json")

    val entity = new StringEntity(URLEncoder.encode(content, "UTF-8"))
    post.setEntity(entity)

    postEntity(path, params, post)
  }

  private def getCookie(): String = {
    s"token=${MLOpsEnvVariables.token.get}"
  }

  private def postEntity(path: String, params: Map[String, String], post: HttpPost): Unit = {
    uriBuilder.setPath(path)
    params.foreach(param => uriBuilder.setParameter(param._1, param._2))

    if (MLOpsEnvVariables.token.isDefined) {
      post.setHeader("Cookie", s"$getCookie;")
    }

    val uri = uriBuilder.build()
    post.setURI(uri)

    try {
      val response = cl.execute(post)

      if (response.getStatusLine.getStatusCode != 200) {
        logger.error(s"Failed to post data to $uri, Http error code: ${response.getStatusLine.getStatusCode} - ${response.getStatusLine.getReasonPhrase}")
      }
      response.close()
    } catch {
      case e: Throwable =>
        logger.error(s"Failed to execute post request to $uri", e.toString)
    }
  }
}
