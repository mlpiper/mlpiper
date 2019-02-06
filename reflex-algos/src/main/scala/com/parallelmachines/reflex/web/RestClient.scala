package com.parallelmachines.reflex.web

import com.parallelmachines.mlops.MLOpsEnvVariables
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.utils.URIBuilder
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.entity.mime.MultipartEntityBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.slf4j.LoggerFactory

import java.net.URLEncoder

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

  def getRequest(path: String, params: Map[String, String]): String = {
    var ret: String = ""
    val get = new HttpGet()

    uriBuilder.setPath(path)

    if (MLOpsEnvVariables.token.isDefined) {
      get.setHeader("Cookie", s"$getCookie;")
    }
    params.foreach(param => uriBuilder.setParameter(param._1, param._2))

    val uri = uriBuilder.build()
    get.setURI(uri)

    try {
      val response = cl.execute(get)

      if (response.getStatusLine.getStatusCode != 200) {
        logger.error(s"Failed to get data from $uri, Http error code: ${response.getStatusLine.getStatusCode} - ${response.getStatusLine.getReasonPhrase}")
      } else {
        ret = EntityUtils.toString(response.getEntity)
      }
      response.close()
    }
    catch {
      case e: Throwable =>
        logger.error(s"Failed to execute get request to $uri", e.toString)
    }
    ret
  }

  def postBinary(path: String, params: Map[String, String], content: String): Unit = {
    val post = new HttpPost()

    val entityBuilder = MultipartEntityBuilder.create
    entityBuilder.addBinaryBody("upload_binary_data", content.getBytes(), ContentType.APPLICATION_OCTET_STREAM, "binary_data_filename")
    val entity = entityBuilder.build()

    post.setEntity(entity)

    postEntity(path, params, post)
  }

  /**
    * TODO: keep an eye on this function and review later, as this call is blocking
    * and can cause performance issues.
    * */
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