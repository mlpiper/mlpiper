package org.apache.flink.streaming.scala.examples.common.serialize

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.apache.commons.codec.digest.DigestUtils
import org.apache.flink.api.common.io.RichOutputFormat
import org.apache.flink.configuration.Configuration
import org.slf4j.LoggerFactory

/** Class is responsible for performing operations relevant to S3 write up. */
class S3SinkForFlink[T](objectName: String,
                        bucketName: String,
                        accessKey: String,
                        secretAccessKey: String,
                        region: String = S3SinkForFlink.DefaultRegion,
                        maxPointToWrite: Int) extends RichOutputFormat[T] {

  private var s3ClientBuilder: AmazonS3 = _

  // it holds the large chunk of data that needs to be written
  private val contentHolder: StringBuilder = StringBuilder.newBuilder
  private var counterOfContentHolder = 0
  val maxContentLimit: Int = maxPointToWrite

  private var objectsWritten: Int = 0

  override def configure(parameters: Configuration): Unit = {
    /* Do Nothing*/
  }

  override def open(taskNumber: Int, numTasks: Int): Unit = {
    s3ClientBuilder
      = S3SinkForFlink.buildAmazonS3(bucketName = bucketName, accessKey = accessKey, secretAccessKey = secretAccessKey, region = region)
  }

  override def writeRecord(record: T): Unit = {
    val contentToSend = record.toString

    // appending new data on new Lines
    contentHolder.append(contentToSend + "\n")
    counterOfContentHolder += 1

    if (counterOfContentHolder == maxContentLimit) {
      // triggering writing after reaching max caching limit
      triggerWritingContent()
    }
  }

  override def close(): Unit = {
    // write remaining contents on the way to close
    triggerWritingContent()
  }

  private def triggerWritingContent(): Boolean = {
    try {
      if (contentHolder.nonEmpty) {
        val contentToUpload = contentHolder.toString()

        val meta = new ObjectMetadata

        meta.setContentMD5(new String(com.amazonaws.util.Base64.encode(DigestUtils.md5(contentToUpload))))
        meta.setContentLength(contentToUpload.length)

        val stream = new ByteArrayInputStream(contentToUpload.getBytes(StandardCharsets.UTF_8))

        val timeStamp = LocalDateTime.now

        // TODO: We can append pipelineID with object name
        val annotedFileName: String = s"${objectName}_${objectsWritten}_$timeStamp"

        val _ = s3ClientBuilder.putObject(bucketName, annotedFileName, stream, meta)

        objectsWritten += 1
        contentHolder.clear()
        counterOfContentHolder = 0

        if (objectsWritten == Int.MaxValue) {
          objectsWritten = 0
        }

        true
      }
      else {
        false
      }
    }
    catch {
      case e: Exception =>
        S3SinkForFlink.logger
          .error(s"Cannot write record to S3! Configuration for S3 is - " +
            s"bucketName : $bucketName, accessKey: $accessKey, secretAccessKey: $secretAccessKey, region: $region")

        false
    }
  }
}

object S3SinkForFlink {
  val DefaultRegion: String = "us-west-1"

  private val logger = LoggerFactory.getLogger(getClass)

  def buildAmazonS3(bucketName: String,
                    accessKey: String,
                    secretAccessKey: String,
                    region: String): AmazonS3 = {
    val basicAWSCredentials = new BasicAWSCredentials(accessKey, secretAccessKey)

    val amazonS3 = AmazonS3ClientBuilder
      .standard
      .withCredentials(new AWSStaticCredentialsProvider(basicAWSCredentials))
      .withRegion(region)
      .withChunkedEncodingDisabled(true)
      .withPathStyleAccessEnabled(true)
      .withPayloadSigningEnabled(true)
      .build

    if (!amazonS3.doesBucketExistV2(bucketName)) {
      try {
        amazonS3.createBucket(bucketName)
      } catch {
        case _: Exception =>
          logger.error(s"Failed to create new bucket with name of $bucketName with given credential. " +
            s"Chances are it may have already been there")
      }
    }
    amazonS3
  }
}
