package com.parallelmachines.reflex.test.reflexpipeline

import java.io.PrintWriter

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, Suite}
import org.slf4j.LoggerFactory

trait TestEnv extends BeforeAndAfterEach { this: Suite =>

  private val logger = LoggerFactory.getLogger(getClass)
  val outTmpFilePath = "/tmp/" + System.getProperty("user.name") + "_ReflexSystemITCase.out"
  var inTmpCsvFile : Option[java.io.File] = None

  override def beforeEach() {
    // Add additional setup functionality
    removePathIfExists(outTmpFilePath)
    removePathIfExists(inTmpCsvFile)
    super.beforeEach()
  }

  override def afterEach() {
    try super.afterEach()
    finally {
      // Add additional tear down functionality
      removePathIfExists(outTmpFilePath)
      removePathIfExists(inTmpCsvFile)
    }
  }

  def removePathIfExists(file: Option[java.io.File]) : Unit = {
    file match {
      case Some(f) => {
        logger.debug("Deleting directory: " + f.getAbsoluteFile)
        FileUtils.deleteQuietly(f)
      }
      case _ =>
    }
  }

  def removePathIfExists(path: String) : Unit = {
    removePathIfExists(Some(new java.io.File(path)))
  }

  def generateTmpCsvFile(content: String) : java.io.File = {
    val file = java.io.File.createTempFile(System.getProperty("user.name") + "_", ".csv")
    inTmpCsvFile = Some(file)
    file.deleteOnExit()
    val pw = new PrintWriter(file)
    pw.write(content)
    pw.close()
    file
  }
}
