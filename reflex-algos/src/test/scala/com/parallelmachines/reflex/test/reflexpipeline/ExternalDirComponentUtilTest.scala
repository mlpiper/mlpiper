package com.parallelmachines.reflex.test.reflexpipeline

import java.nio.file.Paths

import org.scalatest.FlatSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.slf4j.LoggerFactory
import com.parallelmachines.reflex.factory.ExternalDirComponentUtil
import com.parallelmachines.reflex.pipeline.ComputeEngineType

@RunWith(classOf[JUnitRunner])
class ExternalDirComponentUtilTest extends FlatSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  "External component directory" should "be valid" in {

    val componentsDir = DagTestUtil.getComponentsDir()
    logger.info(s"compDir $componentsDir")
    val testCompDir = Paths.get(componentsDir, ComputeEngineType.PySpark.toString, "mllib-random-forest").toAbsolutePath.toString
    logger.info(s"TestCompDir : $testCompDir")
    val compMeta = ExternalDirComponentUtil.verifyComponentDir(testCompDir)
    print(s"Comp meta: ${compMeta.engineType}")
  }

}
