package com.parallelmachines.reflex.factory

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.NoSuchElementException

import com.parallelmachines.reflex.common.FileUtil
import com.parallelmachines.reflex.components.flink.streaming.algorithms.ModelBehaviorType
import com.parallelmachines.reflex.pipeline.ComputeEngineType.ComputeEngineType
import com.parallelmachines.reflex.pipeline.Language.Language
import com.parallelmachines.reflex.pipeline.{ComputeEngineType, Language}
import org.slf4j.LoggerFactory

import scala.io.Source


object ExternalDirComponentUtil {
  protected val logger = LoggerFactory.getLogger(getClass)
  val parser = ComponentJSONSignatureParser

  def verifyComponentDir(compDir: String, metadataFilename: String = componentSignatureFilename):
      ComponentMetadata = {
    logger.info(s"Verifying and loading dir: '$compDir', metadata filename: '$metadataFilename'")

    val compDirFile = new File(compDir)

    validateCompDirExistance(compDirFile)

    val signaturePath = getCompDescFile(compDirFile, metadataFilename)
    val signatureJSON = FileUtil.readContent(signaturePath)
    logger.info(s"After loading json: [$signatureJSON]")
    val compMeta = parser.parseSignature(signatureJSON)
    compMeta.signature = signatureJSON

    // TODO: the following 2 checks should be moved into the signature parser code.
    validateModelBehavior(compMeta)
    validateEngineType(compMeta)

    validateCompDirName(compDirFile, compMeta)
    validateCompProgFile(compDirFile, compMeta)

    compMeta
  }


  protected def checkModelDir(compMeta: ComponentMetadata): Boolean = {
    !compMeta.modelDirArgument().isEmpty
  }

  protected def checkInputModelPath(compMeta: ComponentMetadata): Boolean = {
    !compMeta.inputModelPathArgument().isEmpty
  }

  /**
    * Verify that model behavior is correct and the right tag arguments are provided
    * @param compMeta
    */
  def validateModelBehavior(compMeta: ComponentMetadata) : Unit = {
    if (compMeta.isUserStandalone && compMeta.modelBehavior.isEmpty) {
      val modelBehaviorStrings = ModelBehaviorType.values.toList.sorted.mkString(",")
      val msg = s"A stand alone component must have modelBehavior attribute defined: $modelBehaviorStrings"
      logger.error(msg)
      throw new Exception(msg)
    }

    if (compMeta.modelBehavior.isDefined) {
      val modelBehavior = {
        try {
          ModelBehaviorType.withName(compMeta.modelBehavior.get)
        } catch {
          case _: Throwable => throw new Exception("Invalid 'modelBehavior' field's value in component's " +
            s"description file! value: ${compMeta.modelBehavior.get}, " +
            s"valid values: ${ModelBehaviorType.values.mkString("[", ", ", "]")}")
        }
      }

      if (ModelBehaviorType.isModelProducer(modelBehavior)) {
        if (!checkModelDir(compMeta)) {
          val tag_name = ComponentArgumentsTags.OutputModelPath.toString
          val msg = s"Component ${compMeta.name} does not have a model directory argument tagged. " +
            s"Fix the component.json file by adding 'tag': '$tag_name' to the output model argument definition."
          logger.error(msg)
          throw new Exception(msg)
        }
      }

      if (ModelBehaviorType.isModelConsumer(modelBehavior)) {
        if (!checkInputModelPath(compMeta)) {
          val tag_name = ComponentArgumentsTags.InputModelPath.toString
          val msg = s"Component ${compMeta.name} does not have a input model path argument tagged. " +
          s"Fix the component.json file by adding 'tag': $tag_name to the input model argument definition."
          logger.error(msg)
          throw new Exception(msg)
        }
      }
    }
  }


  def validateCompDirExistance(compDirFile : File): Unit = {
      if (!compDirFile.exists() || !compDirFile.isDirectory) {
        val msg = s"External directory [${compDirFile.toString}] does not exists or is not a directory."
        logger.error(msg)
        throw new Exception(msg)
      }
  }

  def getCompDescFile(compDirFile: File, metadataFilename: String): File = {
    val sigFiles = compDirFile.listFiles.filter(f => f.isFile && f.canRead && f.getName == metadataFilename)
    if (sigFiles.isEmpty) {
      val msg = s"Could not detect signature file '$metadataFilename' inside '${compDirFile.toString}'"
      logger.error(msg)
      throw new Exception(msg);
    }
    sigFiles(0)
  }

  def validateEngineType(compMeta: ComponentMetadata): Unit = {
    try {
      ComputeEngineType.withName(compMeta.engineType)
    } catch {
      case _: NoSuchElementException => throw new Exception(s"Invalid engine type: ${compMeta.engineType}")
    }
  }

  def validateCompDirName(compDirFile: File, compMeta: ComponentMetadata) : Unit =  {
    if (compDirFile.getName != compMeta.name) {
      val msg = s"The component's directory name should match the 'name' field in 'component.json' " +
        s"['${compDirFile.getName}' != '${compMeta.name}']"
      logger.error(msg)
      throw new Exception(msg)
    }
  }

  def validateCompProgFile(compDirFile: File, compMeta: ComponentMetadata): Unit = {
    // Programming language is mandatory for any given component description
    val engineType: ComputeEngineType = ComputeEngineType.withName(compMeta.engineType)
    engineType match {

      case  ComputeEngineType.Python =>
        val language = validateLanguage(compMeta, Language.Python)

        language match {
          case Language.Java =>
            // Component written in java, either as a standalone or connected, must provide
            // the main java class name, which is the entry point for the program
            validateComponentClass(compMeta)
          case Language.Python =>
            if (!compMeta.isUserStandalone) {
              // In the case of Python, an assumption on the class name is done only for connected
              // components and the main class must exists in the main program file
              validateComponentClass(compMeta)
              validateComponentClassInMainSourceCode(compDirFile, compMeta)
            }
          case _ =>
        }
      case _ =>
    }
  }

  def validateLanguage(compMeta: ComponentMetadata, defaultLang: Language.Language): Language.Language = {
      if (!compMeta.language.isEmpty) {

        try {
          Language.withName(compMeta.language.get)
        } catch {
          case _: Throwable =>
            val msg = "Invalid programming language in component metadata description (json file):" +
              s" ${compMeta.language.get}"
            logger.error(msg)
            throw new Exception(msg)
        }
      } else{
        defaultLang
      }
  }

  def validateComponentClass(compMeta: ComponentMetadata): Unit = {
       if (compMeta.componentClass.isEmpty) {
        val msg = s"Missing mandatory 'componentClass' field in component metadata description (json file)!"
        logger.error(msg)
        throw new Exception(msg)
      }
  }

  def validateComponentClassInMainSourceCode(compDirFile: File, compMeta: ComponentMetadata): Unit = {
    if (compMeta.program.isEmpty) {
      val msg = s"Empty program path! Make sure the 'program' field in 'component.json' is valid!"
      logger.error(msg)
      throw new Exception(msg)
    }

    val progFullPath = Paths.get(compDirFile.toString, compMeta.program.getOrElse("<missing-program-file-path>"))
    if (!Files.exists(progFullPath)) {
      val msg = s"Invalid program path: '${compMeta.programPath()}'. Make sure the 'program' field in 'component.json' is valid!"
      logger.error(msg)
      throw new Exception(msg)
    }

    val bufferedSource = Source.fromFile(progFullPath.toFile)
    val content = bufferedSource.getLines.mkString
    bufferedSource.close()
    val className = s"\\s*class\\s+${compMeta.componentClass.get}\\s*[:(].*".r
    val result = className.findFirstIn(content)
    if (result.isEmpty) {
      val msg = s"Failed to find '${compMeta.componentClass.get}' class in '${compMeta.program.get}' program!" +
        s" Please make sure the component's class name that specified in the component's metadata" +
        s" description is actually defined in the component's main program!"
      logger.error(msg)
      throw new Exception(msg)
    }
  }

  def copyCompDirToRepo(compDir: String, compMeta: ComponentMetadata): Unit = {

  }

}
