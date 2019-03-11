package org.mlpiper.infrastructure.factory

import java.io.File
import java.nio.file.{Files, Paths}

import org.mlpiper.utils.FileUtil
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * A factory for Tensorflow python components, each component is provided as a directory containing the component
  * implementation (python) and a component.json file which will provide the component signature.
  *
  * @param testMode Register also testing components
  * @param externalDir External directory where to scan for components
  */
abstract class ExternalFileComponentFactory(testMode: Boolean, val externalDir:String) extends EngineComponentFactory {

  protected val logger = LoggerFactory.getLogger(getClass)

  protected var componentsDirList = mutable.MutableList[File]()

  // TODO: register component from scanning directories in the installation
  protected val parser = ComponentJSONSignatureParser

  require(externalDir != null, "Error initializing ExternalFileComponentFactory - externalDir should no be null")

  scanRepo()
  verifyAndRegisterComponents()

  /**
    * Scan the directory and return all sub-dirs
    * @return List[String] of sub directories
    */
  protected def scanRepo(): Unit = {
    logger.info(s"Scanning repo $externalDir")
    val d = new File(externalDir)

    if (!d.exists()) {
      throw new Exception(s"External directory '$externalDir' does not exist!")
    }

    componentsDirList = mutable.MutableList[File]() ++ d.listFiles.filter(f =>
      f.isDirectory && doesContainCompMetadataFile(f)
    )
    if (componentsDirList.isEmpty) {
      logger.warn("Component directories were not found!!!!")
    } else {
      logger.info(s"Detected ${componentsDirList.length} possible components: " +
        s"${componentsDirList.map(d => d.getName).mkString(", ")}")
    }
  }

  def doesContainCompMetadataFile(compDir: File): Boolean = {
    var exists = false
    val refFile = Paths.get(compDir.toString, componentReferenceFilename).toFile
    if (refFile.exists()) {
      exists = true
    } else {
      val metadataFile = Paths.get(compDir.toString, componentSignatureFilename).toFile
      if (metadataFile.exists()) {
        exists = true
      }
    }

    if (!exists) {
      logger.warn(s"Detected directory without metadata reference inside it: '$compDir'")
    }

    exists
  }

  def getComponentsDirList: java.util.List[File] = {
    componentsDirList.asJava
  }

  override def reconfigure(): Unit = {
    logger.info(s"Performing reconfigure for factory: dir = ${externalDir}")
    unRegisterComponents()
    scanRepo()
    verifyAndRegisterComponents()
  }

  protected def checkModelDir(compMeta: ComponentMetadata): Boolean = {
    if (compMeta.modelDirArgument().isEmpty) {
      false
    } else {
      true
    }
  }

  protected def checkInputModelPath(compMeta: ComponentMetadata): Boolean = {
    if (compMeta.inputModelPathArgument().isEmpty) {
      false
    } else {
      true
    }
  }


  protected def checkLogDir(compMeta: ComponentMetadata): Boolean = {
    if (compMeta.logDirArgument().isEmpty) {
      logger.warn(s"Component ${compMeta.name} does not have a log directory argument tagged")
      false
    } else {
      true
    }
  }

  protected def checkProgramPath(compMeta: ComponentMetadata): Boolean = {
    val program = compMeta.program.getOrElse("")
    val progPath = Paths.get(externalDir, compMeta.name, program)
    if (!Files.exists(progPath)) {
      logger.error(s"program ${progPath.toString} does not exists")
      false
    } else {
      true
    }
  }

  protected def detectComponentAdditionalFiles(compInfo: ComponentInfo): List[String] = {

    val compDirPath = Paths.get(externalDir, compInfo.componentMetadata.get.name)
    val compDirFile = new File(compDirPath.toString)
    val compDirFileList = compDirFile.listFiles.filter(f =>
      f.isFile &&
      f.canRead &&
      f.getName != compInfo.metadataFilename.getOrElse("") &&
      f.getName != compInfo.componentMetadata.get.program.getOrElse("")).toList
    val l = compDirFileList.map(f => f.getName)
    logger.debug(s"App program: '${compInfo.componentMetadata.get.program}'")
    if (l.nonEmpty) {
      logger.debug("Detected the following additional files: " + l.mkString(", "))
    }
    l
  }

  /**
    * Go over the detected directories, load the json file and if valid register the component
    */
  protected def verifyAndRegisterComponents(): Unit = {
    logger.info("Verifying detected components")

    componentsDirList.foreach { d => registerCompDir(d) }
  }

  def registerCompDir(compDir: File) : Unit = {
    var compDirFullPath : String = null
    try {
      compDirFullPath = Paths.get(externalDir, compDir.getName).toString
      val meta = readCompMeta(compDirFullPath)
      val metadataFilename = meta._1
      if (metadataFilename == null) {
        logger.warn(s"Detected directory without component's metadata file inside it: '$compDirFullPath'")
        return
      }

      val compMeta = meta._2
      val signatureJsonStr = meta._3

      ExternalDirComponentUtil.validateModelBehavior(compMeta)

      if (!checkProgramPath(compMeta)) {
        return
      }

      logger.info(s"Registering component: ${compMeta.name}")

      val compInfo = ComponentInfo("external-dir", compMeta.name, compMeta.name, classAvail = false,
        signatureJsonStr, Some(compMeta), Some(metadataFilename))
      components += compInfo

      detectComponentAdditionalFiles(compInfo)

    } catch {
      case e: Exception =>
        logger.warn(s"Invalid JSON signature or component directory found at [$compDirFullPath] + Exception: " + e.toString)
    }
  }

  def unRegisterComponents() : Unit = {
    components.clear()
  }

  def unRegisterComponent(compName: String): Unit = {
    components = components.filter(_.typeName != compName)
  }

  override def getComponentInfo(compTypeName: String): ComponentInfo = {
    val infoList = components.filter(x => x.canonicalName == compTypeName || x.typeName == compTypeName)
    logger.info(s"infoList $infoList")
    if (infoList.length == 1) {
      infoList.head
    } else {
      logger.info(s"returning null for $compTypeName")
      null
    }
  }

  override def componentSignature(typeName: String): String = {
    val comp = getComponentInfo(typeName)
    if (comp == null) {
      null
    } else {
      comp.signature
    }
  }

  def readCompMeta(compDirPath: String): (String, ComponentMetadata, String) = {
    var metadataFilename : String = null
    val metaRefFile = Paths.get(compDirPath, componentReferenceFilename).toFile
    if (metaRefFile.exists()) {
      val compRef = ComponentJSONReferenceParser.parseReferenceFile(metaRefFile.toString)
      metadataFilename = compRef.metadataFilename
    } else {
      logger.debug(s"Component metadata reference file was not found: ${metaRefFile.toString}! Falling back " +
        s"to default metadata file name: $componentSignatureFilename!")
      metadataFilename = componentSignatureFilename
    }

    val compMetadataFile = Paths.get(compDirPath, metadataFilename).toFile
    if (!compMetadataFile.exists()) {
      logger.info("Neither component metadata file nor reference file were found! path: " + compMetadataFile)
      return (null, null, null)
    }

    val signatureJsonStr = FileUtil.readContent(compMetadataFile)
    val compMetadata = ComponentJSONSignatureParser.parseSignature(signatureJsonStr)

    (metadataFilename, compMetadata, signatureJsonStr)
  }
}
