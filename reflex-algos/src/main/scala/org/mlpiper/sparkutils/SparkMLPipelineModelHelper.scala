package org.mlpiper.sparkutils

import java.io.File
import java.nio.file.Paths
import java.util.UUID.randomUUID

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.ml.PipelineModel
import org.slf4j.LoggerFactory

import scala.util.control.Breaks._

/**
  * Class is responsible for providing model loading and saving
  */
class SparkMLPipelineModelHelper extends Serializable {

  private var engHadoop: SparkContext = _
  private var sparkContext: SparkContext = _
  private var engModel: PipelineModel = _
  private var hadoopFs: FileSystem = _

  // the ones below needs to be provided by user
  private var localPath: String = _
  private var sharedPath: String = _
  private var currentTmpDir = ""
  private var sharedPathPrefix: String = _

  // type of shared space provided

  private var sharedSpaceType = ""

  //this are preset variables.

  private val modelDirNamePrefix = "model_dir_"

  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * set the local path - a local path is MCenter system provided.
    * This path is local to the spark driver (and MCenter Agent).
    *
    * @param localPath1 the path provided by MCenter (as the model path)
    * @return None
    */
  def setLocalPath(localPath1: String): Unit = {
    if (localPath1.startsWith("file://")) {
      this.localPath = localPath1.replace("file://", "")
    }
    else
      this.localPath = localPath1
  }

  /**
    * set the shared path prefix - this is a user provided scratch space,
    * which is accessible
    * from any node on the spark cluster. This can either be a hdfs path or a nfs path.
    *
    * @param sharedPathPrefix1 : a user provided scratch space path
    * @return None
    */
  def setSharedPathPrefix(sharedPathPrefix1: String): Unit = {
    if (sharedPathPrefix1.isEmpty) {}
    else if (sharedPathPrefix1.startsWith("hdfs://") | sharedPathPrefix1.startsWith("file://")) {
      val parts = sharedPathPrefix1.split("://")
      this.sharedSpaceType = parts(0)
      this.sharedPathPrefix = parts(1)
    }
    else
      throw new Exception(s"Invalid prefix path type $sharedPathPrefix1")
  }

  /**
    * Provide the hadoop or spark context - this will be used when files are copied
    * into shared and loaded into spark.
    *
    * @param hadoop        : hadoop handle
    * @param sparkContext1 : spark context
    * @return None
    *
    */
  def setSharedContext(hadoop: SparkContext = null, sparkContext1: SparkContext = null): Unit = {
    this.sparkContext = sparkContext1
    if (hadoop != null)
      this.engHadoop = hadoop

    else if (sparkContext1 != null)
      this.engHadoop = sparkContext1

    if (this.engHadoop != null)
      this.hadoopFs = FileSystem.get(this.engHadoop.hadoopConfiguration)
  }

  /**
    * Copy a local path (MCenter provided) to a shared path location
    *
    * @param sharedPath1 : a user provided path (either hdfs or nfs)
    * @return None
    */
  def copyToSharedPath(sharedPath1: String): Unit = {
    if (this.localPath == null)
      throw new Exception(s"Local path needs to be specified to complete `copy_to_shared_path`")

    if (sharedPath1.isEmpty)
      throw new Exception(s"Shared path needs to be specified to complete `copy_to_shared_path`")

    if ((this.sharedSpaceType == "hdfs") & (this.engHadoop == null))
      throw new Exception(s"Hadoop handle not provided or configured, though shared path was hdfs")
    this.sharedPath = sharedPath1
    if (this.sharedSpaceType == "hdfs") {
      // file was trimmed when set_local_path was called
      this.localPath = "file://" + this.localPath

      // shared path may be provided by user or created by this library
      // so better to check for both cases (hdfs prefix is present or not)

      if (!sharedPath1.startsWith("hdfs://"))
        this.sharedPath = "hdfs://" + sharedPath1

      val dstPath = new Path(this.sharedPath)
      val srcPath = new Path(this.localPath)
      logger.info(s"Accessing HDFS - moving from $srcPath to $dstPath")

      // copying local model to hdfs
      this.hadoopFs.copyFromLocalFile(srcPath, dstPath)
    }
    else {
      // just copy
      logger.info(s"File copy - from ${this.localPath} to ${this.sharedPath}")
      FileUtils.copyDirectory(new File(this.localPath), new File(this.sharedPath))
    }
  }


  /**
    * Copy from a shared path to the local path
    *
    * @param sharedPath1 : a user provided shared path containing the model
    * @return None
    */
  def copyFromSharedPath(sharedPath1: String): Unit = {

    if (sharedPath1.isEmpty)
      throw new Exception(s"Shared path needs to be specified to complete `copy_from_shared_path`")
    if (this.localPath == null)
      throw new Exception(s"Local path needs to be specified to complete `copy_from_shared_path`")

    if (this.sharedSpaceType == "hdfs") {
      this.localPath = "file://" + this.localPath

      if (!sharedPath1.startsWith("hdfs://"))
        this.sharedPath = "hdfs://" + sharedPath1

      val srcPath = new Path(this.sharedPath)
      val dstPath = new Path(this.localPath)
      logger.info(s"Accessing HDFS - moving from $srcPath to $dstPath")

      // copying local model to hdfs
      this.hadoopFs.moveToLocalFile(srcPath, dstPath)
    }

    else {
      // just copy
      logger.info(s"File copy - from $sharedPath1 to ${this.localPath}")
      FileUtils.copyDirectory(new File(sharedPath1), new File(this.localPath))
    }
  }


  /**
    * Use the user provided scratch space and create a new unique path to save models
    * TODO: kindly check before returning the path
    *
    * @return unique path, randomly generated
    */
  def createUniqueSharedPath(): String = {
    if (this.sharedPathPrefix == null)
      throw new Exception(s"Shared path prefix must be set")

    this.currentTmpDir = Paths.get(this.sharedPathPrefix,
      this.modelDirNamePrefix + randomUUID().toString).toString
    this.currentTmpDir
  }

  /**
    * Delete the unique path that was created earlier
    *
    * @return None
    **/
  def deleteSharedTmpFile(): Unit = {

    if (this.currentTmpDir == null)
      throw new Exception(s"No temp path created - cannot delete it")

    if (this.engHadoop != null) {
      val path = new Path(this.currentTmpDir)
      this.hadoopFs.delete(path, true)
    }
    else {
      FileUtils.deleteDirectory(new File(this.currentTmpDir))
      this.currentTmpDir = null
    }
  }


  /**
    * A user facing wrapper that can load a sparkml model from the MCenter-provided
    * local path (via the shared space)
    *
    * @return sparkml model
    */
  def loadSparkmlModel(): PipelineModel = {

    // don't do anything if the path was preset with necessary information
    if (this.localPath.startsWith("hdfs://") | (this.sharedPathPrefix == null)) {
      logger.info(s"Direct load from ${this.localPath}")
      return PipelineModel.read.load(this.localPath)
    }

    // we may have to walk down the tree to find the right spark - ml folder
    val directoriesOfLocalPath = new File(this.localPath)
      .listFiles.filter(_.isDirectory).map(_.getName)
    for (file <- directoriesOfLocalPath) {
      val directoryInsideModelPath = Paths.get(this.localPath, file)
      val directoryInsideModelPathList = new File(directoryInsideModelPath.toString)
        .listFiles.filter(_.isDirectory).map(_.getName)
      for (eachFile <- directoryInsideModelPathList) {
        if (eachFile.contains("metadata")) {
          this.localPath = directoryInsideModelPath.toString
          logger.info(s"Switching to ${this.localPath} for model")
          break
        }
      }
    }

    try {
      // create a shared path
      val tmpPath = createUniqueSharedPath()
      // copy model from local path to shared path
      copyToSharedPath(tmpPath)
      // load the model
      this.engModel = PipelineModel.read.load(tmpPath)
      // delete local file
      deleteSharedTmpFile()
      this.engModel
    }
    catch {
      /** This will be thrown when try to schedule task twice. */
      case exception: Exception =>
        logger.error(s"Not able to load model because of error:")
        exception.printStackTrace()
        null
    }
  }


  /**
    * A user facing wrapper that helps save a spark-ml model to the MCenter provided local
    * path (via the shared space)
    *
    * @param smlModel : spark-ml model
    * @return None
    */
  def saveSparkmlModel(smlModel: PipelineModel): Unit = {

    // save directly, don't attempt transfer via shared space
    if (this.localPath.startsWith("hdfs://") | (this.sharedPathPrefix == null)) {
      logger.info(s"Direct write to ${this.localPath}")
      smlModel.write.overwrite().save(this.localPath)
      return
    }

    try {
      // create a shared path
      val tmpPath = createUniqueSharedPath()
      // save model to shared path
      smlModel.write.overwrite().save(tmpPath)
      // copy model from shared path to local path
      copyFromSharedPath(tmpPath)
      //delete shared path
      deleteSharedTmpFile()
    }
    catch {
      /** This will be thrown when try to schedule task twice. */
      case exception: Exception =>
        logger.error(s"Not able to save model because of error:")
        exception.printStackTrace()
    }
  }

}

