package com.parallelmachines.reflex.pipeline

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import com.parallelmachines.reflex.common.constants.ReflexAlgosConstants
import com.parallelmachines.reflex.common.util.StringOps
import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponentFactory
import com.parallelmachines.reflex.components.flink.streaming.StreamExecutionEnvironment
import com.parallelmachines.reflex.components.spark.batch.SparkBatchComponentFactory
import com.parallelmachines.reflex.factory.{ReflexComponentFactory, SparkPythonComponentFactory, TensorflowComponentFactory}
import com.parallelmachines.reflex.pipeline.spark.stats.SystemStatsListener
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.mlpiper.mlops.MLOpsEnvVariables
import org.mlpiper.utils.FileUtil
import org.slf4j.LoggerFactory

import scala.reflect.runtime.universe._
import scala.util.control.NonFatal
import scala.util.parsing.json._
import scopt.OptionParser

/**
  * Main ReflexAlgos object - parsing command line arguments and running/validating dag or producing
  * Components definition file
  *
  */
object DagGen {
  val jsonVersion = "0"
  val dagVersion = "1.0.0"
  var jsonFile: String = ""

  var jobExecutionResult: Option[String] = None

  private val logger = LoggerFactory.getLogger(getClass)

  case class DagGenConfig(jsonFile: File = null,
                          jsonString: String = "",
                          jsonStringBase64: String = "",
                          parallelism: Option[Int] = None,
                          validateMode: Boolean = false,
                          componentDescriptionFile: String = "",
                          externalComponentsDir: String = "",
                          outputFile: String = "",
                          verbose: Boolean = false,
                          testMode: Boolean = false,
                          appName: String = "ReflexApp",
                          master: String = "",
                          engineConf: String = "",
                          restServerPort: Int = 12123,
                          waitForExit: Boolean = false,
                          callExitAtEnd: Boolean = false)

  private var config: DagGenConfig = _

  /**
    * Recursively run on a JSON data structure and print it in a formatted way
    *
    * @param json        JSON data structure
    * @param indentLevel Current indentation level
    * @return Formatted String
    */
  @deprecated
  private def formatJSON(json: Any, indentLevel: Int = 2): String = json match {
    case o: JSONObject =>
      o.obj.map { case (k, v) =>
        "  " * (indentLevel + 1) + JSONFormat.defaultFormatter(k) + ": " + formatJSON(v, indentLevel + 1)
      }.mkString("{\n", ",\n", "\n" + "  " * indentLevel + "}")

    case a: JSONArray =>
      a.list.map {
        e => "  " * (indentLevel + 1) + formatJSON(e, indentLevel + 1)
      }.mkString("[\n", ",\n", "\n" + "  " * indentLevel + "]")

    case _ =>
      JSONFormat defaultFormatter json
  }

  @deprecated
  private def systemConfigJSONStr: String = {
    val fieldList = typeOf[ReflexSystemConfig].members.filter(!_.isMethod).map(_.name).map(x => "\"" + x.toString.trim + "\"")
    "[" + fieldList.mkString(",") + "]"
  }

  /**
    * Generate a JSON string describing the Reflex components defined
    *
    * @return String containing the JSON description of all components
    */
  def generateComponentInfoJSON(): String = {
    ReflexComponentFactory.generateComponentsDescriptionJSON()
  }

  def handleCompDescArg(): Unit = {
    config.componentDescriptionFile match {
      case "-" =>
        println(generateComponentInfoJSON())
      case _ =>
        writeComponentDescToFile(config.componentDescriptionFile)
    }
  }

  def writeComponentDescToFile(filePath: String): Unit = {
    Files.write(Paths.get(filePath), generateComponentInfoJSON().getBytes(StandardCharsets.UTF_8))
  }

  def getPipeJSONStr: String = {

    if (config.jsonFile != null) {
      val pipeFileContent = FileUtil.readContent(config.jsonFile.getAbsolutePath, "utf-8", "\n")
      logger.info("Loaded file: ")
      logger.info(s"$pipeFileContent")
      pipeFileContent
    } else if (config.jsonString != "") {
      config.jsonString
    } else if (config.jsonStringBase64 != "") {
      Base64Wrapper.decode(config.jsonStringBase64)
    } else {
      throw new Exception("Pipeline json was not provided")
    }
  }

  /**
    * Parse the arguments obtained when running the jar
    *
    * @param args Array of arguments
    * @return DagGenConfig class containing the configuration of the DagGen
    */
  def parseArgs(args: Array[String]): DagGenConfig = {

    val parser = new scopt.OptionParser[DagGenConfig]("scopt") {
      // TODO: get the version from somewhere
      head("ReflexAlgo Flink Pipeline generator", "0.9")

      opt[File]("pipe").valueName("PIPE-FILE").
        action((x, c) => c.copy(jsonFile = x)).
        text("Path to pipeline description file")

      opt[String]("pipe-str").valueName("PIPE-STRING").
        action((x, c) => c.copy(jsonString = x)).
        text("Provide the pipeline as a string")

      opt[String]("pipe-str-base64").valueName("PIPE-STRING-BASE64").
        action((x, c) => c.copy(jsonStringBase64 = x)).
        text("Provide the pipeline as a string encoded in base64")

      opt[Int]('p', "parallelism").valueName("PARALLELISM").
        action((x, c) => c.copy(parallelism = Some(x))).
        text("Set execution parallelism")

      opt[Unit]("validate").action((_, c) => c.copy(validateMode = true)).
        text("Only validate graph. Do not run it on engine")

      opt[String]("comp-desc").valueName("COMPONENT-DESC-FILE").
        action((x, c) => c.copy(componentDescriptionFile = x)).
        text("Generate Reflex component description in given file")

      opt[String]("external-comp").valueName("EXTERNAL-COMPONENT-DIR").
        action((x, c) => c.copy(externalComponentsDir = x)).
        text("Directory containing external components files")

      opt[Unit]("verbose").action((_, c) =>
        c.copy(verbose = true)).text("Run in verbose mode")

      opt[Unit]("test-mode").action((_, c) =>
        c.copy(testMode = true)).text("Explicitly register components which are used for test mode")

      opt[String]("app-name").valueName("NAME").optional().
        action((x, c) => c.copy(appName = x)).
        text("Application name (Optional)")

      opt[String]("master").valueName("URL").optional().
        action((x, c) => c.copy(master = x)).
        text("Engine specific manager's URL (Optional)")

      opt[String]("conf").valueName("k1=v1,k2=v2,...").optional().
        action((x, c) => c.copy(engineConf = x)).
        text("Engine specific commandline configuration (Optional)")

      opt[Int]("rest-server-port").valueName("PORT").
        action((x, c) => c.copy(restServerPort = x)).
        text("Set REST server port")

      opt[Unit]("wait-for-exit").action((_, c) =>
        c.copy(waitForExit = true)).text("Wait for 'exit' command via rest api")

      opt[Unit]("call-exit-at-end").action((_, c) =>
        c.copy(callExitAtEnd = true)).text("Call sys.exit() at end of run")

    }

    parser.parse(args, DagGenConfig()) match {
      case Some(config) =>
        config
      case None =>
        throw new Exception("Bad arguments provided")
    }
  }

  def executeSparkBatchPipeline(reflexPipe: ReflexPipelineDag, config: DagGenConfig): Unit = {
    var spO: Option[SparkContext] = None
    try {
      val sparkSession = SparkSession.builder().appName(config.appName)
      if (!config.master.isEmpty) {
        sparkSession.master(config.master)
      } else if (SystemEnv.runningFromIntelliJ) {
        sparkSession.master("local[*]")
      } else {
        throw new Exception("Manager's URL must be provided!")
      }

      if (config.parallelism.isDefined) {
        sparkSession.config("spark.default.parallelism", config.parallelism.get.toString)
      }

      for (kvPart <- config.engineConf.split("\\s*,\\s*")) {
        Option(kvPart) filterNot {
          _.isEmpty
        } foreach { s =>
          val kv = s.split("\\s*=\\s*")
          kv.length match {
            case 2 => sparkSession.config(kv(0), kv(1))
            case _ => throw new Exception("Invalid engine configuration command line input: " + config.engineConf)
          }
        }
      }

      sparkSession
        .config(key = "mapreduce.input.fileinputformat.input.dir.recursive", value = true)

        /**
          * Register SparkListener here.
          * When Spark context is initiated, it also starts application.
          * So, if we want to override onApplicationStart, this is the place to register a listener. */
        .config("spark.extraListeners", new SystemStatsListener().getClass.getName)

      try {
        sparkSession
          .enableHiveSupport()
          // Querying External Hive Table And Recursive Tables Require Below Config Too
          .config(key = "hive.mapred.supports.subdirectories", value = true)

        SparkCapability.HiveSupport = true
      }
      catch {
        case _: Exception =>
          logger.debug("Hive is not enabled!")
          SparkCapability.HiveSupport = false
      }
      val env = sparkSession.getOrCreate().sparkContext

      spO = Some(env)
      logger.debug(s"SparkContext has been initialized correctly! Configs are ${env.getConf.getAll.mkString("\n")}")
    } catch {
      case t: Throwable =>
        throw new IllegalArgumentException(s"SparkContext has not been initialized correctly!\n" +
          s"Not executing pipeline because of that.\n" +
          s"Local error message is as follow:\n${t.getLocalizedMessage}")
    }

    if (spO.isDefined) {
      val env: SparkContext = spO.get

      var errMsg = ""
      try {
        reflexPipe.materialize(EnvironmentWrapper(env))
      } catch {
        case NonFatal(e) =>
          errMsg = e.toString // Using 'e.toString' ensures that 'errMsg' will never be null, even if the exception
          // does not contain any message. The exception type is always printed (along with the message)
          logger.error("Got non-fatal exception: " + errMsg)
          logger.error(e.getStackTrace.mkString("\n"))
          throw e
      } finally {
        logger.info("Stopping spark context")
        env.stop()

        if (errMsg.isEmpty) {
          logger.info("Reflex app exited gracefully!")
        } else {
          logger.error(s"${ReflexAlgosConstants.errorLinePrefix} err: $errMsg")
        }

        // Note:
        // There is a potential race condition between the SparkLauncher and the errors written to the logs -
        // It's possible that the FINISHED state will be received and processed by the SparkLauncher and
        // the hosting application, before all the logs are flushed to the output. As for Spark 2.1.1, there's no
        // valid mechanism to report about application failure, therefore it is done by analysing the logs. An
        // exception written to the log may be missed by the hosting application.
      }
    }
  }

  def listJVMThreads(): Unit = {
    val threads = Thread.getAllStackTraces.keySet
    logger.info("List of threads:")
    import scala.collection.JavaConversions._
    for (t <- threads) {
      val name = t.getName
      val state = t.getState
      val priority = t.getPriority
      val tt = if (t.isDaemon) "Daemon" else "Normal"
      logger.info(s"Thread $name $state $priority $tt")
    }
  }

  /**
    * The main entry point for the ReflexAlgos jar
    *
    * @param args String array containing possible arguments for the main
    */
  def main(args: Array[String]): Unit = {
    config = parseArgs(args)
    logger.info(StringOps.maskPasswords(config.toString))

    val externalComponentsDir = if (config.externalComponentsDir.isEmpty) "components" else config.externalComponentsDir
    logger.info("ExternalComponentRepo: " + externalComponentsDir)

    ReflexComponentFactory.init()
    ReflexComponentFactory.registerEngineFactory(ComputeEngineType.FlinkStreaming, FlinkStreamingComponentFactory(config.testMode))
    ReflexComponentFactory.registerEngineFactory(ComputeEngineType.SparkBatch, SparkBatchComponentFactory(config.testMode))

    try {
      if (config.testMode) {
        CollectedData.clear
      }

      // Emitting the component description to file or stdout
      if (config.componentDescriptionFile != "") {
        // Tensflow components will be registered only during the build phase of the reflex repo.
        val tfDir = Paths.get(externalComponentsDir, ComputeEngineType.Tensorflow.toString).toString
        ReflexComponentFactory.registerEngineFactory(
          ComputeEngineType.Tensorflow,
          TensorflowComponentFactory(config.testMode, tfDir))

        val sparkPythonDir = Paths.get(externalComponentsDir, ComputeEngineType.PySpark.toString).toString
        ReflexComponentFactory.registerEngineFactory(
          ComputeEngineType.PySpark,
          SparkPythonComponentFactory(config.testMode, sparkPythonDir))

        handleCompDescArg()
        return
      }

      // Taking the pipeline either from a file or from a string
      if (config.jsonFile == null && config.jsonString == "" && config.jsonStringBase64 == "") {
        throw new Exception("No pipeline definition was provided")
      }

      val reflexPipe = new ReflexPipelineBuilder().buildPipelineFromJson(getPipeJSONStr, testMode = config.testMode)

      if (config.validateMode) {
        return
      }

      // Compute engine specifc part
      // TODO: This should be generalized

      reflexPipe.pipeInfo.engineType match {
        case ComputeEngineType.FlinkStreaming =>
          val env = StreamExecutionEnvironment()
          if (config.parallelism.isDefined) {
            env.setParallelism(config.parallelism.get)
          }
          reflexPipe.materialize(EnvironmentWrapper(env))

          // Don't call execute, if it is TestMode and collectionData is not empty
          if (!(config.testMode && !CollectedData.isEmpty)) {
            jobExecutionResult = Some(env.execute(reflexPipe.pipeInfo.name))
          }
        case ComputeEngineType.SparkBatch =>

          /** This init is not really required, but helps to detect if rest host/port env vars were provided. */
          MLOpsEnvVariables.init
          executeSparkBatchPipeline(reflexPipe, config)
        case ComputeEngineType.Tensorflow =>
          throw new Exception(s"Engine type ${reflexPipe.pipeInfo.engineType} is not runnable through this code")
        case _ =>
          throw new Exception(s"Engine type ${reflexPipe.pipeInfo.engineType} is not supported yet")
      }
    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage)
        throw e
    }
    finally {
      logger.info("DagGen - is done - end of main")

      ReflexComponentFactory.cleanup()
      listJVMThreads()

      if (config.callExitAtEnd) {
        // TODO: find why some spark pipelines does not exit the JVM
        // TODO: the current exit is a bypass for that.
        // TODO: REF-2985 was opened to track this one.
        logger.info("Calling sys.exit(0)")
        sys.exit(0)
      }
    }
  }
}
