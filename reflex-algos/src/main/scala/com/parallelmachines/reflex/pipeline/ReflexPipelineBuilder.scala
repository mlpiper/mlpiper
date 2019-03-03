package com.parallelmachines.reflex.pipeline

import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent
import com.parallelmachines.reflex.factory.ReflexComponentFactory
import org.mlpiper.datastructures.Prediction
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.existentials
import scala.reflect.runtime.universe._

case class Parent(var parent: Int, var output: Int, var input: Option[Int], eventType: Option[String], eventLabel: Option[String], var connectionTypeTag: Option[TypeTag[_]] = None) {
  var repeatEvent: Boolean = false
}

case class Component(name: String,
                     id: Int,
                     `type`: String,
                     parents: ListBuffer[Parent],
                     arguments: Option[Map[String, Any]]) {
  /* Flag needed to handle side component addition */
  var checkSideComponent: Option[Boolean] = Some(true)
  /* Required for merging sourceSingletons. Keeps list of output connections. */
  var sourceSingletonOutputs: ListBuffer[Parent] = _

  /**
    * Getter for arguments map
    */
  def getArguments: Map[String, Any] = {
    if (arguments.isDefined) arguments.get else Map[String, Any]()
  }

  /**
    * Compare the arguments of current component to provided one.
    * Used by eco to compared if components has similar arguments
    */
  def equalArgumentsTo(component: Component): Boolean = {
    this.arguments.equals(component.arguments)
  }
}

/**
  * Parser of the Pipeline json
  */

//TODO: Refactor this object and API
object ParserHelper {
  def addSystemConfig(args: mutable.Map[String, Any], systemArgs: ReflexSystemConfig): Unit = {
    // Converting the case class to a Map
    val values = systemArgs.productIterator
    val argMap: Map[String, Any] = systemArgs.getClass.getDeclaredFields.map(_.getName -> values.next).toMap[String, Any]

    /* Don't throw Exception if argument is defined in component, but not in system config */
    var argumentDefinedTwice = false
    for ((k, v) <- argMap) {
      if (args.contains(k)) {
        argumentDefinedTwice = true
      }
      v match {
        case o: Option[_] =>
          if (o.isDefined) {
            args(k) = o.get
          } else {
            argumentDefinedTwice = false
          }
        case _ => args(k) = v
      }
      require(!argumentDefinedTwice, s"Error system argument $k already exists in component map")
    }
  }
}

class ReflexPipelineBuilder {
  private val logger = LoggerFactory.getLogger(classOf[ReflexPipelineJsonParser])
  var pipeInfo: ReflexPipelineInfo = _

  val componentsMap = mutable.Map.empty[Int, Component]

  private var incomingEdges = collection.mutable.Set[String]()

  /**
    * Add the parents to the dad node in the right order from the json component
    *
    * @param node The ReflexDagNode to use
    * @param comp Json component to take info from
    */
  private def addParents(node: ReflexDagNode, comp: Component): Unit = {
    var incomingEdgesCount = 0

    val inputs = comp.parents.map(_.input)

    // parents don't contain 'input' field
    if (inputs.exists(_.isEmpty)) {
      if (inputs.exists(_.isDefined)) {
        throw new Exception(s"Parent JSON is in mixed format - some parents without input index and some with: in ${comp.`type`}")
      }
      incomingEdgesCount = comp.parents.length
    } else {
      // parents contain 'input' field
      if (inputs.size != inputs.distinct.size) {
        throw new Exception("More than one parent is connected to the same input index")
      }
      // detecting max input index and add 1 to get sufficient count of incoming edges
      incomingEdgesCount = inputs.map(_.get).max + 1
    }

    node.incomingEdges = mutable.ArrayBuffer.fill[EdgeSideInfo](incomingEdgesCount)(new EdgeSideInfo(-1, -1))

    for ((parent, idx) <- comp.parents.zipWithIndex) {
      val inputIndex = parent.input.getOrElse(idx)
      node.incomingEdges(inputIndex) = new EdgeSideInfo(parent.parent, parent.output)
    }
  }

  // TODO: handle a case when a singleton can be not a source or a sink
  private def mergeSinkSingletons(): Unit = {
    val allSinkSingletons = ListBuffer[Component]()

    // Filter out only Sink Singletons
    for (comp <- pipeInfo.pipe) {
      val compInstance = ReflexComponentFactory(pipeInfo.engineType, comp.`type`, null)
      if (compInstance.isSingleton && !compInstance.isSource) {
        allSinkSingletons += comp
      }
    }

    // Get list of classes for SinkSingletons (currently only EventSocketSink)
    val distinctNamesOfSinkSingletons = allSinkSingletons.map(_.`type`).distinct

    for (name <- distinctNamesOfSinkSingletons) {
      // Check that all parents, used with singletons, have eventType and eventLabel
      for (comp <- allSinkSingletons.filter(_.`type` == name)) {
        comp.parents.foreach(fetchEventTypeAndLabel(_, comp.`type`))
      }

      // For all singletons of the same type get all parents and point them to a new component
      val listOfParents = allSinkSingletons.filter(_.`type` == name).flatMap(_.parents)

      /*
       * Set input field to be the same as parent's order.
       * Copy connection tag from the component definition to parent, to use it in
       * addConnectionToSingletonComponent
       */
      var i = 0
      for (p <- listOfParents) {
        val parentId = p.parent
        val parentTypeName = pipeInfo.getComponentById(parentId).get.`type`

        val compInstance = ReflexComponentFactory(pipeInfo.engineType, parentTypeName, null)
        p.connectionTypeTag = Some(compInstance.outputTypes(p.output).tag)

        p.input = Some(i)
        i += 1
      }

      pipeInfo.addComponent(Component("Auto Generated Singleton", pipeInfo.getMaxId + 1, name, listOfParents, None))
    }
    // Remove original Sink singletons from pipeline
    pipeInfo.pipe --= allSinkSingletons
  }

  private def mergeSourceSingletons(): Unit = {
    val allSourceSingletons = ListBuffer[Component]()
    val allParentsWithCompType = ListBuffer[(Parent, String)]()

    // Build a list of all parents with typeName of the component, where this parent resides (for error message)
    for (comp <- pipeInfo.pipe) {
      for (p <- comp.parents) {
        allParentsWithCompType += ((p, comp.`type`))
      }
    }

    /* Filter out only Source Singletons */
    for (comp <- pipeInfo.pipe) {
      val compInstance = ReflexComponentFactory(pipeInfo.engineType, comp.`type`, null)
      if (compInstance.isSingleton && compInstance.isSource) {
        allSourceSingletons += comp
      }
    }

    /* Get list of type names for all Source Singletons */
    val distinctTypesOfSourceSingletons = allSourceSingletons.map(_.`type`).distinct

    for (typeName <- distinctTypesOfSourceSingletons) {
      val newId = pipeInfo.getMaxId + 1

      // Create new singleton of this type
      val newSourceSingleton = Component("Auto Generated Singleton", newId, typeName, ListBuffer[Parent](), None)

      // Get ids of source singletons of this type
      val idsSourceSingletonsOfCurType = allSourceSingletons.filter(_.`type` == typeName).map(_.id)

      /*
       * Find all parents which were connected to singletons of this type.
       * Update parent and output fields to make parents point to new Source Singleton.
       */
      val listOfParentsConnectedToSourceSingletonsOfCurType = ListBuffer[Parent]()
      var i = 0
      for ((parent, typeName) <- allParentsWithCompType) {
        if (idsSourceSingletonsOfCurType.contains(parent.parent)) {
          fetchEventTypeAndLabel(parent, typeName)
          parent.parent = newId
          parent.output = i

          /* Copy connection tag from the component definition to parent, to use it in
           * addConnectionToSingletonComponent
           */
          val compInstance = ReflexComponentFactory(pipeInfo.engineType, typeName, null)
          parent.connectionTypeTag = Some(compInstance.inputTypes(parent.input.get).tag)

          listOfParentsConnectedToSourceSingletonsOfCurType += parent
          i += 1
        }
      }

      newSourceSingleton.sourceSingletonOutputs = listOfParentsConnectedToSourceSingletonsOfCurType
      pipeInfo.addComponent(newSourceSingleton)
    }
    // Remove original Source Singletons from pipeline
    pipeInfo.pipe --= allSourceSingletons
  }

  /**
    * Checks presence and fetches eventType and eventLabel fields from JSON:
    * "parents": [{"parent": 1, "output": 0, "eventType":"Alert"}],
    *
    * @param parent Parent object of a node
    * @return (eventType, eventLabel)
    */
  private def fetchEventTypeAndLabel(parent: Parent, compName: String): (ReflexEvent.EventType, Option[String]) = {
    val eventTypeEnums = ReflexEvent.EventType.enumCompanion.values.mkString(", ")
    require(parent.eventType.isDefined, s"eventType must be defined in $compName for input: {parent: ${parent.parent}, output: ${parent.output}}")
    val eventType = ReflexEvent.EventType.enumCompanion.fromName(parent.eventType.get)
    val eventLabel = parent.eventLabel
    require(eventType.isDefined, s"Wrong event type: ${parent.eventType.get} in $compName for input: {parent: ${parent.parent}, output: ${parent.output}}; Expected values: [$eventTypeEnums]")
    (eventType.get, eventLabel)
  }

  /**
    * Singletons don't have input/output connections by default.
    * Register connections according to provided information.
    *
    * @param component component instance for which we register
    * @param curComp   wrapper around JSON component description
    */
  private def addConnectionToSingletonComponent(component: ReflexPipelineComponent, curComp: Component): Unit = {
    var connectionList: ConnectionList = null
    var parentsList: ListBuffer[Parent] = null
    if (!component.isSingleton) {
      return
    }

    if (!List[ComputeEngineType.Value](ComputeEngineType.FlinkStreaming, ComputeEngineType.SparkBatch).contains(pipeInfo.engineType)) {
      throw new Exception(s"Engine ${pipeInfo.engineType} is not supported by singleton components")
    }

    if (component.isSource) {
      connectionList = component.outputTypes
      parentsList = curComp.sourceSingletonOutputs
    } else {
      connectionList = component.inputTypes
      parentsList = curComp.parents
    }

    for ((parent, idx) <- parentsList.zipWithIndex) {
      // Fetch evenType and eventLabel
      val (eventType, eventLabel) = fetchEventTypeAndLabel(parent, curComp.`type`)

      // Add output connection to Singleton according to eventType and eventLabel
      connectionList += ComponentConnection(
        tag = parent.connectionTypeTag.get,
        // Labels must be unique, so add idx
        label = eventType.toString + idx.toString,
        eventTypeInfo = Some(EventTypeInfo(eventType, eventLabel, parent.repeatEvent)),
        group = ConnectionGroups.OTHER)
    }
  }

  private def addDefaultOutputs(curComp: Component): Unit = {
    val compId = curComp.id
    val compInstance = ReflexComponentFactory(pipeInfo.engineType, curComp.`type`, null)

    logger.debug(s"Checking node outputs: $compId")
    for (outputIndex <- compInstance.outputTypes.indices) {
      logger.debug(s"\t\toutput: $outputIndex")
      // Check if output is already connected
      if (!incomingEdges(compId.toString + outputIndex.toString)) {
        logger.debug("\t\toutput does not exists")
        compInstance.outputTypes(outputIndex).defaultComponentClass match {
          case Some(compType) =>
            val defaultClassSimpleName = compType.getSimpleName
            var childComp: Component = null
            var eventTypeStr: Option[String] = None
            var eventLabelStr: Option[String] = None

            // if component is a singleton
            if (ReflexComponentFactory(pipeInfo.engineType, defaultClassSimpleName, null).isSingleton) {
              require(compInstance.outputTypes(outputIndex).eventTypeInfo.isDefined,
                s"SingletonEventType is not defined for connection idx: $outputIndex, component ${compInstance.name}")
              eventTypeStr = Some(compInstance.outputTypes(outputIndex).eventTypeInfo.get.eventType.toString())
              eventLabelStr = compInstance.outputTypes(outputIndex).eventTypeInfo.get.eventLabel
            }

            logger.debug(s"Adding a default output node to node: $compId output index: $outputIndex")
            val parent = Parent(compId, outputIndex, Some(0), eventTypeStr, eventLabelStr)
            if (compInstance.outputTypes(outputIndex).eventTypeInfo.isDefined) {
              parent.repeatEvent = compInstance.outputTypes(outputIndex).eventTypeInfo.get.forwardEvent
            }
            childComp = Component("Auto generated", pipeInfo.getMaxId + 1, defaultClassSimpleName, ListBuffer[Parent](parent), None)
            incomingEdges += compId.toString + outputIndex.toString
            pipeInfo.addComponent(childComp)
          case None =>
            val msg = s"Error: Graph is invalid, missing output for component id = $compId"
            logger.error(msg)
            throw new Exception(msg)
        }
      } else {
        logger.debug("\t\toutput exists")
      }
    }
  }

  private def addDefaultInputs(curComp: Component): Unit = {
    val compId = curComp.id
    val compInstance = ReflexComponentFactory(pipeInfo.engineType, curComp.`type`, null)

    logger.debug(s"Checking node inputs: $compId")
    // Assuming the default inputs are at the end
    if (curComp.parents.length < compInstance.inputTypes.length) {
      logger.debug("There are missing inputs to this node")

      for (inputIndex <- compInstance.inputTypes.indices) {
        if (!curComp.parents.map(_.input.get).contains(inputIndex)) {
          //for (inputIndex <- curComp.parents.length to compInstance.inputTypes.length - 1) {
          logger.debug(s"Adding missing input idx $inputIndex")
          compInstance.inputTypes(inputIndex).defaultComponentClass match {
            case Some(defaultClass) =>
              val defaultClassSimpleName = defaultClass.getSimpleName
              var parentComp: Component = null
              val newId = pipeInfo.getMaxId + 1
              var eventTypeStr: Option[String] = None
              var eventLabelStr: Option[String] = None

              // if component is a singleton
              if (ReflexComponentFactory(pipeInfo.engineType, defaultClassSimpleName, null).isSingleton) {
                require(compInstance.inputTypes(inputIndex).eventTypeInfo.isDefined,
                  s"SingletonEventType is not defined for connection idx: $inputIndex, component ${compInstance.name}")

                eventTypeStr = Some(compInstance.inputTypes(inputIndex).eventTypeInfo.get.eventType.toString())
                eventLabelStr = compInstance.inputTypes(inputIndex).eventTypeInfo.get.eventLabel

              } else {
                require(ReflexComponentFactory(pipeInfo.engineType, defaultClassSimpleName, null).outputTypes.size == 1, s"Default input component '${defaultClass.getCanonicalName}' must have only 1 output")
              }
              parentComp = Component("Auto generated", newId, defaultClassSimpleName, ListBuffer[Parent](), None)
              val parent = Parent(newId, 0, Some(inputIndex), eventTypeStr, eventLabelStr)
              if (compInstance.inputTypes(inputIndex).eventTypeInfo.isDefined) {
                parent.repeatEvent = compInstance.inputTypes(inputIndex).eventTypeInfo.get.forwardEvent
              }
              curComp.parents.insert(inputIndex, parent)

              incomingEdges += newId.toString + 0.toString
              pipeInfo.addComponent(parentComp)

            case None =>
              val msg = s"Error: Graph is invalid, missing input for component id = $compId"
              logger.error(msg)
              throw new Exception(msg)
          }
        }
      }
    }
  }

  private def addSideComponents(curComp: Component): Unit = {
    val compId = curComp.id
    val compInstance = ReflexComponentFactory(pipeInfo.engineType, curComp.`type`, null)

    logger.debug(s"Checking node side components: $compId")
    // Assuming the default inputs are at the end
    for (i <- compInstance.inputTypes.indices) {
      if (compInstance.inputTypes(i).sideComponentClass.isDefined && curComp.checkSideComponent.get) {
        val sideComponentClass = compInstance.inputTypes(i).sideComponentClass.get
        logger.debug(s"Adding side input idx $i")
        logger.debug(s"Found a side component: ${sideComponentClass.getSimpleName}")

        var newId = pipeInfo.getMaxId + 1
        val p = curComp.parents(i)

        // create dup component, with the same input as current component
        val dupComp = Component("Auto generated", newId, MandatoryComponentsPerEngine.TwoDup.toString, ListBuffer[Parent](Parent(p.parent, p.output, Some(0), p.eventType, p.eventLabel)), None)
        // disconnect input from current component
        curComp.parents.remove(i)
        // connect 0th Dup's output into current component
        curComp.parents.insert(i, Parent(dupComp.id, 0, Some(i), None, None))
        incomingEdges += dupComp.id.toString + 0.toString
        pipeInfo.addComponent(dupComp)

        newId = pipeInfo.getMaxId + 1
        // add side component and connect to the 1st Dup's output
        val sideComponent = Component("Auto generated", newId, sideComponentClass.getSimpleName, ListBuffer[Parent](Parent(dupComp.id, 1, Some(0), None, None)), None)
        incomingEdges += dupComp.id.toString + 1.toString
        pipeInfo.addComponent(sideComponent)
      }
    }
    curComp.checkSideComponent = Some(false)
  }

  private def addDefaultComponents(): Unit = {
    //TODO: need to optimize current solution.
    //TODO: t.e. if default node is going to be added, check if component in this node could cause a loop
    logger.debug("Adding default nodes")
    var startLength = pipeInfo.pipe.length
    var endLength = 0
    var numResolveIters = 42
    /* This code will iterate over all nodes and apply default and side components.
       It will try to perform 42 iterations, if no success, exit.
     */
    while (startLength != endLength) {
      if (0 == numResolveIters) {
        throw new Exception("Presumably DAG has infinite dependency")
      }
      startLength = pipeInfo.pipe.length
      for (i <- 0 until startLength) {
        val comp = pipeInfo.pipe(i)
        addDefaultOutputs(comp)
        addDefaultInputs(comp)
        addSideComponents(comp)
      }
      endLength = pipeInfo.pipe.length
      numResolveIters -= 1
    }
    logger.debug("Done adding default nodes")
  }

  /**
    * Build pipeline form a JSON string
    *
    * @param jsonStr string to parse
    * @return (ReflexPipelineInfo, MutableList[ReflexDagNode])
    */
  def buildPipelineFromJson(jsonStr: String, testMode: Boolean = false): ReflexPipelineDag = {
    require(jsonStr != null, "Provided parameter is nullNo or empty JSON was provided to parse")
    require(jsonStr != "", "Empty JSON is provided to parse")
    buildPipelineFromInfo(new ReflexPipelineJsonParser().parsePipelineInfo(jsonStr), testMode)
  }

  /**
    * Build pipeline from ReflexPipelineInfo
    *
    * @param pipeInfoIn pipeInfo to build from
    * @return (ReflexPipelineInfo, MutableList[ReflexDagNode])
    */
  def buildPipelineFromInfo(pipeInfoIn: ReflexPipelineInfo, testMode: Boolean = false)
  : ReflexPipelineDag = {
    require(pipeInfoIn != null, "No or empty JSON was provided to parse")
    pipeInfo = pipeInfoIn

    var listOfNodes = new mutable.MutableList[ReflexDagNode]()

    /*
     Check uniqueness of component ids
    */
    for (compJSON <- pipeInfo.pipe) {
      if (componentsMap.contains(compJSON.id)) {
        throw new Exception(s"Pipeline parsing: Found duplicate id in node list ${compJSON.id}")
      }
      componentsMap(compJSON.id) = compJSON
    }

    /*
     Check that only valid ids are used in parents and
     that parents are in correct format
     */
    for (compJSON <- pipeInfo.pipe) {
      for (parent <- compJSON.parents) {
        require(componentsMap.contains(parent.parent), s"Pipeline does not contain component id: ${parent.parent}")
        incomingEdges += parent.parent.toString + parent.output.toString
      }
      /*
        'parents' section contains list of parents description.
        Field 'input' in Parent is optional, it shows index of input this parent connects to.
        If 'input' field is absent, parents are connected according to order in list.
        All parents in this list must contain or not contain 'input' field.
      */
      val inputs = compJSON.parents.map(_.input)
      if (inputs.exists(_.isEmpty)) {
        if (inputs.exists(_.isDefined)) {
          throw new Exception(s"Parent JSON is in mixed format - some parents without input index and some with: in ${compJSON.`type`}")
        }

        // add 'input' field to each parent
        for (i <- compJSON.parents.indices) {
          compJSON.parents(i).input = Some(i)
        }
      } else {
        if (inputs.size != inputs.distinct.size) {
          throw new Exception("More than one parent is connected to the same input index")
        }
        /*
           Sorting parents list by order of 'input' values,
           so from now parents are in correct order.
        */
        val parentsTmpList = compJSON.parents.sortBy(_.input.get)
        compJSON.parents.clear()
        parentsTmpList.foreach(compJSON.parents += _)
      }
    }

    // Resolve defaults
    addDefaultComponents()

    mergeSinkSingletons()
    mergeSourceSingletons()

    // Components creation
    var isUserStandalone = false
    var isMlStatsUsed = false
    for (compJSON <- pipeInfo.pipe) {
      logger.info("Component extracted, name: " + compJSON.name + ", type: " + compJSON.`type` + ", id: " + compJSON.id)

      val args = {
        compJSON.arguments match {
          case Some(value) => mutable.Map(value.toSeq: _*)
          case None => mutable.Map[String, Any]()
        }
      }

      ParserHelper.addSystemConfig(args, pipeInfo.systemConfig)
      val comp = ReflexComponentFactory(pipeInfo.engineType, compJSON.`type`, args.toMap[String, Any])
      if (comp.isUserStandalone) {
        isUserStandalone = true
      }

      if (comp.isMlStatsUsed) {
        isMlStatsUsed = true
      }
      var node = new ReflexDagNode(compJSON.name, compJSON.id, comp)

      addConnectionToSingletonComponent(comp, compJSON)
      if (compJSON.parents.nonEmpty) {
        addParents(node, compJSON)
      }
      listOfNodes += node
    }

    new ReflexPipelineDag(pipeInfo, listOfNodes, isUserStandalone, isMlStatsUsed)
  }
}
