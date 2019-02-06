package com.parallelmachines.reflex.pipeline

import scala.collection.mutable
import org.slf4j.LoggerFactory

/**
  * A DAG class - containing all the nodes and all the info that the dag has
  *
  * @param pipeInfo General info about the pipe
  * @param nodeList List of components to include
  */
class ReflexPipelineDag(val pipeInfo: ReflexPipelineInfo, var nodeList: mutable.MutableList[ReflexDagNode],
                        val isUserStandalone: Boolean, val isMlStatsUsed: Boolean) {

  val pipelineName = "ReflexPipeline"
  val nodeMap = scala.collection.mutable.Map.empty[Int, ReflexDagNode]
  private var sortedNodeList: mutable.MutableList[ReflexDagNode] = new mutable.MutableList[ReflexDagNode]()
  private val logger = LoggerFactory.getLogger(classOf[ReflexPipelineDag])

  if (nodeList.isEmpty) {
    throw new Exception("Node list is empty - can not create a DAG without nodes")
  }

  // Creating the nodeMap hash
  for (node <- nodeList) {
    nodeMap(node.id) = node
  }

  logger.debug("Node List:")
  logger.debug("\n" + nodeList.mkString("\n"))
  logger.debug("Node Map:")
  logger.debug("\n" + nodeMap.mkString("\n"))

  setOutgoingEdges()
  validate()

  def printDag(): Unit = {
    for (node <- nodeList) {
      println(node)
    }
  }

  /**
    * Setting the outgoing edges of the graph nodes
    */
  private def setOutgoingEdges(): Unit = {
    logger.debug("Setting outgoing edges")

    val outEdgesCount = scala.collection.mutable.Map.empty[Int, Int]

    // The first loop is to count the number of outgoing elements so we can allocate the array size
    for (nodeI <- nodeList) {
      for (incomingSide <- nodeI.incomingEdges) {
        if (nodeMap.contains(incomingSide.nodeId)) {
          val nn = nodeMap(incomingSide.nodeId)
          nn.outgoingEdges += null
          if (outEdgesCount.contains(incomingSide.nodeId)) {
            outEdgesCount(incomingSide.nodeId) += 1
          } else {
            outEdgesCount(incomingSide.nodeId) = 1
          }
        } else {
          throw new Exception(s"Graph is not valid -> incoming node [${
            incomingSide.nodeId
          }] not exists")
        }
      }
    }

    logger.debug("Setting outgoing edges content")
    for (node <- nodeList) {
      logger.debug("Node: " + node.id)
      for (incomingIdx <- node.incomingEdges.indices) {

        val incomingSide = node.incomingEdges(incomingIdx)
        logger.debug("Handling incoming side: " + incomingSide)
        val outNode = nodeMap(incomingSide.nodeId)
        outNode.outgoingEdges(incomingSide.connectionId) = new EdgeSideInfo(node.id, incomingIdx)
      }
    }

    logger.debug("Outgoing edges:")
    for (node <- nodeList) {
      logger.debug("node: " + node.id + " outgoing: " + node.outgoingEdges.toString())
    }
    logger.debug("Done setting outgoing edges - all is ok")
  }

  /**
    * Validate a given parent of a node.
    * This method is doing only DAG structure checks, no type checking is done here
    *
    * @param node      The node to use
    * @param parentIdx The parent index to validate
    * @return a tuple of (true/false, errMsg)
    */
  private def validateParent(node: ReflexDagNode, parentIdx: Int): Option[String] = {

    val parentInfo = node.incomingEdges(parentIdx)
    val parentNode = nodeMap(parentInfo.nodeId)

    val inputInfo = node.component.inputTypes

    // Case where the node is getting more inputs than it should get
    // Todo: this check can be moved to be inside the component - so component like Union can allow any number
    // of inputs
    if (inputInfo.length <= parentIdx) {
      val err = "Node " + node.id + " is supposed to get " + inputInfo.length + " inputs, but got more"
      logger.error(err)
      return Some(err)
    }

    // Checking that the parent has the output id
    // Todo: this check can also be passed to the component class
    val parentOutputTypes = parentNode.component.outputTypes
    if (parentInfo.connectionId >= parentOutputTypes.length) {
      var err = "Parent node: " + parentInfo.nodeId + " does not contain output id " + parentInfo.connectionId
      err += " parentOutputTypes.length " + parentOutputTypes.length
      logger.error(err)
      return Some(err)
    }

    None
  }

  /**
    * Iterating over the sorted nodes and feeding the types to each component validateAndPropagateIncomingTypes function
    */
  private def validateConnectionTypes(): Unit = {

    for (node <- sortedNodeList) {
      logger.debug(s"Validating types for node id: ${
        node.id
      } name: ${
        node.name
      }")
      val incomingTypes: ConnectionList = ConnectionList.empty()
      for (parent <- node.incomingEdges) {
        incomingTypes += nodeMap(parent.nodeId).component.outputTypes(parent.connectionId)

      }
      node.component.validateAndPropagateIncomingTypes(incomingTypes)
    }
  }

  private def validateNodesConnections(): Unit = {
    logger.debug("Checking edges type compatibility")
    for (node <- nodeList) {

      // Checking that the inputs and output numbers are enough
      val nodeInputInfo = node.component.inputTypes
      if (nodeInputInfo.length != node.incomingEdges.length) {
        val msg = "Node " + node.id + " inputs number is incorrect - expecting " + nodeInputInfo.length + " got " + node.incomingEdges.length
        logger.error(msg)
        throw new Exception(msg)
      }

      val nodeOutputInfo = node.component.outputTypes
      if (nodeOutputInfo.length != node.outgoingEdges.length) {
        val msg = "Node " + node.id + " output number is not correct. Expecting: " + nodeOutputInfo.length +
          " got: " + node.outgoingEdges.length
        logger.error(msg)
        throw new Exception(msg)
      }

      // Checking each parent
      for (parentIdx <- node.incomingEdges.indices) {
        val parent = node.incomingEdges(parentIdx)
        if (nodeMap.contains(parent.nodeId)) {

          val res = validateParent(node, parentIdx)
          res match {
            case Some(error) => throw new Exception(error)
            case _ =>
          }

        } else {
          val msg = "Node " + node.id + " input: " + parent + " does not exists"
          logger.error(msg)
          throw new Exception(msg)
        }
      }
    }
  }

  /**
    * Helper for topological sort
    *
    * @return a node which is unmarked, otherwise (if no such node is found) return -1
    */
  private def findUnmarkedNode(): Int = {
    for (node <- nodeList) {
      if (!node.permMark) {
        return node.id
      }
    }
    -1
  }

  /**
    * Perform topological sort on the node - if a loop is detected throw exception
    */
  private def topologicalSort(): Unit = {
    logger.debug("Doing topological sort")

    def visit(nodeId: Int): Unit = {
      logger.debug(s"Visiting node $nodeId")

      val node: ReflexDagNode = nodeMap(nodeId)
      if (node.tempMark) {
        logger.error("Detected tempMark --- Loop")
        throw new Exception("Dag contains loop")
      }
      if (!node.permMark) {
        node.tempMark = true

        for (edgeSide <- node.outgoingEdges) {
          logger.debug("\tAbout to call visit on " + edgeSide)
          visit(edgeSide.nodeId)
        }

        node.permMark = true
        node.tempMark = false
        // Note: the line below PREPEND to the list (as opposed to appending to it)
        sortedNodeList.+=:(node)
      }
    }

    var unmarkedNodeId: Int = findUnmarkedNode()

    while (unmarkedNodeId != -1) {
      logger.debug(s"Found unmarked $unmarkedNodeId")
      visit(unmarkedNodeId)
      unmarkedNodeId = findUnmarkedNode()
    }

    logger.debug("Done topological sort")
    if (logger.isInfoEnabled) {
      for (sn <- sortedNodeList) {
        logger.debug("id: " + sn.id)
      }
    }
  }

  /**
    * Validate the entire DAG
    *
    * @return a tuple of (Bool, String) where the bool is true if the DAG is valid, if not the String contains the err
    */
  private def validate(): Unit = {
    logger.debug(s"Validating Dag")

    if (nodeList.length != nodeMap.size) {
      throw new Exception("Provided node list contains duplicate id")
    }
    validateNodesConnections()
    topologicalSort()
    validateConnectionTypes()
    // TODO: make sure nothing is pointing to src nodes
    // TODO: make sure internal nodes inputs + outputs are covered
    // TODO: make sure sink nodes does not point to anything.
  }

  /**
    * Check if a given edge is valid - do not assume the edge already exists in the graph
    * This is to be used to check if adding such an edge would be a legal action
    *
    * @param edge Describing the Edge in the DAG to check
    * @return
    */
  def checkEdgeValidity(edge: DagEdge): (Boolean, String) = {
    if (!nodeMap.contains(edge.uId)) {
      return (false, "Error: parent node " + edge.uId + " is not part of the DAG")
    }
    if (!nodeMap.contains(edge.vId)) {
      return (false, "Error: child node " + edge.vId + " is not part of the DAG")
    }
    (true, "")
  }

  /**
    * Check if an edge between 2 components is possible (the graph does not have to contain the nodes)
    *
    * @return
    */
  def checkPossibleEdgeValidity(): (Boolean, String) = {
    (false, "Edge is not valid")
  }

  /**
    * Setting the node input data streams. Assuming all the parents of this node already called
    * materialize and these nodes output datastreams are populated
    *
    * @param nodeId The id of the node to set input DS for
    */
  private def setNodeInputDS(nodeId: Int): Unit = {
    val node: ReflexDagNode = nodeMap(nodeId)
    for (parentIdx <- node.incomingEdges.indices) {
      val parent: EdgeSideInfo = node.incomingEdges(parentIdx)
      val parentNode = nodeMap(parent.nodeId)

      node.inputDs += parentNode.outputDs(parent.connectionId)
    }
  }

  /**
    * Interact with the compute engine and generate the dag in the underline engine
    *
    * @param env Engine specific info
    */
  def materialize(env: EnvWrapperBase): Unit = {

    for (i <- sortedNodeList.indices) {
      val nrParents = sortedNodeList(i).incomingEdges.length
      val id = sortedNodeList(i).id
      logger.debug(s"Generating dag element: $i parents num: $nrParents  id: $id")

      for (parentIdx <- sortedNodeList(i).incomingEdges.indices) {
        val parentId = sortedNodeList(i).incomingEdges(parentIdx)
        logger.debug(s"p $parentIdx: $parentId")
      }

      setNodeInputDS(id)
      sortedNodeList(i).outputDs = sortedNodeList(i).materialize(env, sortedNodeList(i).inputDs)
    }
  }
}
