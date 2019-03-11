package org.mlpiper.infrastructure

import scala.collection.mutable.ArrayBuffer

/**
  * A node in the DAG
  *
  * @param name      Name of node (human readable)
  * @param id        Unique id of node
  * @param component Component this node contains
  */
class ReflexDagNode(var name: String, var id: Int, var component: ReflexPipelineComponent) {
  var compType: String = ""
  var incomingEdges: ArrayBuffer[EdgeSideInfo] = new ArrayBuffer[EdgeSideInfo]()
  var outgoingEdges: ArrayBuffer[EdgeSideInfo] = new ArrayBuffer[EdgeSideInfo]()

  var outputDs = new ArrayBuffer[DataWrapperBase](0)
  var inputDs = new ArrayBuffer[DataWrapperBase](0)

  var tempMark: Boolean = false // used for the topological sort
  var permMark: Boolean = false // used for the topological sort of the node

  override def toString: String = "(n: " + name + ", id: " + id + ", p: " + incomingEdges + ")"

  def addParent(p: EdgeSideInfo): Unit = {
    incomingEdges += p
  }

  /** Generate the DAG nodes of the flow engine (e.g. Flink)
    *
    * @param env   Flink environment
    * @param dsArr Array of DataStream[Any]
    * @return
    */
  def materialize(env: EnvWrapperBase, dsArr: ArrayBuffer[DataWrapperBase]): ArrayBuffer[DataWrapperBase] = {
    val errPrefix = s"$id:$name: "
    component.materialize(env, dsArr, errPrefix)
  }
}
