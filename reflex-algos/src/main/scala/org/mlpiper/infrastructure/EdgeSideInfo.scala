package org.mlpiper.infrastructure

/**
  * Describing a side of an edge which is the node is + the connection id
  *
  * @param nodeIdArg       Id of node
  * @param connectionIdArg Id of connection (incoming or outgoing)
  */
class EdgeSideInfo(val nodeId: Int, val connectionId: Int) {

  override def toString: String = s"($nodeId : $connectionId)"
}
