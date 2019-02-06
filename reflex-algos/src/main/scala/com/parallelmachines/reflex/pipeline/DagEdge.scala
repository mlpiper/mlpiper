package com.parallelmachines.reflex.pipeline

/**
  * An edge in the dag
  *
  * @param uId       u id
  * @param uOutputId output
  * @param vId       v id
  * @param vInputId  input
  */
// TODO: change to include 2 sides
class DagEdge(var uId: Int, var uOutputId: Int, var vId: Int, var vInputId: Int) {

    override def toString: String = s"($uId : $uOutputId) -> ($vId : $vInputId)"
}
