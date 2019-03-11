package org.mlpiper.mlobject

import org.mlpiper.mlobject.MLObjectType.MLObjectType
import org.mlpiper.infrastructure.rest.RestApis

/**
  * Base class for MLObjects
  * Handles object types and IDs
  */
class MLObject(objType: MLObjectType, var id: Option[String] = None) {
  if (!id.isDefined) {
    id = Some(RestApis.generateUUID(objType))
  }

  require(id.isDefined, "MLObject id can not be left unassigned")
  /**
    * Get MLObject id
    * @return String object id
    */
  def getId: String = {
    id.get
  }
  /**
    * Get MLObject type
    * @return MLObjectType object type
    */
  def getType: MLObjectType = {
    objType
  }

  override def toString: String = {
    s"${this.getClass.getCanonicalName}; type: ${getType.toString};"
  }
}
