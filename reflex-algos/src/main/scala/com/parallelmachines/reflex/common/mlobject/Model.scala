package com.parallelmachines.reflex.common.mlobject

import com.parallelmachines.reflex.common.enums.ModelFormat

/**
  * Class for handling model objects
  */
class Model(name: String, format: ModelFormat, description: String = "", id: Option[String] = None) extends MLObject(MLObjectType.Model, id) {

  var data: Option[Array[Byte]] = None

  require(format != ModelFormat.UNKNOWN, "Model format can not be set to UNKNOWN")

  /**
    * Set data on Model object
    *
    * @param data byte data to set for a model
    * @return
    */
  def setData(data: Array[Byte]): Unit = {
    this.data = Some(data)
  }

  /**
    * Get model data
    *
    * @return Array[Byte] data
    */
  def getData: Option[Array[Byte]] = {
    data
  }

  /**
    * Get model format
    *
    * @return ModelFormat format
    */
  def getFormat: ModelFormat = {
    format
  }

  /**
    * Get model name
    *
    * @return String name
    */
  def getName: String = {
    name
  }

  /**
    * Get model description
    *
    * @return String description
    */
  def getDescription: String = {
    description
  }

  override def toString: String = {
    s"${super.toString} name: $getName; description: $getDescription; format: ${getFormat.toString}"
  }
}

object Model {
  def apply(name: String, format: ModelFormat, description: String = "", id: Option[String] = None): Model = {
    new Model(name, format, description, id)
  }
}