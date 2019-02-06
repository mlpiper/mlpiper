package com.parallelmachines.reflex.common.mlobject

import com.parallelmachines.reflex.common.enums.ModelFormat

/**
  * Class for handling model objects
  */
class Model(name: String, format: ModelFormat, description: String = "", id: Option[String] = None) extends MLObject(MLObjectType.Model, id) {

  var data: String = ""

  require(format != ModelFormat.UNKNOWN, "Model format can not be set to UNKNOWN")

  /**
    * Set data on Model object
    *
    * @param data   data to set for a model
    * @return
    */
  def set_data(data: String): Unit = {
    this.data = data
  }

  /**
    * Get model data
    *
    * @return String data
    */
  def get_data: String = {
    data
  }

  /**
    * Get model format
    *
    * @return ModelFormat format
    */
  def get_format: ModelFormat = {
    format
  }

  /**
    * Get model name
    *
    * @return String name
    */
  def get_name: String = {
    name
  }

  /**
    * Get model description
    *
    * @return String description
    */
  def get_description: String = {
    description
  }

  override def toString: String = {
    s"${super.toString} name: $get_name; description: $get_description; format: ${get_format.toString}"
  }
}

object Model {
  def apply(name: String, format: ModelFormat, description: String = "", id: Option[String] = None): Model = {
    new Model(name, format, description, id)
  }
}