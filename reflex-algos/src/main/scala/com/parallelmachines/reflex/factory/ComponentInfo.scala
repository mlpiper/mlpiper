package com.parallelmachines.reflex.factory

case class ComponentInfo(packageName: String,
                         canonicalName: String,
                         typeName: String,
                         classAvail: Boolean,
                         signature: String,
                         componentMetadata: Option[ComponentMetadata],
                         metadataFilename: Option[String] = None) {
  def isTransient(): Boolean = {
    if (componentMetadata.isDefined) {
      componentMetadata.get.isTransient()
    } else {
      false
    }
  }
}
