package org.mlpiper.parameters.io.hdfs

import org.mlpiper.parameters.common.DefinedStringParameter

trait Hostname extends DefinedStringParameter {
  override val defaultValue: Option[String] = Some("localhost")
}
