package org.mlpiper.parameters.common

import java.nio.file.{Files, Paths}

trait NewFilePathParameter extends DefinedStringParameter {
  override val defaultValue = None
  override lazy val errorMessage = key + " must be not existing file in existing directory"

  override def condition(filePath: Option[String],
                         parameters: ArgumentParameterChecker)
  : Boolean = {!Files.exists(Paths.get(filePath.get)) &&
                Files.exists(Paths.get(filePath.get).getParent) &&
                Files.isWritable(Paths.get(filePath.get).getParent)
  }
}
