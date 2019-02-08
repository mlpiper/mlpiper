package org.mlpiper.parameters.common

import java.nio.file.{Files, Paths}

trait ExistingFilePathParameter extends StringParameter {
  override val defaultValue = None
  override lazy val errorMessage = key + " must be an existing file"

  override def condition(filePath: Option[String],
                         parameters: ArgumentParameterChecker)
  : Boolean = {
    filePath.isDefined && Files.exists(Paths.get(filePath.get))
  }
}

trait MaybeExistingFilePathParameter extends StringParameter {
  override val defaultValue = Some("")
  override lazy val errorMessage = key + " must be file name"

  override def condition(filePath: Option[String],
                         parameters: ArgumentParameterChecker)
  : Boolean = {
    filePath.isDefined
  }
}

trait ReadableFilePathParameter extends StringParameter {
  override val defaultValue = None
  override lazy val errorMessage = key + " must be an existing, readable file"

  override def condition(filePath: Option[String],
                         parameters: ArgumentParameterChecker)
  : Boolean = {
    filePath.isDefined && Files.isReadable(Paths.get(filePath.get))
  }
}

trait WritableFilePathParameter extends StringParameter {
  override val defaultValue = None
  override lazy val errorMessage = key + " must be a writable file"

  override def condition(filePath: Option[String],
                         parameters: ArgumentParameterChecker)
  : Boolean = {
    filePath.isDefined &&
      Files.exists(Paths.get(filePath.get).getParent) &&
      Files.isWritable(Paths.get(filePath.get).getParent)
  }
}
