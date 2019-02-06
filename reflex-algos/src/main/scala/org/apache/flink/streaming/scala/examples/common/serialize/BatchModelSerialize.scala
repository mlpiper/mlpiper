package org.apache.flink.streaming.scala.examples.common.serialize

import java.io.{FileOutputStream, ObjectOutputStream}

trait BatchModelSerialize[Model] {
  def writeModelToFile(model: Model,
                       path: String): Unit = {
    val file = new FileOutputStream(path)
    val oos = new ObjectOutputStream(file)

    oos.writeObject(model)
    oos.close()
    file.close()
  }

}
