package com.parallelmachines.reflex.components

import com.parallelmachines.reflex.components.ComponentAttribute.ComponentAttribute
import org.mlpiper.infrastructure.JsonHeaders

import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

object ComponentAttribute {

  def apply[T: TypeTag](key: String, value: T, label: String, desc: String, optional: Boolean = false): ComponentAttribute[T] =
    new ComponentAttribute(key, value, label, desc, optional)

  case class InvalidValueException(message: String) extends Exception(message)

  class ComponentAttribute[T: TypeTag](val key: String, var value: T, val label: String,
                                       val desc: String, val optional: Boolean) {

    /**
      * Following code is required to handle UI behavior
      * according to parameters type.
      *
      **/
    var uiType: Option[String] = None
    var uiOptions: Option[List[Map[String, String]]] = None
    private var validator: (T) => Boolean = (_: T) => true
    private var validateMsg: Option[String] = None

    def typeName(): String = {
      typeOf[T] match {
        case t if t =:= typeOf[Int] => "int"
        case t if t =:= typeOf[Long] => "long"
        case t if t =:= typeOf[String] => "string"
        case t if t =:= typeOf[Double] => "double"
        case t if t =:= typeOf[Boolean] => "boolean"
        case t if t <:< typeOf[Seq[Any]] => {
          s"sequence.${t.typeArgs(0).toString.toLowerCase}"
        }
        case _ => "unknown"
      }
    }

    def setValidator(other: (T) => Boolean, msg: Option[String] = None): ComponentAttribute[T] = {
      validator = other
      validateMsg = msg
      this
    }

    def setValue(paramMap: Map[String, Any]): Unit = {
      if (optional && !paramMap.exists(_._1 == key)) {
        if (!validator(value)) {
          val msg = validateMsg.getOrElse("Invalid attribute default value")
          throw new InvalidValueException(s"$msg key=$key, value=$value")
        }
        return
      }

      val v = typeOf[T] match {
        case t if t =:= typeOf[Int] => paramMap(key).asInstanceOf[Number].intValue()
        case t if t =:= typeOf[Long] => paramMap(key).asInstanceOf[Number].longValue()
        case t if t =:= typeOf[Double] => paramMap(key).asInstanceOf[Number].doubleValue()
        case _ => paramMap(key)
      }
      value = v.asInstanceOf[T]
      if (!validator(value)) {
        val msg = validateMsg.getOrElse("Invalid attribute configuration value")
        throw new InvalidValueException(s"$msg! key=$key, value=$value")
      }
    }

    /**
      * Defines parameter UI field to behave as drop down list
      **/
    def setOptions(options: List[(String, String)]): Unit = {
      uiOptions = Some(options.map { option => ListMap[String, String]("label" -> option._1, "value" -> option._2) })
      this.uiType = Some("select")
    }
  }
}

object ComponentAttributePack {

  def apply(attrs: ComponentAttribute[_]*): ComponentAttributePack = {
    new ComponentAttributePack(attrs.to[ArrayBuffer])
  }

  class ComponentAttributePack(attrs: ArrayBuffer[ComponentAttribute[_]]) {

    def add(additionalAttrs: ComponentAttribute[_]*): ComponentAttributePack = {
      attrs.append(additionalAttrs: _*)
      this
    }

    def configure(paramMap: Map[String, Any]): Unit = {
      for (attr <- attrs) {
        attr.setValue(paramMap)
      }
    }

    def toJsonable(): ArrayBuffer[ListMap[String, Any]] = {
      attrs.map { attr =>
        ListMap[String, Any](
          JsonHeaders.KeyHeader -> attr.key,
          JsonHeaders.TypeHeader -> attr.typeName(),
          JsonHeaders.LabelHeader -> attr.label,
          JsonHeaders.DescriptionHeader -> attr.desc,
          JsonHeaders.DefaultValueHeader -> attr.value,
          JsonHeaders.OptionalHeader -> attr.optional,
          JsonHeaders.UITypeHeader -> attr.uiType,
          "options" -> attr.uiOptions
        )
      }
    }
  }
}
