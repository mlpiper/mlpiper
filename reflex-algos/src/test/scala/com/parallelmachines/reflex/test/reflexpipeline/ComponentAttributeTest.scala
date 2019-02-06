package com.parallelmachines.reflex.test.reflexpipeline

import org.scalatest.FlatSpec
import org.json4s.jackson.JsonMethods._
import com.parallelmachines.reflex.components.{ComponentAttribute, ComponentAttributePack}
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.slf4j.LoggerFactory

@RunWith(classOf[JUnitRunner])
class ComponentAttributeTest extends FlatSpec {

  private val logger = LoggerFactory.getLogger(getClass)

  "Test-1" should "be valid" in {
    val intArg = ComponentAttribute("int-arg", 100, "int-label", "Int description")
    val strArg = ComponentAttribute("string-arg", value = "Hi", "string-label", "String description", optional = true)

    val paramArgs = Map("int-arg" -> 101, "some-arg" -> "hi")
    intArg.setValue(paramArgs)
    strArg.setValue(paramArgs)

    val attrPack = ComponentAttributePack(intArg, strArg)

    val json = Json(DefaultFormats).write(attrPack.toJsonable())
    logger.info(s"Test-1:\n ${pretty(render(parse(json)))}")

    Json(DefaultFormats).write(attrPack.toJsonable())
  }


  "Test-2" should "throw InvalidAttributeValue with generic message" in {
    val intArg = ComponentAttribute("int-arg", 100, "int-label", "Int description")
      .setValidator(_ >= 103)

    val paramArgs = Map("int-arg" -> 101, "some-arg" -> "hi")

    val ex = intercept[ComponentAttribute.InvalidValueException] {
      intArg.setValue(paramArgs)
    }
    assert(ex.message.contains("Invalid attribute configuration value"))

    val attrPack = ComponentAttributePack(intArg)
    val json = Json(DefaultFormats).write(attrPack.toJsonable())
    logger.debug(compact(render(parse(json))))
  }

  "Test-3" should "throw InvalidAttributeValue" in {
    val exMsg = "Should be equal or greater then 103"
    val intArg = ComponentAttribute("int-arg", 100, "int-label", "Int description")
      .setValidator(_ >= 103, Some(exMsg))

    val paramArgs = Map("int-arg" -> 101, "some-arg" -> "hi")

    val ex = intercept[ComponentAttribute.InvalidValueException] {
      intArg.setValue(paramArgs)
    }
    assert(ex.message.contains(exMsg))

    val attrPack = ComponentAttributePack(intArg)
    val json = Json(DefaultFormats).write(attrPack.toJsonable())
    logger.debug(compact(render(parse(json))))
  }
}
