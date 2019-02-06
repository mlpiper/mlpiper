package org.apache.flink.streaming.test.exampleScalaPrograms.clustering

import org.apache.flink.streaming.scala.examples.common.parameters.common.{IntParameter, DoubleParameter}
import org.apache.flink.streaming.scala.examples.common.parameters.tools.{ArgumentParameterChecker, ArgumentParameterTool}
import org.junit.Test
import org.scalatest.Matchers

import scala.collection.mutable
import scala.util.{Failure, Try}

class ArgumentParameterTest extends Matchers{

  trait PositiveIntParameterNoDefault extends IntParameter {
    override val key: String
    final val defaultValue = None
    override val required: Boolean
    override val description: String
    override final val errorMessage: String = key + " must be greater than zero"

    override final def condition(x: Option[Int], parameters: ArgumentParameterChecker)
    : Boolean = {
      x.isDefined && x.get > 0
    }
  }

  trait PositiveDoubleParameterNoDefault extends DoubleParameter {
    override val key: String
    final val defaultValue = None
    override val required: Boolean
    override val description: String
    override final val errorMessage: String = key + " must be greater than zero"

    override final def condition(x: Option[Double], parameters: ArgumentParameterChecker)
    : Boolean = {
      x.isDefined && x.get > 0
    }
  }

  trait NonNegativeParameterDefault extends IntParameter {
    override val key: String
    final val defaultValue = Some(2017)
    override val required: Boolean
    override val description: String
    override final val errorMessage: String = key + " must be greater/equal zero"

    override final def condition(x: Option[Int], parameters: ArgumentParameterChecker)
    : Boolean = {
      x.isDefined && x.get >= 0
    }
  }

  object Test01P01Req extends PositiveDoubleParameterNoDefault {
    val key = "param01"
    val label = key
    val required = true
    val description = "Input vector number of attributes"
  }
  object Test01P02NotReq extends PositiveIntParameterNoDefault {
    val key = "param02"
    val label = key
    val required = false
    val description = "Input vector number of attributes"
  }
  object Test01P03NotReq extends NonNegativeParameterDefault {
    val key = "param03"
    val label = key
    val required = false
    val description = "Input vector number of attributes"
  }
  object Test01P04NotReq extends NonNegativeParameterDefault {
    val key = "param01"
    val label = key
    val required = false
    val description = "Input vector number of attributes"
  }

  /*
  Tests the following
  1. add one required parameter with a condition
  2. tests validity when parameter is not provided
  3. checks existence of parameter
  4. checks condition on the value
   */
  @Test def test01(): Unit = {
    val argTool01 = new ArgumentParameterTool("test01")
    argTool01.add(Test01P01Req)

    //required parameter is missing
    Try(argTool01.initializeParameters(Map[String, Any]("someRandom" -> 10.0))).isFailure should be(true)

    //value doesn't matter at this stage
    argTool01.initializeParameters(Map[String, Any]("param01" -> 10.0)) should be(true)
    argTool01.get(Test01P01Req) should be(10.0)


    // adding a not-required param
    argTool01.add(Test01P02NotReq)

    // check params
    argTool01.initializeParameters(Map[String, Any]("param01" -> 10.0, "param02" -> 20)) should be(true)
    argTool01.initializeParameters(Map[String, Any]("param01" -> 10.0)) should be(true)

    // param has no default set, so this will fail the condition
    Try(argTool01.get(Test01P02NotReq)).isFailure should be(true)

    // add a not-required param with default value
    argTool01.add(Test01P03NotReq)

    // param03 should get the default value because it was not provided
    argTool01.initializeParameters(Map[String, Any]("param01" -> 10.0)) should be(true)
    argTool01.get(Test01P03NotReq) should be(2017)

    // test parameter duplication
    Try(argTool01.add(Test01P04NotReq)).isFailure should be(true)
  }
}