package com.traackr.util

import org.scalatest._, matchers._
import com.traackr.util.Conversions._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ConversionsSpec extends FlatSpec with MustMatchers {
  
  case class Fixture(text: String, number: Int)

  "an object" should "be converted to a map composed of it's public attributes" in {
    toMap(Fixture("the", 1)) must be === Map("text" -> "the", "number" -> 1)
  }

  "any object" should "be implicitly converted to a map when called" in {
    methodNeedingImplicitConversionToMap(Fixture("the", 1)) === Map("text" -> "the", "number" -> 1)
  }

  def methodNeedingImplicitConversionToMap(map: Map[String, AnyRef]): Map[String, AnyRef] = {
    map
  }
}