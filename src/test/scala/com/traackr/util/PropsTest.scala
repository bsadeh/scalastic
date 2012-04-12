package com.traackr.util

import org.scalatest._, matchers._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class PropsTest extends FunSuite with ShouldMatchers {
  import java.io._
  import Preamble._

  test("read properties") {
    val filename = "target/test.properties"
    new File(filename).delete
    val defenitions = Seq("one.value=1", "second.value=other")
    using(new FileWriter(filename)) { writer =>
      defenitions foreach { writer.append(_).append("\n") }
    }

    val properties = Props(filename)
    properties("one.value") should be === "1"
    properties("second.value") should be === "other"
  }

}