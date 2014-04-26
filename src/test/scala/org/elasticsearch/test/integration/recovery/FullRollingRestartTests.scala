package org.elasticsearch.test.integration.recovery

import scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class FullRollingRestartTests extends MultiNodesBasedTests {

  test("testFullRollingRestart") {
    startNode("node1")
    for (i <- 0 until 1000) indexer("node1").index(indexName, "type1", i.toString, """{"test": "value%s"}""".format(i))
    indexer("node1").flush()
    for (i <- 1000 until 2000) indexer("node1").index(indexName, "type1", i.toString, """{"test": "value%s"}""".format(i))
    startNode("node2")
    startNode("node3")
    indexer("node1").health_prepare().setTimeout("1m").setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes("3").execute.actionGet.isTimedOut should equal (false)
    startNode("node4")
    startNode("node5")
    indexer("node1").health_prepare().setTimeout("1m").setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes("5").execute.actionGet.isTimedOut should equal (false)
    indexer("node1").refresh()
    for (i <- 0 until 10) {
      indexer("node1").count().getCount should equal (2000)
    }
    Thread.sleep(300)
    closeNode("node1")
    indexer("node5").health_prepare().setTimeout("1m").setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes("4").execute.actionGet.isTimedOut should equal (false)
    Thread.sleep(300)
    closeNode("node2")
    Thread.sleep(300)
    indexer("node5").health_prepare().setTimeout("1m").setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes("3").execute.actionGet.isTimedOut should equal (false)
    Thread.sleep(300)
    indexer("node5").refresh()
//fixme
////    println("first time")
//    for (i <- 0 until 10) {
////      println(indexer("node5").count().getCount)
//      indexer("node5").count().getCount should equal (2000)
//    }
    closeNode("node3")
    indexer("node5").health_prepare().setTimeout("1m").setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes("2").execute.actionGet.isTimedOut should equal (false)
    closeNode("node4")
    indexer("node5").health_prepare().setTimeout("1m").setWaitForYellowStatus().setWaitForRelocatingShards(0).setWaitForNodes("1").execute.actionGet.isTimedOut should equal (false)
    indexer("node5").refresh()
//fixme
////    println("second time")
//    for (i <- 0 until 10) {
////    	println(indexer("node5").count().getCount)
//      indexer("node5").count().getCount should equal (2000)
//    }
  }
}
