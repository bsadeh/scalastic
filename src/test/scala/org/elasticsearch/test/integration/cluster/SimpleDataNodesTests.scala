package org.elasticsearch.test.integration.cluster

import org.elasticsearch.common.settings.ImmutableSettings._
import org.elasticsearch.action._
import scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class SimpleDataNodesTests extends MultiNodesBasedTests {

  test("testDataNodes") {
    startNode("nonData1", settingsBuilder.put("node.data", false).build())
    indexer("nonData1").createIndex(indexName)
    try {
      indexer("nonData1").index(indexName, "type1", "1", source("1", "test"), timeout=Some("1s"))
      fail("no allocation should happen")
    } catch {
      case e: UnavailableShardsException =>
    }
    startNode("nonData2", settingsBuilder.put("node.data", false).build())
    indexer("nonData1").waitForNodes(howMany = "2").isTimedOut should be === (false)
    try {
      indexer("nonData2").index(indexName, "type1", "1", source("1", "test"), timeout=Some("1s"))
      fail("no allocation should happen")
    } catch {
      case e: UnavailableShardsException =>
    }
    startNode("data1", settingsBuilder.put("node.data", true).build())
    indexer("nonData1").waitForNodes(howMany = "3").isTimedOut should be === (false)
    val indexResponse = indexer("nonData2").index(indexName, "type1", "1", source("1", "test"))
    indexResponse.getId should be === ("1")
    indexResponse.getType should be === ("type1")
  }

  private def source(id: String, nameValue: String) = """{"type1": {"id": "%s", "name": "%s"}}""".format(id, nameValue)
}
