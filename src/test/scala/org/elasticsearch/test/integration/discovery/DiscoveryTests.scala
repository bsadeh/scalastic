package org.elasticsearch.test.integration.discovery

import org.elasticsearch.common.settings.ImmutableSettings._
import scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner]) 
class DiscoveryTests extends MultiNodesBasedTests {

  test("testUnicastDiscovery") {
    val settings = settingsBuilder
      .put("discovery.zen.multicast.enabled", false)
      .put("discovery.zen.unicast.hosts", "localhost")
      .build()
    startNode("node1", settings)
    startNode("node2", settings)
    indexer("node1").state().getState.nodes().size should equal (2)
    indexer("node2").state().getState.nodes().size should equal (2)
  }
}
