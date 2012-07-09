package org.elasticsearch.test.integration.cluster

import org.scalatest._, matchers._
import org.elasticsearch.common.settings.ImmutableSettings._
import org.elasticsearch.discovery._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner]) 
class SpecificMasterNodesTests extends AbstractZenNodesTests {

  test("simpleOnlyMasterNodeElection") {
    //logger.info("--> start data node / non master node")
    startNode("data1", settingsBuilder()
      .put("node.data", true)
      .put("node.master", false)
      .put("discovery.initial_state_timeout", "1s"))
    try {
      indexer("data1").state(timeout = Some("100ms")).state().nodes().masterNodeId() should be(null)
      fail("should not be able to find master")
    } catch {
      case e: MasterNotDiscoveredException =>
    }
    //logger.info("--> start master node")
    startNode("master1", settingsBuilder()
      .put("node.data", false)
      .put("node.master", true))
    indexer("data1").state().state.nodes().masterNode().name() should be === ("master1")
    indexer("master1").state().state.nodes().masterNode().name() should be === ("master1")
    //logger.info("--> stop master node")
    closeNode("master1")
    try {
      indexer("data1").state(timeout = Some("100ms")).state().nodes().masterNodeId() should be(null)
      fail("should not be able to find master")
    } catch {
      case e: MasterNotDiscoveredException =>
    }
    //logger.info("--> start master node")
    startNode("master1", settingsBuilder()
      .put("node.data", false)
      .put("node.master", true))
    indexer("data1").state().state.nodes().masterNode().name() should be === ("master1")
    indexer("master1").state().state.nodes().masterNode().name() should be === ("master1")
    //logger.info("--> stop all nodes")
    closeNode("data1")
    closeNode("master1")
  }

  test("electOnlyBetweenMasterNodes") {
    //logger.info("--> start data node / non master node")
    startNode("data1", settingsBuilder()
      .put("node.data", true)
      .put("node.master", false)
      .put("discovery.initial_state_timeout", "1s"))
    try {
      indexer("data1").state(timeout = Some("100ms")).state().nodes().masterNodeId() should be(null)
      fail("should not be able to find master")
    } catch {
      case e: MasterNotDiscoveredException =>
    }
    //logger.info("--> start master node (1)")
    startNode("master1", settingsBuilder()
      .put("node.data", false)
      .put("node.master", true))
    indexer("data1").state().state.nodes().masterNode().name() should be === ("master1")
    indexer("master1").state().state.nodes().masterNode().name() should be === ("master1")
    //logger.info("--> start master node (2)")
    startNode("master2", settingsBuilder()
      .put("node.data", false)
      .put("node.master", true))
    indexer("data1").state().state.nodes().masterNode().name() should be === ("master1")
    indexer("master1").state().state.nodes().masterNode().name() should be === ("master1")
    indexer("master2").state().state.nodes().masterNode().name() should be === ("master1")
    closeNode("master1")
    indexer("data1").state().state.nodes().masterNode().name() should be === ("master2")
    indexer("master2").state().state.nodes().masterNode().name() should be === ("master2")
  }
}
