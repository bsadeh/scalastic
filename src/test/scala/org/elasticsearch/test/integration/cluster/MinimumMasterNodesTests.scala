package org.elasticsearch.test.integration.cluster

import org.elasticsearch.common.settings.ImmutableSettings._
import org.elasticsearch.action.admin.cluster.health._
import org.elasticsearch.discovery._
import org.elasticsearch.env._
import org.elasticsearch.gateway._
import org.elasticsearch.node.internal._
import scala.collection._, JavaConversions._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class MinimumMasterNodesTests extends AbstractZenNodesTests {

  override def beforeEach {
    super.beforeEach
    cleanAndCloseNodes()
  }

  def cleanAndCloseNodes() {
    for (i <- 0 until 10) {
      val nodeName = "node" + i
      if (indexer(nodeName) != null) {
        indexer(nodeName).stop()
        val injector = node(nodeName).asInstanceOf[InternalNode].injector
        if (injector.getInstance(classOf[NodeEnvironment]).hasNodeFile)
          injector.getInstance(classOf[Gateway]).reset()
      }
    }
  }

  test("simpleMinimumMasterNodes") {
    buildNode("node1", settingsBuilder.put("gateway.type", "local"))
    buildNode("node2", settingsBuilder.put("gateway.type", "local"))
    cleanAndCloseNodes()
    val settings = settingsBuilder.
      put("discovery.type", "zen").
      put("discovery.zen.minimum_master_nodes", 2).
      put("discovery.zen.ping_timeout", "200ms").
      put("discovery.initial_state_timeout", "500ms").
      put("gateway.type", "local").
      put("number_of_shards", 1).
      build()
    //logger.info("--> start first node")
    startNode("node1", settings)
    //logger.info("--> should be blocked, no master...")
    var state = indexer("node1").state(local = Some(true)).getState
    state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK) should equal (true)
    //logger.info("--> start second node, cluster should be formed")
    startNode("node2", settings)
    var clusterHealthResponse = indexer("node1").waitForNodes(howMany = "2")
    clusterHealthResponse.isTimedOut should equal (false)
    state = indexer("node1").state(local = Some(true)).getState
    state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK) should equal (false)
    state = indexer("node2").state(local = Some(true)).getState
    state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK) should equal (false)
    state = indexer("node1").state().getState
    state.nodes().size should equal (2)
    state.metaData().indices().containsKey(indexName) should equal (false)
    indexer("node1").createIndex(indexName)
    for (i <- 0 until 100) {
      indexer("node1").index(indexName, "type1", i.toString, """{"field": "value"}""")
    }
    indexer("node1").refresh()
    //logger.info("--> verify we the data back")
    for (i <- 0 until 10) {
      indexer("node1").count().getCount should equal (100)
    }
    var masterNodeName = state.nodes().masterNode().name()
    var nonMasterNodeName = if (masterNodeName == "node1") "node2" else "node1"
    closeNode(masterNodeName)
    Thread.sleep(200)
    state = indexer(nonMasterNodeName).state(local = Some(true)).getState
    state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK) should equal (true)
    //logger.info("--> starting the previous master node again...")
    startNode(masterNodeName, settings)
    clusterHealthResponse = indexer("node1").health_prepare().setWaitForYellowStatus()
      .setWaitForNodes("2").execute.actionGet
    clusterHealthResponse.isTimedOut should equal (false)
    state = indexer("node1").state(local = Some(true)).getState
    state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK) should equal (false)
    state = indexer("node2").state(local = Some(true)).getState
    state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK) should equal (false)
    state = indexer("node1").state().getState
    state.nodes().size should equal (2)
    state.metaData().indices().containsKey(indexName) should equal (true)
    var clusterHealth = indexer("node1").waitForGreenStatus()
    clusterHealth.isTimedOut should equal (false)
    clusterHealth.getStatus should equal (ClusterHealthStatus.GREEN)
    //logger.info("--> verify we the data back")
    for (i <- 0 until 10) {
      indexer("node1").count().getCount should equal (100)
    }
    masterNodeName = state.nodes().masterNode().name()
    nonMasterNodeName = if (masterNodeName == "node1") "node2" else "node1"
    closeNode(nonMasterNodeName)
    Thread.sleep(200)
    state = indexer(masterNodeName).state(local = Some(true)).getState
    state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK) should equal (true)
    //logger.info("--> starting the previous master node again...")
    startNode(nonMasterNodeName, settings)
    Thread.sleep(200)
    clusterHealthResponse = indexer("node1").health_prepare().setWaitForNodes("2")
      .setWaitForGreenStatus().execute.actionGet
    clusterHealthResponse.isTimedOut should equal (false)
    state = indexer("node1").state(local = Some(true)).getState
    state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK) should equal (false)
    state = indexer("node2").state(local = Some(true)).getState
    state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK) should equal (false)
    state = indexer("node1").state().getState
    state.nodes().size should equal (2)
    state.metaData().indices().containsKey(indexName) should equal (true)
    clusterHealth = indexer("node1").waitForGreenStatus()
    clusterHealth.isTimedOut should equal (false)
    clusterHealth.getStatus should equal (ClusterHealthStatus.GREEN)
    //logger.info("--> verify we the data back")
    for (i <- 0 until 10) {
      indexer("node1").count().getCount should equal (100)
    }
  }

  test("multipleNodesShutdownNonMasterNodes") {
    //logger.info("--> cleaning nodes")
    buildNode("node1", settingsBuilder.put("gateway.type", "local"))
    buildNode("node2", settingsBuilder.put("gateway.type", "local"))
    buildNode("node3", settingsBuilder.put("gateway.type", "local"))
    buildNode("node4", settingsBuilder.put("gateway.type", "local"))
    cleanAndCloseNodes()
    val settings = settingsBuilder.
      put("discovery.type", "zen").
      put("discovery.zen.minimum_master_nodes", 3).
      put("discovery.zen.ping_timeout", "200ms").
      put("discovery.initial_state_timeout", "500ms").
      put("gateway.type", "local").
      build()
    //logger.info("--> start first 2 nodes")
    startNode("node1", settings)
    startNode("node2", settings)
    Thread.sleep(500)
    var state = indexer("node1").state(local = Some(true)).getState
    state.getBlocks.hasGlobalBlock(Discovery.NO_MASTER_BLOCK) should equal (true)
    state = indexer("node2").state(local = Some(true)).getState
    state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK) should equal (true)
    //logger.info("--> start two more nodes")
    startNode("node3", settings)
    startNode("node4", settings)
    var clusterHealthResponse = indexer("node1").waitForNodes(howMany = "4")
    clusterHealthResponse.isTimedOut should equal (false)
    state = indexer("node1").state().getState
    state.nodes().size should equal (4)
    val masterNode = state.nodes().masterNode().name()
    val nonMasterNodes = new java.util.LinkedList[String]
    for (node <- state.nodes())
      if (node.name() != masterNode) nonMasterNodes.add(node.name)
    for (i <- 0 until 100) indexer("node1").index(indexName, "type1", i.toString, """{"field": "value"}""")
    indexer("node1").refresh()
    //logger.info("--> verify we the data back")
    for (i <- 0 until 10) indexer("node1").count().getCount should equal (100)

    var nodesToShutdown = new mutable.HashSet[String]
    nodesToShutdown.add(nonMasterNodes.removeLast())
    nodesToShutdown.add(nonMasterNodes.removeLast())
    //logger.info("--> shutting down two master nodes {}", nodesToShutdown)
    nodesToShutdown foreach (closeNode(_))
    Thread.sleep(1000)
    val lastNonMasterNodeUp = nonMasterNodes.removeLast()
    //logger.info("--> verify that there is no master anymore on remaining nodes")
    state = indexer(masterNode).state(local = Some(true)).getState
    state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK) should equal (true)
    state = indexer(lastNonMasterNodeUp).state(local = Some(true)).getState
    state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK) should equal (true)
    //logger.info("--> start back the nodes {}", nodesToShutdown)
    nodesToShutdown foreach (startNode(_, settings))
    clusterHealthResponse = indexer("node1").waitForNodes(howMany = "4")
    clusterHealthResponse.isTimedOut should equal (false)
    val clusterHealth = indexer("node1").waitForGreenStatus()
    clusterHealth.isTimedOut should equal (false)
    clusterHealth.getStatus should equal (ClusterHealthStatus.GREEN)
    state = indexer("node1").state().getState
    state.nodes().size should equal (4)
    //logger.info("--> verify we the data back")
    for (i <- 0 until 10) indexer("node1").count().getCount should equal (100)
  }
}
