package org.elasticsearch.test.integration.gateway.none

import org.elasticsearch.common.settings.ImmutableSettings._
import org.elasticsearch.cluster.block._
import org.elasticsearch.gateway._
import scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner]) class RecoverAfterNodesTests extends MultiNodesBasedTests {

  override def afterEach() { closeAllNodes() }

  test("testRecoverAfterNodes") {
    //logger.info("--> start node (1)")
    startNode("node1", settingsBuilder.put("gateway.recover_after_nodes", 3))
    indexer("node1").state(local = Some(true)).getState.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    //logger.info("--> start node (2)")
    startNode("node2", settingsBuilder.put("gateway.recover_after_nodes", 3))
    indexer("node1").state(local = Some(true)).getState.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    indexer("node2").state(local = Some(true)).getState.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    //logger.info("--> start node (3)")
    startNode("node3", settingsBuilder.put("gateway.recover_after_nodes", 3))
    Thread.sleep(300) // wait a sec for recovery!
    indexer("node1").state(local = Some(true)).getState.blocks().global(ClusterBlockLevel.METADATA).isEmpty should equal (true)
    indexer("node2").state(local = Some(true)).getState.blocks().global(ClusterBlockLevel.METADATA).isEmpty should equal (true)
    indexer("node3").state(local = Some(true)).getState.blocks().global(ClusterBlockLevel.METADATA).isEmpty should equal (true)
  }

  test("testRecoverAfterMasterNodes") {
    //logger.info("--> start master_node (1)")
    startNode("master1", settingsBuilder
      .put("gateway.recover_after_master_nodes", 2)
      .put("node.data", false)
      .put("node.master", true))
    indexer("master1").state(local = Some(true)).getState.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    //logger.info("--> start data_node (1)")
    startNode("data1", settingsBuilder
      .put("gateway.recover_after_master_nodes", 2)
      .put("node.data", true)
      .put("node.master", false))
    indexer("master1").state(local = Some(true)).getState.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    indexer("data1").state(local = Some(true)).getState.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    //logger.info("--> start data_node (2)")
    startNode("data2", settingsBuilder
      .put("gateway.recover_after_master_nodes", 2)
      .put("node.data", true)
      .put("node.master", false))
    indexer("master1").state(local = Some(true)).getState.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    indexer("data1").state(local = Some(true)).getState.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    indexer("data2").state(local = Some(true)).getState.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    //logger.info("--> start master_node (2)")
    startNode("master2", settingsBuilder
      .put("gateway.recover_after_master_nodes", 2)
      .put("node.data", false)
      .put("node.master", true))
    Thread.sleep(300)
    indexer("master1").state(local = Some(true)).getState.blocks().global(ClusterBlockLevel.METADATA).isEmpty should equal (true)
    indexer("master2").state(local = Some(true)).getState.blocks().global(ClusterBlockLevel.METADATA).isEmpty should equal (true)
    indexer("data1").state(local = Some(true)).getState.blocks().global(ClusterBlockLevel.METADATA).isEmpty should equal (true)
    indexer("data2").state(local = Some(true)).getState.blocks().global(ClusterBlockLevel.METADATA).isEmpty should equal (true)
  }

  test("testRecoverAfterDataNodes") {
    //logger.info("--> start master_node (1)")
    startNode("master1", settingsBuilder
      .put("gateway.recover_after_data_nodes", 2)
      .put("node.data", false)
      .put("node.master", true))
    indexer("master1").state(local = Some(true)).getState.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    //logger.info("--> start data_node (1)")
    startNode("data1", settingsBuilder
      .put("gateway.recover_after_data_nodes", 2)
      .put("node.data", true)
      .put("node.master", false))
    indexer("master1").state(local = Some(true)).getState.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    indexer("data1").state(local = Some(true)).getState.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    //logger.info("--> start master_node (2)")
    startNode("master2", settingsBuilder
      .put("gateway.recover_after_data_nodes", 2)
      .put("node.data", false)
      .put("node.master", true))
    indexer("master1").state(local = Some(true)).getState.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    indexer("data1").state(local = Some(true)).getState.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    indexer("master2").state(local = Some(true)).getState.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    //logger.info("--> start data_node (2)")
    startNode("data2", settingsBuilder
      .put("gateway.recover_after_data_nodes", 2)
      .put("node.data", true)
      .put("node.master", false))
    Thread.sleep(300)
    indexer("master1").state(local = Some(true)).getState.blocks().global(ClusterBlockLevel.METADATA).isEmpty should equal (true)
    indexer("master2").state(local = Some(true)).getState.blocks().global(ClusterBlockLevel.METADATA).isEmpty should equal (true)
    indexer("data1").state(local = Some(true)).getState.blocks().global(ClusterBlockLevel.METADATA).isEmpty should equal (true)
    indexer("data2").state(local = Some(true)).getState.blocks().global(ClusterBlockLevel.METADATA).isEmpty should equal (true)
  }
}
