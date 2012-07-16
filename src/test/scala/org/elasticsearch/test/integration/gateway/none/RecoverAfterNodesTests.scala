package org.elasticsearch.test.integration.gateway.none

import org.elasticsearch.common.settings.ImmutableSettings._
import org.elasticsearch.cluster.block._
import org.elasticsearch.gateway._
import com.traackr.scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner]) class RecoverAfterNodesTests extends MultiNodesBasedTests {

  override def afterEach = closeAllNodes()

  test("testRecoverAfterNodes") {
    //logger.info("--> start node (1)")
    startNode("node1", settingsBuilder.put("gateway.recover_after_nodes", 3))
    indexer("node1").state(local = Some(true)).state.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    //logger.info("--> start node (2)")
    startNode("node2", settingsBuilder.put("gateway.recover_after_nodes", 3))
    indexer("node1").state(local = Some(true)).state.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    indexer("node2").state(local = Some(true)).state.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    //logger.info("--> start node (3)")
    startNode("node3", settingsBuilder.put("gateway.recover_after_nodes", 3))
    indexer("node1").state(local = Some(true)).state.blocks().global(ClusterBlockLevel.METADATA).isEmpty should be === (true)
    indexer("node2").state(local = Some(true)).state.blocks().global(ClusterBlockLevel.METADATA).isEmpty should be === (true)
    indexer("node3").state(local = Some(true)).state.blocks().global(ClusterBlockLevel.METADATA).isEmpty should be === (true)
  }

  test("testRecoverAfterMasterNodes") {
    //logger.info("--> start master_node (1)")
    startNode("master1", settingsBuilder
      .put("gateway.recover_after_master_nodes", 2)
      .put("node.data", false)
      .put("node.master", true))
    indexer("master1").state(local = Some(true)).state.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    //logger.info("--> start data_node (1)")
    startNode("data1", settingsBuilder
      .put("gateway.recover_after_master_nodes", 2)
      .put("node.data", true)
      .put("node.master", false))
    indexer("master1").state(local = Some(true)).state.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    indexer("data1").state(local = Some(true)).state.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    //logger.info("--> start data_node (2)")
    startNode("data2", settingsBuilder
      .put("gateway.recover_after_master_nodes", 2)
      .put("node.data", true)
      .put("node.master", false))
    indexer("master1").state(local = Some(true)).state.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    indexer("data1").state(local = Some(true)).state.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    indexer("data2").state(local = Some(true)).state.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    //logger.info("--> start master_node (2)")
    startNode("master2", settingsBuilder
      .put("gateway.recover_after_master_nodes", 2)
      .put("node.data", false)
      .put("node.master", true))
    Thread.sleep(300)
    indexer("master1").state(local = Some(true)).state.blocks().global(ClusterBlockLevel.METADATA).isEmpty should be === (true)
    indexer("master2").state(local = Some(true)).state.blocks().global(ClusterBlockLevel.METADATA).isEmpty should be === (true)
    indexer("data1").state(local = Some(true)).state.blocks().global(ClusterBlockLevel.METADATA).isEmpty should be === (true)
    indexer("data2").state(local = Some(true)).state.blocks().global(ClusterBlockLevel.METADATA).isEmpty should be === (true)
  }

  test("testRecoverAfterDataNodes") {
    //logger.info("--> start master_node (1)")
    startNode("master1", settingsBuilder
      .put("gateway.recover_after_data_nodes", 2)
      .put("node.data", false)
      .put("node.master", true))
    indexer("master1").state(local = Some(true)).state.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    //logger.info("--> start data_node (1)")
    startNode("data1", settingsBuilder
      .put("gateway.recover_after_data_nodes", 2)
      .put("node.data", true)
      .put("node.master", false))
    indexer("master1").state(local = Some(true)).state.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    indexer("data1").state(local = Some(true)).state.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    //logger.info("--> start master_node (2)")
    startNode("master2", settingsBuilder
      .put("gateway.recover_after_data_nodes", 2)
      .put("node.data", false)
      .put("node.master", true))
    indexer("master1").state(local = Some(true)).state.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    indexer("data1").state(local = Some(true)).state.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    indexer("master2").state(local = Some(true)).state.blocks().global(ClusterBlockLevel.METADATA) should contain(GatewayService.STATE_NOT_RECOVERED_BLOCK)
    //logger.info("--> start data_node (2)")
    startNode("data2", settingsBuilder
      .put("gateway.recover_after_data_nodes", 2)
      .put("node.data", true)
      .put("node.master", false))
    indexer("master1").state(local = Some(true)).state.blocks().global(ClusterBlockLevel.METADATA).isEmpty should be === (true)
    indexer("master2").state(local = Some(true)).state.blocks().global(ClusterBlockLevel.METADATA).isEmpty should be === (true)
    indexer("data1").state(local = Some(true)).state.blocks().global(ClusterBlockLevel.METADATA).isEmpty should be === (true)
    indexer("data2").state(local = Some(true)).state.blocks().global(ClusterBlockLevel.METADATA).isEmpty should be === (true)
  }
}
