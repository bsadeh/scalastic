package org.elasticsearch.test.integration.cluster.allocation

import org.elasticsearch.common.settings.ImmutableSettings._
import scala.collection.JavaConversions._
import com.traackr.scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner]) 
class FilteringAllocationTests extends MultiNodesBasedTests {

  test("testDecommissionNodeNoReplicas") {
    startNode("node1")
    startNode("node2")
    indexer("node1").createIndex(indexName, settings = Map("number_of_replicas" -> "0"))
    var clusterHealthResponse = indexer("node1").waitForGreenStatus()
    clusterHealthResponse.timedOut() should be === (false)
    //logger.info("--> index some data")
    for (i <- 0 until 100) {
      indexer("node1").index(indexName, "type", i.toString, """{"field": "value%s"}""".format(i))
    }
    indexer("node1").refresh()
    indexer("node1").count().count should be === (100)
    //logger.info("--> decommission the second node")
    indexer("node1").client.admin().cluster().prepareUpdateSettings()
      .setTransientSettings(settingsBuilder.put("cluster.routing.allocation.exclude._name", "node2"))
      .execute.actionGet
    Thread.sleep(200)
    clusterHealthResponse = indexer("node1").health_prepare().setWaitForGreenStatus()
      .setWaitForRelocatingShards(0).execute.actionGet
    clusterHealthResponse.timedOut() should be === (false)
    //logger.info("--> verify all are allocated on node1 now")
    val clusterState = indexer("node1").state().state
    for (
      indexRoutingTable <- clusterState.routingTable();
      indexShardRoutingTable <- indexRoutingTable;
      shardRouting <- indexShardRoutingTable
    ) {
      clusterState.nodes().get(shardRouting.currentNodeId()).name() should be === ("node1")
    }
    indexer("node1").refresh()
    indexer("node1").count().count should be === (100)
  }
}
