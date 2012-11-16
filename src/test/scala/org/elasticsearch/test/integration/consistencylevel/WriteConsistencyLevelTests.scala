package org.elasticsearch.test.integration.consistencylevel

import org.elasticsearch.action._
import org.elasticsearch.action.admin.cluster.health._
import scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])class WriteConsistencyLevelTests extends MultiNodesBasedTests {

  test("testWriteConsistencyLevelReplication2") {
    startNode("node1")
    indexer("node1").createIndex(indexName, settings = Map("number_of_shards" -> "1", "number_of_replicas" -> "2"))
    var clusterHealth = indexer("node1").health_prepare().setWaitForActiveShards(1).setWaitForYellowStatus().execute.actionGet
    clusterHealth.timedOut() should be === (false)
    clusterHealth.status() should be === (ClusterHealthStatus.YELLOW)
    indexer("node1").index(indexName, "type1", "1", source("1", "test"), consistencyLevel = Some(WriteConsistencyLevel.ONE))
    try {
      indexer("node1").index(indexName, "type1", "1", source("1", "test"), consistencyLevel = Some(WriteConsistencyLevel.QUORUM), timeout = Some("100ms"))
      fail("can't index, does not match consistency")
    } catch {
      case e: UnavailableShardsException =>
    }
    startNode("node2")
    clusterHealth = indexer("node1").health_prepare().setWaitForActiveShards(2).setWaitForYellowStatus().execute.actionGet
    clusterHealth.timedOut() should be === (false)
    clusterHealth.status() should be === (ClusterHealthStatus.YELLOW)
    indexer("node1").index(indexName, "type1", "1", source("1", "test"), consistencyLevel = Some(WriteConsistencyLevel.QUORUM), timeout = Some("1s"))
    try {
      indexer("node1").index(indexName, "type1", "1", source("1", "test"), consistencyLevel = Some(WriteConsistencyLevel.ALL), timeout = Some("100ms"))
      fail("can't index, does not match consistency")
    } catch {
      case e: UnavailableShardsException =>
    }
    startNode("node3")
    clusterHealth = indexer("node1").health_prepare().setWaitForActiveShards(3)
      .setWaitForGreenStatus().execute.actionGet
    clusterHealth.timedOut() should be === (false)
    clusterHealth.status() should be === (ClusterHealthStatus.GREEN)
    indexer("node1").index(indexName, "type1", "1", source("1", "test"), consistencyLevel = Some(WriteConsistencyLevel.ALL), timeout = Some("1s"))
  }

  private def source(id: String, nameValue: String) = """{"type1": {"id": "%s", "name": "%s"}}""".format(id, nameValue)
}
