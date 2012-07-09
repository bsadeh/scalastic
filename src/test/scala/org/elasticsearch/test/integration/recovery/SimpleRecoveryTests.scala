package org.elasticsearch.test.integration.recovery

import org.scalatest._, matchers._
import org.elasticsearch.action.admin.cluster.health._
import org.elasticsearch.action.get._
import org.elasticsearch.common.settings._
import com.traackr.scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner]) class SimpleRecoveryTests extends MultiNodesBasedTests {

  protected def recoverySettings = ImmutableSettings.Builder.EMPTY_SETTINGS

  test("testSimpleRecovery") {
    startNode("server1", recoverySettings)
    indexer("server1").createIndex(indexName, timeout = Some("5s"))
    var clusterHealth = indexer("server1").waitForYellowStatus()
    clusterHealth.timedOut() should be === (false)
    clusterHealth.status() should be === (ClusterHealthStatus.YELLOW)

    indexer("server1").index(indexName, "type1", "1", source("1", "test"))
    val flushResponse = indexer("server1").flush(Seq(indexName))
    flushResponse.totalShards() should be === (10)
    flushResponse.successfulShards() should be === (5)
    flushResponse.failedShards() should be === (0)

    indexer("server1").index(indexName, "type1", "2", source("2", "test"))
    val response = indexer("server1").refresh(indices = Seq(indexName))
    response.totalShards() should be === (10)
    response.successfulShards() should be === (5)
    response.failedShards() should be === (0)

    startNode("server2", recoverySettings)
    clusterHealth = indexer("server1").health_prepare().setWaitForGreenStatus().setWaitForNodes("2").setWaitForRelocatingShards(0).execute.actionGet
    clusterHealth.timedOut() should be === (false)
    clusterHealth.status() should be === (ClusterHealthStatus.GREEN)

    var getResult: GetResponse = null
    for (i <- 0 until 5) {
      getResult = indexer("server1").get(indexName, "type1", "1", operationThreaded = Some(false))
      getResult.sourceAsString() should be === (source("1", "test"))
      getResult = indexer("server2").get(indexName, "type1", "1", operationThreaded = Some(false))
      getResult.sourceAsString() should be === (source("1", "test"))

      getResult = indexer("server1").get(indexName, "type1", "2", operationThreaded = Some(true))
      getResult.sourceAsString() should be === (source("2", "test"))
      getResult = indexer("server2").get(indexName, "type1", "2", operationThreaded = Some(true))
      getResult.sourceAsString() should be === (source("2", "test"))
    }

    startNode("server3", recoverySettings)
    Thread.sleep(200)
    clusterHealth = indexer("server1").health_prepare().setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes("3").execute.actionGet
    clusterHealth.timedOut() should be === (false)
    clusterHealth.status() should be === (ClusterHealthStatus.GREEN)

    for (i <- 0 until 5) {
      getResult = indexer("server1").get(indexName, "type1", "1")
      getResult.sourceAsString() should be === (source("1", "test"))
      getResult = indexer("server2").get(indexName, "type1", "1")
      getResult.sourceAsString() should be === (source("1", "test"))
      getResult = indexer("server3").get(indexName, "type1", "1")
      getResult.sourceAsString() should be === (source("1", "test"))

      getResult = indexer("server1").get(indexName, "type1", "2", operationThreaded = Some(true))
      getResult.sourceAsString() should be === (source("2", "test"))
      getResult = indexer("server2").get(indexName, "type1", "2", operationThreaded = Some(true))
      getResult.sourceAsString() should be === (source("2", "test"))
      getResult = indexer("server3").get(indexName, "type1", "2", operationThreaded = Some(true))
      getResult.sourceAsString() should be === (source("2", "test"))
    }
  }

  private def source(id: String, nameValue: String) = """{"type1": {"id": "%s", "name": "%s"}}""".format(id, nameValue)
}
