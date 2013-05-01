package org.elasticsearch.test.integration.broadcast

import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.action.admin.cluster.health._
import org.elasticsearch.action.support.broadcast.BroadcastOperationThreading._
import scala.collection.JavaConversions._
import scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class BroadcastActionsTests extends IndexerBasedTest {

  override def shouldCreateDefaultIndex = false

  test("testBroadcastOperations") {
    indexer.createIndex(indexName, timeout = Some("5s"))
    val clusterHealth = indexer.waitForYellowStatus()
    clusterHealth.isTimedOut should be === (false)
    clusterHealth.getStatus should be === (ClusterHealthStatus.YELLOW)

    indexer.index(indexName, "type1", "1", source("1", "test"))
    val flushResponse = indexer.flush(Seq(indexName))
    flushResponse.getTotalShards should be === (10)
    flushResponse.getSuccessfulShards should be === (5)
    flushResponse.getFailedShards should be === (0)

    indexer.index(indexName, "type1", "2", source("2", "test"))
    val response = indexer.refresh(indices = Seq(indexName))
    response.getTotalShards should be === (10)
    response.getSuccessfulShards should be === (5)
    response.getFailedShards should be === (0)

    for (i <- 0 until 5) {
      val countResponse = indexer.count(indices = Seq(indexName), query = termQuery("_type", "type1"), operationThreading = Some(NO_THREADS))
      countResponse.getCount should be === (2)
      countResponse.getTotalShards should be === (5)
      countResponse.getSuccessfulShards should be === (5)
      countResponse.getFailedShards should be === (0)
    }
    for (i <- 0 until 5) {
      val countResponse = indexer.count(indices = Seq(indexName), query = termQuery("_type", "type1"), operationThreading = Some(SINGLE_THREAD))
      countResponse.getCount should be === (2)
      countResponse.getTotalShards should be === (5)
      countResponse.getSuccessfulShards should be === (5)
      countResponse.getFailedShards should be === (0)
    }
    for (i <- 0 until 5) {
      val countResponse = indexer.count(indices = Seq(indexName), query = termQuery("_type", "type1"), operationThreading = Some(THREAD_PER_SHARD))
      countResponse.getCount should be === (2)
      countResponse.getTotalShards should be === (5)
      countResponse.getSuccessfulShards should be === (5)
      countResponse.getFailedShards should be === (0)
    }
    for (i <- 0 until 5) {
      val countResponse = indexer.count(indices = Seq(indexName), query = termQuery("_type", "type1"))
      //fixme: failing
      //      countResponse.count should be === (0)
      //      countResponse.totalShards should be === (5)
      //      countResponse.successfulShards should be === (0)
      //      countResponse.failedShards should be === (5)
      for (exp <- countResponse.getShardFailures) {
        exp.reason should include("QueryParsingException")
      }
    }
  }

  private def source(id: String, nameValue: String) = """{"id": "%s", "name": "%s"}""".format(id, nameValue)
}
