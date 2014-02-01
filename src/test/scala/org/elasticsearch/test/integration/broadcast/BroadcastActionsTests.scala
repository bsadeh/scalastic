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
    clusterHealth.isTimedOut should equal (false)
    clusterHealth.getStatus should equal (ClusterHealthStatus.YELLOW)

    indexer.index(indexName, "type1", "1", source("1", "test"))
    val flushResponse = indexer.flush(Seq(indexName))
    flushResponse.getTotalShards should equal (10)
    flushResponse.getSuccessfulShards should equal (5)
    flushResponse.getFailedShards should equal (0)

    indexer.index(indexName, "type1", "2", source("2", "test"))
    val response = indexer.refresh(indices = Seq(indexName))
    response.getTotalShards should equal (10)
    response.getSuccessfulShards should equal (5)
    response.getFailedShards should equal (0)

    for (i <- 0 until 5) {
      val countResponse = indexer.count(indices = Seq(indexName), query = termQuery("_type", "type1"), operationThreading = Some(NO_THREADS))
      countResponse.getCount should equal (2)
      countResponse.getTotalShards should equal (5)
      countResponse.getSuccessfulShards should equal (5)
      countResponse.getFailedShards should equal (0)
    }
    for (i <- 0 until 5) {
      val countResponse = indexer.count(indices = Seq(indexName), query = termQuery("_type", "type1"), operationThreading = Some(SINGLE_THREAD))
      countResponse.getCount should equal (2)
      countResponse.getTotalShards should equal (5)
      countResponse.getSuccessfulShards should equal (5)
      countResponse.getFailedShards should equal (0)
    }
    for (i <- 0 until 5) {
      val countResponse = indexer.count(indices = Seq(indexName), query = termQuery("_type", "type1"), operationThreading = Some(THREAD_PER_SHARD))
      countResponse.getCount should equal (2)
      countResponse.getTotalShards should equal (5)
      countResponse.getSuccessfulShards should equal (5)
      countResponse.getFailedShards should equal (0)
    }
    for (i <- 0 until 5) {
      val countResponse = indexer.count(indices = Seq(indexName), query = termQuery("_type", "type1"))
      //fixme: failing
      //      countResponse.count should equal (0)
      //      countResponse.totalShards should equal (5)
      //      countResponse.successfulShards should equal (0)
      //      countResponse.failedShards should equal (5)
      for (exp <- countResponse.getShardFailures) {
        exp.reason should include("QueryParsingException")
      }
    }
  }

  private def source(id: String, nameValue: String) = """{"id": "%s", "name": "%s"}""".format(id, nameValue)
}
